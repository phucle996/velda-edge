# Velda Edge — Architecture Specification

This document describes the architecture, invariants, memory model and request/background workflows of **Velda Edge**. Companion documents: [Hourglass Model](docs/HOURGLASS_MODEL.md), [Pipe Strategies](docs/pipe/README.md), [Connection Storms](docs/CONNECTION_STORMS_AND_UPSTREAM_THRASHING.md), [Engineering Rules](AGENTS.md).

---

## 1. System Tiers

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │ CONSOLE        React 19.3 + TypeScript + Vite 8 + Tailwind v4          │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ HTTP / JSON
 ┌───────────────────────────────────▼────────────────────────────────────┐
 │ CONTROL PLANE  Go 1.27, Clean Architecture, PostgreSQL (pgx/v5)        │
 │  - validates and versions declarative config (JSONB, CTE-first repos)  │
 │  - compiles config into binary artifacts                               │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ velda-sync: stage → validate → LKG
 ┌───────────────────────────────────▼────────────────────────────────────┐
 │ DATA PLANE     Rust 1.98 / Edition 2024 (crates/)                      │
 │  - serves from an in-memory ArcSwap<Runtime>                           │
 │  - autonomous: restarts and serves from local LKG with no Control Plane│
 └────────────────────────────────────────────────────────────────────────┘
```

`velda-sync` owns configuration staging, validation and LKG artifacts on the data-plane node. `velda-edge` loads the artifacts at boot and on reload, compiles them into a `Runtime`, and publishes it.

---

## 2. Core Model & Invariants

### 2.1 Hourglass

```text
               DOWNSTREAM INGRESS (top cone)
   [ HTTP/1.1 | HTTP/2 | HTTP/3 | gRPC TCP | gRPC UDP | L4 TCP/UDP ]
   ↳ listeners pre-bound by velda-ingress, protocol declared in config
                  \                         /
                   ▼                       ▼
              ┌───────────────────────────────┐
              │   NARROW WAIST: route lookup  │
              │ (listener, host/path/method)  │
              │        → upstream id          │
              └───────────────────────────────┘
                   ▲                       ▲
                  /                         \
               UPSTREAM EGRESS (bottom cone)
   [ HTTP/1 pool | H2 mux | H3 QUIC | gRPC TCP | gRPC UDP | L4 pipe ]
   ↳ autonomous upstreams: health, load balancing, pooling
```

See [Hourglass Model](docs/HOURGLASS_MODEL.md) for the mental model and anti-patterns.

### 2.2 Hot-Path Invariant (Zero-IO in Serving)

Request processing never parses JSON, performs config file I/O, calls the Control Plane, validates dynamic schemas, or compiles configuration. All of that happens in `velda-sync` / `velda-edge` before a snapshot is published.

### 2.3 Flat Workflows

```text
decode → pre_route → route → pre_upstream → upstream → post_response → encode
```

State lives in structs, behavior in functions, boundaries in modules. Each pipeline strategy module is self-contained from step 1 to the end; no ad-hoc glue in parent modules.

### 2.4 Protocol & Pipeline Isolation

- Listeners declare their protocol: `raw` → L4 direct forward, `http` → HTTP pipeline, `grpc` → gRPC pipeline.
- No payload or header sniffing decides protocol on the hot path.
- `http` listeners never evaluate gRPC routes; `grpc` listeners never evaluate HTTP routes.

### 2.5 Canonical Vocabulary

`Endpoint` (`velda_core::Endpoint`) means only a **physical backend destination** (`address`, `weight`). Listener ports are `Listener`/`Binding`, client sockets are `Peer`/`ClientAddr`, HTTP paths are `Route`/`Path`/`Prefix`.

### 2.6 Single Ingress Header Enrichment

`x-forwarded-*` / `x-real-ip` are enriched exactly once at the downstream entrypoint, before route matching and plugin phases. Upstream pipe strategies only forward headers.

---

## 3. Request Pipeline

```text
CLIENT
  │
  ▼
1. INGRESS  (velda-ingress, velda-sock, velda-xdp, velda-tls)
   • accept on pre-bound listener shards
   • optional downstream TLS; ALPN validated against declared protocol
   • hand off to the pipeline for that listener id (no guessing)
  │
  ▼
2. PIPELINE (velda-http1 / http2 / http3 / grpc, or raw L4)
   • decode wire frames
   • enrich forwarding headers (once)
   • run pre_route plugins
   • route lookup → dense route id → route_to_upstream[route_id] → upstream
   • run pre_upstream plugins
  │
  ▼
3. UPSTREAM (velda-upstream, velda-lb, velda-connection-pool)
   • drop tripped endpoints (circuit breaker)
   • single-round load balancing
   • lease a pooled connection (HIT) or request a new one from the protocol connector (MISS)
   • stream pipe: buffered | server_stream | client_stream | duplex
   • RAII release: health record + connection returned to pool
  │
  ▼
4. post_response plugins → encode → CLIENT
```

---

## 4. In-Memory Runtime

```text
SharedRuntime = Arc<ArcSwap<Runtime>>

Runtime (revision: u64)
├── config            declarative domains loaded from *.bin
│                     listeners · routes · upstreams · plugins · tls
├── router            protocol-isolated route tables (l4, http1, http2, http3, grpc)
├── pipelines         PipelineTable: listener id → TcpPipeline | UdpPipeline
├── upstreams         UpstreamTable: protocol-isolated (tcp, udp, http1, http2, http3, grpc)
├── route_to_upstream Box<[UpstreamId]>   dense route index → dense upstream index
├── tls_server        pre-compiled downstream TLS engine
└── tls_client        pre-compiled upstream TLS engine
```

### 4.1 Dense IDs and Catalogs

`RouteId` and `UpstreamId` are dense `u16` indices with an `UNASSIGNED` sentinel. `velda_edge::runtime::lut` builds, at bootstrap and on every reload:

- `route_to_upstream`: direct indexed mapping, no string lookup on the hot path.
- `RouteCatalog` (`velda-router`) and `UpstreamCatalog` (`velda-upstream`): 4 KB page-aligned byte catalogs that map a dense id back to its name, published through global `ArcSwap` and used by logging and tracing (names resolve to `unmatched` when no id is assigned).

### 4.2 Upstream Ownership

- Every upstream owns its private load balancer, timeouts, health tracker and connection pool. Pools are never shared across upstreams.
- Backend endpoints are pre-resolved into `Vec<Endpoint>` by `velda-discovery`.
- TLS client engines, SNI targets, pipe strategy and kernel acceleration paths are compiled into the upstream at snapshot build time.
- Pool kinds: sharded sequential pool (HTTP/1.1, TCP) and multiplexed pool (HTTP/2, HTTP/3, gRPC).

### 4.3 Atomic Swap

```text
Active snapshot (gen N)  ──ArcSwap::store──►  Candidate (gen N+1)
```

- Reads are an atomic pointer load with no allocation.
- In-flight requests finish on generation N; new connections read N+1. No resets, no drops.

---

## 5. Plugin System (`velda-plugin`)

Plugins belong to exactly one phase and are bound to routes ahead of time (`RoutePluginPlan`).

| Phase | When | Plugins |
|---|---|---|
| `pre_route` | after decode, before route lookup | `cors_preflight`, `ip_filter`, `resource_limit`, `waf_lite` |
| `pre_upstream` | after route, before upstream dispatch | `api_key_auth`, `api_metering`, `canary`, `consumer_quota`, `csrf`, `grpc_fault_injection`, `header_mutation`, `hmac_auth`, `http_cache_lookup`, `http_fault_injection`, `jwt_auth`, `oauth2_introspection`, `rate_limit`, `request_mirror`, `request_termination`, `url_rewrite` |
| `post_response` | on upstream response, before encode | `ai_token_metering`, `cors`, `http_cache_store`, `response_compression`, `response_header_mutation`, `security_headers` |
| `background` | on shadow workers, never in the request path | `acme_cert_rotator`, `health_probe`, `jwks_rotator`, `loki_log_shipper`, `otlp_log_shipper`, `otlp_metrics_exporter`, `otlp_trace_exporter`, `prometheus_exporter`, `statsd_exporter`, `std_log_shipper` |

Traffic-phase hooks have constrained authority: `Action::Continue`, `Action::Respond`, `Action::Reject`. Plugin configuration is validated in `velda-plugin::validation` and by `velda-sync` before publication.

Early rejections use a header workspace with a per-header visibility bit, so a blocked request can only emit the headers meant to escape a rejection (`Retry-After`, `x-ratelimit-*`, `WWW-Authenticate`, `x-request-id`) and never leaks upstream-bound headers. Details: [`crates/velda-core/ARCHITECTURE.md`](crates/velda-core/ARCHITECTURE.md).

---

## 6. Workers: Frontline and Shadow

Every CPU core hosts a pair of threads, created in `velda-edge::lifecycle::worker`:

```text
┌──────────────────────────────── CPU CORE k ────────────────────────────────┐
│  FRONTLINE  velda-worker-{k}  (SCHED_OTHER)   SHADOW  velda-shadow-{k}      │
│  serves downstream traffic                    (SCHED_IDLE)                  │
│  upstream.acquire(): lane k HIT/MISS          prewarm_lane(k), background   │
│  no warmup, no task boxing, no channels       plugins, log/trace shipping   │
│                    └───────── sharded connection pool, lane k ─────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
```

### 6.1 Frontline

- Serves traffic only. `acquire()` is flat and zero-coordination: pool HIT reuses a connection; MISS connects directly.
- Forbidden on the hot path: spawning warmup tasks, boxing futures, sending over `mpsc` channels, coordination flags such as `is_warming`.

### 6.2 Shadow

- Pinned to the same core, runs under `SCHED_IDLE`, so it consumes only spare cycles.
- Calls `prewarm_lane(k)` at startup, on config reload and every 2 s, depositing warm connections into lane `k` (`release_to_lane()` / `register_warmed_to_lane()`).
- One shadow per lane means no cross-thread races on that lane, so no locks or flags are needed.
- Performs socket-owner handshake bridging: the socket is registered on the frontline reactor while CPU-heavy handshakes (TLS, ALPN, HPACK/QPACK init) execute on the shadow thread.

### 6.3 Background Plugin Execution

Background plugins reach the shadow worker through the core-local hook (`velda_plugin::hook`, `ShadowTask`). Frontline code does not spawn them.

- **Single consumer per ring**: telemetry rings are SPSC. Only the shadow worker of the owning core drains its ring; shipper plugins never flush rings themselves.
- **Bin-packing**: background plugins are statically bin-packed onto shadow workers; a plugin is not pinned to a single core by hard affinity.
- **Outbound TLS**: all outbound plugins (`jwks_rotator`, `acme_cert_rotator`, the Loki/OTLP shippers and exporters) use `velda_tls::ClientTlsConfig` built from the plugin's `OutboundTlsConfig` (`ca_pem`, `insecure_skip_verify`). They do not read the OS trust store implicitly.
- **Shipper resilience**: a failed send retries with progressive exponential backoff (10 attempts, 100 ms to 2 s). After 10 failures the connection is torn down, the plugin cools down for 30 s (discarding batches to keep memory flat), then reconnects and resumes.
- **Bounded I/O**: ACME responses are capped at 256 KB. `prometheus_exporter` enforces its concurrency limit at `accept()` time (excess connections get an immediate 503, with no task and no TLS handshake) and applies a 2 s timeout to the TLS handshake and request line.

---

## 7. Observability (`velda-observability`)

| Subsystem | Data | Transport |
|---|---|---|
| `metrics` | cacheline-aligned, per-core sharded counters and pool gauges; snapshot aggregation | in-memory store |
| `logs` | 256-byte `AccessLogRecord` | per-core `SpscLogRing`, drained in batches into `LogStore`, fanned out as `Arc<[AccessLogRecord]>` |
| `traces` | 64-byte `SpanRecord` | per-core `SpscSpanRing`, drained into `TraceStore`, fanned out as `Arc<[SpanRecord]>` |

Records carry integers (dense route/upstream ids, pipe kind, timestamps from the `velda-core` clock subsystem); names are resolved by exporters through the catalogs in §4.1. Exporters (`std_log_shipper`, `loki_log_shipper`, `otlp_*`, `prometheus_exporter`, `statsd_exporter`) are background plugins.

---

## 8. Upstream vs Protocol Subsystems

| | Upstream (`velda-upstream`, `velda-edge/src/upstream/`) | Protocol crates (`velda-http1/2/3`, `velda-grpc`, `velda-edge::tcp`) |
|---|---|---|
| Question | **When and who** | **How** |
| Owns | endpoint selection (via `velda-lb`), health and circuit breaking, pool leasing, idle eviction, GOAWAY handling | socket creation, kernel tuning, TLS/ALPN/QUIC handshakes, connection driver loops, wire codecs, pipe strategies |
| Must never | create or bind sockets, set kernel flags, run handshakes, parse wire frames | choose backends, manage pool lifecycle or endpoint health, fail over across endpoints |

Protocol connectors expose a narrow contract: `connect(target: SocketAddr, …)` returning a client connection handle that the upstream registers into its pool. Pipe strategies run only on established handles.

### Circuit Breaker

```text
            record_success(): fails = 0, unhealthy_since = 0
        ┌────────────────────────────────────────────────────────┐
        ▼                                                        │
  ┌──────────┐  consecutive_fails ≥ max  ┌──────────┐  cooldown   │
  │ HEALTHY  │ ────────────────────────► │ TRIPPED  │ ─────────┐  │
  └──────────┘   CAS(0 → timestamp)      └──────────┘          ▼  │
        ▲                                              ┌───────────────┐
        └──────────── record_success() ────────────────│   HALF-OPEN   │
                                                       └───────────────┘
                                                        failure → re-trip
```

- Only transport-level failures (connect timeout, `ECONNREFUSED`) count as failures; HTTP 4xx/5xx responses are valid application traffic and are never penalized.
- If an endpoint fails during acquisition, the upstream tries the next healthy candidate before returning 502.

---

## 9. Crate Responsibilities

| Crate | Responsibility |
|---|---|
| `velda-core` | Vocabulary only: IDs, `Endpoint`, L4/L7 types, `Action`, errors, clock, buffers, header workspace, worker registry. No routing or upstream logic. |
| `velda-sync` | Config staging, validation, binary artifact compilation, LKG. |
| `velda-ingress` | Listener intake, shard runners, LKG listener reconciliation. |
| `velda-sock` | Linux socket mechanics. |
| `velda-xdp`, `velda-rdma` | Kernel-bypass ingress and RDMA transport. |
| `velda-tls` | TLS termination and client engines, ALPN, certificate state. |
| `velda-http1`, `velda-http2`, `velda-http3` | RFC 9112 / 9113 / 9114 engines. |
| `velda-grpc` | gRPC framing, status codes, stream lifecycle; decoupled from HTTP crates. |
| `velda-router` | Route matching and the route catalog. |
| `velda-plugin` | Hook registration, execution order, plugin implementations. |
| `velda-discovery` | Static/DNS topology discovery, in-memory cache, LKG. |
| `velda-upstream` | Logical backends, endpoint lifecycle, health, upstream catalog. Protocol-blind. |
| `velda-lb` | Load-balancing algorithms. |
| `velda-connection-pool` | Generic sharded pools and RAII leases. |
| `velda-observability` | Metrics, logs, traces. |
| `velda-edge` | Composition root: bootstrap, workers, pipelines, runtime, reload. |
| `velda-bench-backend` | Backend for the benchmark suite. |
| `control-plane/` | Go control plane. |
| `ui/` | React console. |

> [!NOTE]
> For connection churn mechanics and cold-upstream thrashing see [Connection Storms](docs/CONNECTION_STORMS_AND_UPSTREAM_THRASHING.md).
