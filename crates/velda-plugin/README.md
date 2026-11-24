# `velda-plugin`

High-performance, protocol-bound, phase-differentiated Plugin Engine and **PBWW (Plugin Background Workload Weight)** scheduling contracts for the **Velda Edge Data Plane**.

> [!NOTE]
> For a step-by-step developer guide on authoring, scheduling (`pbww`), registering, and testing custom plugins, see [AUTHORING_PLUGINS.md](AUTHORING_PLUGINS.md).

---

## 1. Architectural Philosophy & Invariants

Traditional API gateways rely on untyped property bags (`HashMap<String, Box<dyn Any>>`), runtime type downcasting, per-request JSON parsing, and ad-hoc background threads. In high-throughput edge systems, those patterns cause unpredictable tail latency, allocator contention, and leaked connections on config reload.

`velda-plugin` enforces strict compile-time and runtime invariants:

1. **Protocol-Bound Plugins ([`SupportedProtocols`](src/capability.rs))**: Plugins declare supported protocols via compile-time bitflags (`HTTP1`, `HTTP2`, `HTTP3`, `GRPC`, `TCP`, `UDP`). Mismatches fail fast during config compilation, never on the hot path.
2. **Reverse Permission Granting ([`PluginCapability`](src/capability.rs))**: Plugins explicitly declare read/write access levels separately for headers and payloads (`None < Read < Write`).
3. **Orthogonal Streaming Pipe Decoupling**: Any plugin requesting payload `Write` access is rejected at compile time if attached to a client- or server-streaming route (`PluginError::StreamingConflict`).
4. **Zero-IO Hot Path**: `from_config(&PluginConfig)` compiles all state, regexes, CIDRs, and contiguous arenas into RAM ahead of time. `on_request`, `on_upstream_request`, and `on_response` perform zero JSON parsing, zero disk I/O, and zero blocking network calls.
5. **No Rogue Threads or Hidden Spawns**: Plugins **MUST NOT** spawn raw OS threads (`std::thread::spawn`) or detached tasks (`tokio::spawn`) inside `from_config`. All persistent background loops are scheduled deterministically by Velda's **PBWW** scheduler onto dedicated `SCHED_IDLE` Shadow Workers.

---

## 2. Execution Phases vs Background Scheduling (PBWW Model)

Velda separates **when a plugin intercepts traffic** (its pipeline phase) from **how much background work it performs** (its **PBWW**):

```text
  REQUEST PIPELINE (Frontline Workers: velda-worker-{i}, SCHED_OTHER)
  ─────────────────────────────────────────────────────────────────────────────
  Ingress ─► [1. PreRoute] ─► Router ─► [2. PreUpstream] ─► Upstream ─► [3. PostResponse] ─► Egress
                 │                            │                                │
                 │  (Optional sporadic per-request offload via thread-local)   │
                 └───────────────► try_dispatch_shadow() ──────────────────────┤
                                                                               ▼
  BACKGROUND LOGISTICS (Shadow Workers: velda-shadow-{i}, SCHED_IDLE)
  ─────────────────────────────────────────────────────────────────────────────
  Every LoadedPlugin (regardless of phase) declares `pbww() -> u32` (0..=100):
    ├── pbww == 0 ──► Hot-path only (no background task scheduled)
    └── pbww > 0  ──► Greedy LPT Bin-Packing partitions tasks across `velda-shadow-{0..N-1}`
                        └─► Assigned Shadow Worker runs `run_background(shutdown)`
                            (Opens outbound Redis/OTLP/TLS connections directly on its runtime;
                             shuts down cleanly & restarts on RuntimeSnapshot hot-reload)
```

### Phase Traits

| Phase | Trait | Hot-Path Hook | Authority / Mutation |
| :--- | :--- | :--- | :--- |
| **`pre_route`** | [`PreRoutePlugin`](src/pre_route/mod.rs) | `on_request(&self, req: &mut L7Request) -> Action` | `Continue`, `Respond`, `Reject(RejectReason)`, `Delay` |
| **`pre_upstream`** | [`PreUpstreamPlugin`](src/pre_upstream/mod.rs) | `on_upstream_request(&self, req: &mut L7Request, target: &mut u16) -> Action` | Same + dynamic upstream rerouting (`canary`) |
| **`post_response`** | [`PostResponsePlugin`](src/post_response/mod.rs) | `on_response(&self, req_headers: &HeaderWorkspace, resp: &mut L7Response)` | Zero-copy/register header & status mutation |
| **`background`** | [`BackgroundPlugin`](src/background/mod.rs) | *(None — background only)* | Runs daemon loop (`run(self, shutdown)`) |

### Universal PBWW (`Plugin Background Workload Weight`) Contract

Every plugin across **all four traits** must explicitly implement `fn pbww(&self) -> u32` (`0..=100`, no default):

- **`pbww() == 0`**: Pure hot-path plugin (e.g., `ip_filter`, `cors`, `waf_lite`, `header_mutation`), or a hybrid plugin operating in `local` mode (e.g., `openai_token_quota` with `mode: "local"`). Ignored by the background scheduler with zero runtime cost.
- **`pbww() ∈ 1..=100`**: Plugin has a persistent background daemon (e.g., Redis quota lease sync, log shipping, telemetry export, certificate rotation).
  - Pure `BackgroundPlugin` implementations simply represent plugins with no hot-path hook and non-zero PBWW (`1..=100`).
  - Traffic-phase plugins with optional background loops (`openai_token_quota`, `api_metering`, `rate_limit`) implement `run_background(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Option<BackgroundTaskFuture>`.
- **Static Multi-Core Greedy LPT Balancing**: In `velda-edge` ([`partition_background_plugins`](../velda-edge/src/runtime/plugin.rs)), all plugins with `pbww() > 0` across all phases are pooled together, sorted descending by PBWW, and placed greedily onto the least-loaded Shadow Worker (`velda-shadow-{i}`).
- **Lazy Connection Ownership**: Because `from_config` never opens sockets, outbound connections (Redis, ClickHouse, OTLP, etc.) are opened lazily inside `run_background()` directly by the assigned Shadow Worker thread on its own `SCHED_IDLE` Tokio runtime.

### Two Distinct Shadow Mechanisms (Do Not Conflate)

| Pattern | Mechanism | Target Plugins |
| :--- | :--- | :--- |
| **Long-Running Daemon** | **`pbww() > 0` + `run_background(shutdown)`** | `openai_token_quota`, `api_metering`, `rate_limit`, all 14 `background/*` plugins |
| **Per-Request One-Shot Offload** | **[`try_dispatch_shadow`](src/hook.rs)** (`LOCAL_SHADOW_SENDER`) | `request_mirror`, `oauth2_introspection` (non-blocking core-local fire-and-forget from `velda-worker-{i}` to `velda-shadow-{i}`) |

---

## 3. Canonical Rejections & Virtual 3-Column Header Model

To allow protocol-independent short-circuiting without leaking internal headers:

1. **Semantic Rejection (`Action::Reject(RejectReason)`)**: Plugins return semantic reasons (`Unauthorized`, `Forbidden`, `TooManyRequests`, etc.). Each ingress protocol driver (`HTTP/1.1`, `HTTP/2`, `HTTP/3`, `gRPC`) renders the protocol-native status code and body.
2. **Bit-1 Bypass Sidecar Tagging**:
   - Normal `req.headers.insert(...)` writes Bit-0 headers intended only for upstream backends; all Bit-0 headers are stripped on `Action::Reject`.
   - `x-request-id` is pre-loaded in Bypass Slot 0.
   - Headers meant to reach rejected clients (`Retry-After`, `WWW-Authenticate`, `x-ratelimit-*`) are written via `req.headers.insert_bypass(...)` / `insert_bypass_u64(...)` (up to `MAX_BYPASS_HEADERS = 8` sidecar slots).

---

## 4. Built-in Plugins Inventory

### `pre_route`
| Plugin | Default Protocols | PBWW | Description |
| :--- | :---: | :---: | :--- |
| [`cors_preflight`](src/pre_route/cors_preflight/) | HTTP | `0` | Fast-path browser CORS `OPTIONS` preflight handler (204 No Content). |
| [`https_redirect`](src/pre_route/https_redirect/) | HTTP | `0` | Permanent/temporary redirect from plaintext HTTP to HTTPS. |
| [`ip_filter`](src/pre_route/ip_filter/) | All L7 | `0` | Lock-free SIMD/radix CIDR allowlist & denylist IP filtering. |
| [`method_filter`](src/pre_route/method_filter/) | HTTP | `0` | Bitmask HTTP method enforcement (405 Method Not Allowed). |
| [`resource_limit`](src/pre_route/resource_limit/) | HTTP | `0` | Ingress URI length, header count, and header byte-size protection. |
| [`waf_lite`](src/pre_route/waf_lite/) | HTTP | `0` | Multi-pattern Aho-Corasick/SIMD WAF inspection (SQLi, XSS, path traversal). |

### `pre_upstream`
| Plugin | Default Protocols | PBWW | Description |
| :--- | :---: | :---: | :--- |
| [`api_key_auth`](src/pre_upstream/api_key_auth/) | HTTP | `0` | Constant-time API key verification and consumer metadata injection. |
| [`api_metering`](src/pre_upstream/api_metering/) | All L7 | `0` / `1..=100` | Tiered micro-credit billing & optional distributed Redis delta sync. |
| [`canary`](src/pre_upstream/canary/) | HTTP / gRPC | `0` | Deterministic hash/header/weight-based canary upstream rerouting. |
| [`csrf`](src/pre_upstream/csrf/) | HTTP | `0` | Double-submit cookie / origin-verified CSRF token enforcement. |
| [`grpc_fault_injection`](src/pre_upstream/grpc_fault_injection/) | gRPC | `0` | Deterministic gRPC status abort & latency fault injection. |
| [`header_mutation`](src/pre_upstream/header_mutation/) | All L7 | `0` | Request header insertion, dynamic variable substitution, and stripping. |
| [`hmac_auth`](src/pre_upstream/hmac_auth/) | HTTP | `0` | HMAC-SHA256 request signature and replay-window verification. |
| [`http_cache_lookup`](src/pre_upstream/http_cache_lookup/) | HTTP | `0` | Lock-free shared-memory HTTP response cache lookup (short-circuits on HIT). |
| [`http_fault_injection`](src/pre_upstream/http_fault_injection/) | HTTP | `0` | Scoped HTTP status abort & non-blocking delay fault injection. |
| [`ja4_filter`](src/pre_upstream/ja4_filter/) | TLS L7 | `0` | TLS JA4 client fingerprint filtering directly against `req.state.ja4`. |
| [`jwt_auth`](src/pre_upstream/jwt_auth/) | HTTP | `0` | Zero-alloc JWT verification (HS/RS/ES/EdDSA) wired to `jwks_rotator`. |
| [`oauth2_introspection`](src/pre_upstream/oauth2_introspection/) | HTTP | `0` | RFC 7662 token introspection cache with core-local shadow revalidation (`try_dispatch_shadow`). |
| [`rate_limit`](src/pre_upstream/rate_limit/) | All L7 | `0` / `1..=100` | Lock-striped token/sliding-window limiter & optional Redis batch sync. |
| [`request_mirror`](src/pre_upstream/request_mirror/) | HTTP | `0` | Sampled traffic mirroring offloaded via `try_dispatch_shadow`. |
| [`request_termination`](src/pre_upstream/request_termination/) | HTTP | `0` | Static edge synthetic response termination. |
| [`url_rewrite`](src/pre_upstream/url_rewrite/) | HTTP | `0` | Zero-alloc path prefix replacement and regex-free URL rewriting. |

### `post_response`
| Plugin | Default Protocols | PBWW | Description |
| :--- | :---: | :---: | :--- |
| [`cors`](src/post_response/cors/) | HTTP | `0` | Dynamic origin-matched CORS response headers. |
| [`http_cache_store`](src/post_response/http_cache_store/) | HTTP | `0` | Captures eligible upstream responses into the shared cache engine. |
| [`openai_token_quota`](src/post_response/openai_token_quota/) | HTTP | `0` / `1..=100` | Zero-alloc OpenAI SSE/JSON token counter, 128B shared arena, and batch Lua Redis micro-quota leasing. |
| [`response_compression`](src/post_response/response_compression/) | HTTP | `0` | Adaptive zstd/gzip/brotli response body compression. |
| [`response_header_mutation`](src/post_response/response_header_mutation/) | All L7 | `0` | SIMD-accelerated response header insertion and removal. |
| [`security_headers`](src/post_response/security_headers/) | HTTP | `0` | High-speed OWASP browser hardening headers (`HSTS`, `CSP`, `X-Frame-Options`). |

### `background`
| Plugin | PBWW | Description |
| :--- | :---: | :--- |
| [`acme_cert_rotator`](src/background/acme_cert_rotator/) | `10` | Automated ACME HTTP-01 TLS certificate issuance & SNI hot-swap. |
| [`clickhouse_log_shipper`](src/background/clickhouse_log_shipper/) | `75` | Native ClickHouse HTTP RowBinary/JSON access log exporter. |
| [`elasticsearch_log_shipper`](src/background/elasticsearch_log_shipper/) | `60` | Elasticsearch NDJSON bulk access log shipper. |
| [`health_probe`](src/background/health_probe/) | `1` | Sequential inline `/livez` and `/readyz` Kubernetes probe listener. |
| [`jwks_rotator`](src/background/jwks_rotator/) | `5` | Background JWKS keyset fetcher wiring into `jwt_auth`'s shared store. |
| [`kafka_log_shipper`](src/background/kafka_log_shipper/) | `75` | Kafka RecordBatch v2 zstd access log producer. |
| [`loki_log_shipper`](src/background/loki_log_shipper/) | `80` | Grafana Loki multi-stream protobuf/JSON log exporter. |
| [`otlp_log_shipper`](src/background/otlp_log_shipper/) | `70` | OpenTelemetry access log exporter over HTTP/gRPC. |
| [`otlp_metrics_exporter`](src/background/otlp_metrics_exporter/) | `40` | OpenTelemetry atomic metrics snapshot exporter. |
| [`otlp_trace_exporter`](src/background/otlp_trace_exporter/) | `50` | OpenTelemetry distributed span batch exporter. |
| [`prometheus_exporter`](src/background/prometheus_exporter/) | `30` | Lock-free OpenMetrics `/metrics` endpoint on Shadow Worker. |
| [`s3_log_shipper`](src/background/s3_log_shipper/) | `65` | SigV4 + zstd batched access log uploader to S3-compatible storage. |
| [`statsd_exporter`](src/background/statsd_exporter/) | `20` | MTU-chunked StatsD / DogStatsD UDP metrics pusher (`sendmmsg`). |
| [`std_log_shipper`](src/background/std_log_shipper/) | `15` | Buffered `stdout` / `stderr` / file access log writer. |

---

## 5. Verification

```bash
cargo fmt --check -p velda-plugin
cargo clippy -p velda-plugin --all-targets --all-features -- -D warnings
cargo test -p velda-plugin
```
