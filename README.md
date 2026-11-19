# Velda Edge

<div align="center">

**High-Performance, Polyglot Edge Traffic Gateway & Proxy Engine**

[![Rust Edition](https://img.shields.io/badge/rust-edition%202024-orange.svg)](https://www.rust-lang.org/)
[![Go Version](https://img.shields.io/badge/go-1.27-blue.svg)](https://go.dev/)
[![React Version](https://img.shields.io/badge/react-19.3-cyan.svg)](https://react.dev/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

[Architecture](ARCHITECTURE.md) • [Hourglass Model](docs/HOURGLASS_MODEL.md) • [Pipe Strategies](docs/pipe/README.md) • [Connection Storms](docs/CONNECTION_STORMS_AND_UPSTREAM_THRASHING.md) • [Editions & Releases](docs/EDITIONS_AND_RELEASES.md) • [Config Examples](example/README.md) • [Contributing](CONTRIBUTING.md) • [Security](SECURITY.md) • [Engineering Rules](AGENTS.md)

</div>

---

## Overview

**Velda Edge** is a performance-oriented edge gateway built for predictable low latency, strict protocol isolation, and zero-downtime reloads.

Instead of parsing JSON, guessing types, or walking virtual-call chains on every request, Velda Edge compiles all declarative configuration (listeners, routes, upstreams, plugins, TLS) into an **in-memory runtime snapshot** published through wait-free atomic pointer swaps (`ArcSwap`). The request hot path only reads RAM.

The platform has three tiers:

| Tier | Technology | Role |
|---|---|---|
| **Data Plane** (`crates/`) | Rust 1.98, Edition 2024 | Serves traffic. Autonomous from the control plane via node-local Last Known Good (LKG) artifacts. |
| **Control Plane** (`control-plane/`) | Go 1.27, PostgreSQL (`pgx/v5`) | Validates, versions and compiles configuration into binary artifacts. |
| **Console** (`ui/`) | React 19.3, TypeScript, Vite 8, Tailwind v4 | Operator UI. |

---

## Key Features

- **Hourglass Architecture**: pre-bound ingress (top cone) → razor-thin in-memory route lookup (waist) → autonomous upstreams (bottom cone). See [Hourglass Model](docs/HOURGLASS_MODEL.md).
- **Flat workflows**: `decode → pre_route → route → pre_upstream → upstream → post_response → encode`, readable top to bottom without helper chains.
- **Zero-IO hot path**: no JSON parsing, no disk I/O, no Control Plane RPC and no config compilation while serving requests.
- **Strict protocol isolation**: declared listener protocols (`raw`, `http`, `grpc`) with dedicated pipelines for HTTP/1.1, HTTP/2, HTTP/3 (QUIC), gRPC and raw L4 TCP/UDP. No payload sniffing.
- **Upstream vs protocol boundary**: the upstream subsystem decides *when and who* (endpoint selection, health, pooling); protocol crates decide *how* (sockets, handshakes, framing). See [ARCHITECTURE.md §8](ARCHITECTURE.md).
- **Dense integer lookups**: routes and upstreams are dense `u16` indices backed by page-aligned catalogs and a direct `route_to_upstream` table.
- **Frontline + Shadow workers per core**: every CPU core runs a traffic worker and a `SCHED_IDLE` shadow worker. Connection pre-warming and background plugins live on the shadow worker, never on the hot path.
- **Plugin system in four phases**: `pre_route`, `pre_upstream`, `post_response` and `background`, with 35+ plugins (WAF, auth, rate limiting, caching, compression, telemetry exporters, certificate rotation and more).
- **Per-core observability**: lock-free sharded metrics, 256-byte access log records and 64-byte span records flowing through per-core SPSC rings to exporters.
- **Single-round load balancing**: Round-Robin, WRR, Least-Connections, Random, Hash, P2C, Maglev and Ring-Hash, executed exactly once inside the upstream.
- **Resilient background I/O**: outbound telemetry shippers use progressive backoff and a cool-down reconnect cycle; exporters are bounded against slow or malicious peers.
- **Lock-free reloads**: new snapshots are swapped atomically; in-flight requests finish on the old generation.

---

## Monorepo Layout

```text
velda-edge/
├── crates/                  # Rust Data Plane (20 workspace crates)
│   ├── velda-core           # Shared vocabulary: IDs, Endpoint, L4/L7 types, clock, buffers, worker registry
│   ├── velda-sync           # Config staging, validation, LKG binary artifacts
│   ├── velda-ingress        # Listener intake, shard runners, declarative reconciliation
│   ├── velda-sock           # Linux socket mechanics (SO_REUSEPORT, TFO, BBR, GRO/GSO, splice)
│   ├── velda-xdp            # AF_XDP kernel-bypass ingress
│   ├── velda-rdma           # RoCEv2 / InfiniBand transport
│   ├── velda-tls            # TLS server/client engines (rustls)
│   ├── velda-http1/2/3      # HTTP/1.1, HTTP/2 and HTTP/3 (QUIC) protocol engines
│   ├── velda-grpc           # gRPC framing, status codes, stream lifecycle
│   ├── velda-router         # Protocol-isolated route matching and route catalog
│   ├── velda-plugin         # Plugin engine: pre_route / pre_upstream / post_response / background
│   ├── velda-discovery      # Backend topology discovery (static / DNS) with LKG
│   ├── velda-upstream       # Endpoint lifecycle, health tracking, upstream catalog
│   ├── velda-lb             # Load-balancing algorithms
│   ├── velda-connection-pool# Sharded, protocol-agnostic connection pools
│   ├── velda-observability  # Metrics, access logs, traces (per-core rings)
│   ├── velda-edge           # Composition root, workers, pipelines, runtime, reload
│   └── velda-bench-backend  # Synthetic backend server used for benchmarking
├── control-plane/           # Go Clean Architecture control plane
│   ├── cmd/  infra/  migrations/
│   └── internal/{app,config,console,domain,repository,service,transport}
├── ui/                      # React operator console
├── example/                 # Complete declarative config examples
├── docs/                    # Architecture deep-dives and pipe strategy specs
├── proto/  systemd/  docker-compose*.yml  Dockerfile(s)
└── AGENTS.md  ARCHITECTURE.md  CONTRIBUTING.md  SECURITY.md
```

---

## Request Flow

```text
Downstream wire
      │
      ▼
1. Ingress          velda-ingress / velda-sock / velda-xdp
   └── multi-shard accept, optional downstream TLS (ALPN checked against declared protocol)
      │
      ▼
2. L7/L4 pipeline   velda-http1 / http2 / http3 / grpc, or raw L4
   ├── decode frames, enrich forwarding headers once
   ├── pre_route plugins
   ├── route lookup  → dense route id → route_to_upstream[id] → upstream
   └── pre_upstream plugins
      │
      ▼
3. Upstream         velda-upstream / velda-lb / velda-connection-pool
   ├── filter unhealthy endpoints, single-round load balancing
   ├── lease connection (pool HIT, or protocol connector on MISS)
   ├── stream pipe: buffered | server_stream | client_stream | duplex
   └── RAII release + health update
      │
      ▼
4. post_response plugins → encode → client
```

Full details: [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Quick Start

### Prerequisites
- **Rust** `1.98` (pinned by `rust-toolchain.toml`)
- **Go** `1.27+`
- **Node.js** `22+`
- **Docker & Docker Compose** (optional, full stack)

### Run the Data Plane

```bash
cargo build --release --workspace
VELDA_STORAGE_DIR=./storage cargo run --release -p velda-edge
```

Ready-made declarative configs (`listeners`, `routes`, `upstreams`, `plugins`, `tls`, `manifest`) live in [`example/`](example/README.md).

### Run the Full Stack

```bash
docker compose up -d        # production-like stack
docker compose -f docker-compose.dev.yml up -d   # development stack
```

Default endpoints: Console `http://localhost:5173`, Control Plane API `http://localhost:8080`, Edge ingress `:80` / `:443`.

### Install as systemd Services

`build-systemd.sh` and `install-systemd.sh` install `velda-edge`, `velda-sync` and `velda-control-plane` units from [`systemd/`](systemd/). CPU-targeted release editions are described in [Editions & Releases](docs/EDITIONS_AND_RELEASES.md).

---

## Verification

```bash
# Rust Data Plane
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace

# Go Control Plane
cd control-plane && go vet ./... && go test ./... && go build -o /dev/null ./cmd/main.go && cd ..

# React Console
cd ui && npm run build && cd ..
```

---

## Performance Philosophy

Velda Edge states its performance rules as invariants, and backs them with benchmarks rather than claims:

| Invariant | Meaning |
|---|---|
| Wait-free snapshot reads | Serving threads read the runtime through an atomic pointer load. |
| Zero heap allocation in serving | Hot-path structures are pre-sized and pre-compiled. |
| No cross-core coordination on `acquire()` | Pool lanes are per-core; warming belongs to the shadow worker. |
| Zero-downtime reload | Snapshots swap atomically; old generations drain naturally. |
| Bounded background work | Every outbound or inbound background path has size, time and concurrency limits. |

Per-plugin benchmark specifications are defined in the `BENCHMARK.md` file next to each plugin under `crates/velda-plugin/`. Metric cells marked `[TBD]` have not been measured yet.

---

## License

Velda Edge is licensed under the **Apache License, Version 2.0** ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0).
