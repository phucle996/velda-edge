# Velda Edge — Agent Engineering Rules

This document specifies the engineering contract, architectural invariants, and workflow constraints for AI agents and human contributors working on **Velda Edge**.

---

## 1. Mission & Architectural Mindset

Velda Edge is a performance-oriented, polyglot edge platform built around:
- **The Hourglass Architecture**: Top cone (pre-bound Ingress) $\to$ Razor-thin in-memory waist (Router) $\to$ Bottom cone (autonomous Upstream) (see [Hourglass Architecture](docs/HOURGLASS_MODEL.md))
- **Flat workflows** (`decode -> pre_route -> route -> pre_upstream -> upstream -> post_response -> encode`)
- **Flat entities** (`Session`, `Stream`, `RequestContext`, `RequestState`, `Route`, `Upstream`, `Endpoint`)
- **Explicit ownership & clean dependency boundaries**
- **In-memory hot paths** (Zero JSON parsing, zero disk I/O, zero RPC calls in request serving)
- **Autonomous Data Plane** (Operates independently from persisted LKG even if Control Plane is offline)
- **Unified Polyglot Monorepo**:
  - `crates/`: High-performance Rust 1.98 / Edition 2024 Data Plane (16 bounded crates).
  - `control-plane/`: Go 1.27 Clean Architecture Control Plane (PostgreSQL / `pgx/v5`).
  - `ui/`: Modern React 19.3 + TypeScript + Vite 8.3 + Tailwind CSS v4 + Shadcn UI Console.

The goal is not to clone Pingora, Kong, APISIX, or Envoy. External systems are references for ideas, but Velda must keep its own architecture simple, traceable, and explicit.

---

## 2. Non-Negotiable Mindset

### 2.1 Data + Functions + Narrow Contracts
- **Default tools**:
  - Structs for state/data.
  - Enums for finite states.
  - Functions for behavior.
  - Modules for ownership boundaries.
  - Traits/Interfaces only for stable extension/provider contracts.
- **No inheritance-style hierarchies**: Avoid chains like `Base -> Abstract -> Manager -> Coordinator -> Handler -> Adapter`.
- **Duplicate first. Abstract second**: A few lines of duplication are preferable to accidental coupling across subsystems.

### 2.2 Workflow Isolation over Helpers & No Arbitrary Function Splitting
- **No Arbitrary Function Splitting ("Không chia func vô tội vạ")**:
  - Keep sequential workflows contiguous, linear, and readable top-to-bottom within a single function whenever possible.
  - Do NOT chop a cohesive sequential workflow (e.g., streaming pipes, encode/decode loops, connection handshakes, request forwarding) into micro-functions, trivial 1-line wrappers, or artificial parent-module helpers just to reduce line count.
  - Do NOT create OOP-style getters/setters on structs where fields are already public or should be public (e.g., `stream.path()`, `req.method()`, `resp.status()`). Access struct fields directly (`stream.parts.uri.path()`, `req.head.method`, `resp.status`).
- **Carefully Consider Subsystem & Ownership Boundaries**:
  - Keep logic at the workflow owner and call site when it clarifies ownership, state transitions, and failure paths.
  - Duplicating 5–10 lines of setup, framing, or connection logic inside an isolated strategy module is vastly superior to creating an artificial helper in a parent module (`super::*`) that introduces accidental coupling across peer modules and forces the reader to jump back and forth.
  - Only introduce a helper or separate function when strictly required for recursive logic, reusable cross-module invariants with identical contracts, or boundary enforcement (e.g., protocol codecs, hardware topology probing).
  - Helpers must have the narrowest possible scope (module-private before package-local). Never move helpers into shared/global utilities without proven identical contracts across multiple consumers.

### 2.3 Flat Workflow & Flat Entity
- A request path must be readable from top to bottom within its workflow owner.
- Every pipeline strategy module (e.g., `buffered.rs`, `client_stream.rs`, `server_stream.rs`, `duplex.rs` in `pipe/`) must be self-contained from Step 1 to the end, without relying on ad-hoc cross-module glue in parent `mod.rs`.
- Entities own state; workflow modules own behavior.
- API workflow entities must be flat projections specific to the workflow owner. Do not create deeply nested entity graphs or generic dynamic bags like `HashMap<String, Box<dyn Any>>`.
- Mutations must read authority via their own dedicated projection/port. Never invoke read/detail workflows from mutation/publish workflows.

### 2.4 Hot-Path Invariant (Zero-IO in Serving)
Request processing in `crates/` must **never**:
- Parse JSON
- Perform configuration file I/O
- Make Control Plane RPC / HTTP calls
- Perform dynamic schema validation
- Compile configuration

All configuration is parsed, validated, and compiled into RAM (`RuntimeSnapshot`) by `velda-config` before publication to the hot path.

### 2.5 Provider Definition
A Provider is a generic, long-lived, workflow-independent capability (e.g., DNS resolution, clock/time, crypto, TLS backend).
- A Provider **must not** own request business flow or decide route selection, plugin ordering, or upstream policies.

### 2.6 Canonical Monorepo Vocabulary: The `Endpoint` Invariant
`Endpoint` (`velda_core::Endpoint`) is strictly reserved across the entire codebase to denote a **physical backend destination target** (`address: SocketAddr`, `weight: u32`).
- **Forbidden collisions**: Subsystems MUST NOT use the word `Endpoint` for:
  - Ingress ports / listeners (use `Listener`, `Binding`, or `PortBinding`).
  - Client sockets (use `Peer` or `ClientAddr`).
  - HTTP routes / API paths (use `Route`, `Path`, or `Prefix`).

### 2.7 Explicit Ingress Protocol & Pipeline Isolation Invariant (HTTP vs gRPC)
- **Declared Protocol Over Dynamic Sniffing**: Listeners must declare their application protocol explicitly (`raw` $\to$ L4 direct forward, `http` $\to$ HTTP pipeline, `grpc` $\to$ dedicated gRPC pipeline).
- **Zero Dynamic Sniffing on Hot Path**: The request serving hot path must **never** inspect payloads or sniff HTTP headers (such as `Content-Type: application/grpc`) to dynamically branch between protocols.
- **Strict Workflow Isolation**:
  - `http` listeners strictly serve HTTP Web/REST traffic through `HttpRouter` and `handle_http_stream`. They do NOT evaluate gRPC routes or fall back into gRPC semantics.
  - `grpc` listeners are dedicated RPC ingress pipelines (transported over HTTP/2 binary framing) through `GrpcRouter` and `handle_grpc_stream`. They do NOT evaluate HTTP routes.
- **HTTP is HTTP, gRPC is gRPC**: Both protocols have distinct semantics, routing algorithms, status codes (`grpc-status` trailers vs HTTP status codes), and upstream forwarding paths. They must remain completely decoupled.

### 2.8 Hard Subsystem Boundary: Upstream vs Protocol Subsystems (Lifecycle vs Mechanics)
Across **ALL protocols** (HTTP/1.1, HTTP/2, HTTP/3, gRPC, and Raw L4 TCP/UDP) without exception:
- **Upstream Subsystem (`velda-upstream`, `pre_compile/upstream/`)**:
  - **Single Responsibility**: Connection Lifecycle, Topology Resolution & Pool State (**"WHEN & WHO"**).
  - Owns: Target physical endpoint selection (`select_endpoint()` via load balancing: RoundRobin, WRR, Maglev, LeastConn, etc.), active/passive health tracking, circuit breaking, connection pooling & stream leasing (`acquire_stream()`, `register()`, idle timeout eviction, GOAWAY handling, pool cleanup on drop).
  - Determines **WHEN** to lease an existing connection, **WHEN** to request a new connection, and **WHO** (which physical endpoint) to connect to.
  - **Strict Invariants**:
    - Upstream **MUST NEVER** execute raw socket creation or binding syscalls (`TcpSocket::new`, `UdpSocket::bind`, `0.0.0.0:0`).
    - Upstream **MUST NEVER** configure kernel transport flags (`TCP_NODELAY`, `TCP_FASTOPEN_CONNECT`, `TCP_NOTSENT_LOWAT`, socket buffer sizes).
    - Upstream **MUST NEVER** execute protocol or security handshakes (TLS ClientHello, ALPN negotiation, HTTP/2 SETTINGS exchange, QUIC crypto exchange).
    - Upstream **MUST NEVER** inspect, encode, or decode protocol-specific wire frames. Upstream treats connections as opaque typed handles leased from the protocol layer.
- **Protocol Subsystems (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`, `velda-edge::tcp`)**:
  - **Single Responsibility**: Connection Mechanics, Kernel Acceleration & Wire Protocol Execution (**"HOW"**).
  - Owns: Transport socket instantiation (IPv4/IPv6 address families, TCP/UDP sockets), kernel acceleration tuning (`TcpAccelerationPath`, `Http1AccelerationPath`, `GrpcAccelerationPath`), cryptographic and protocol handshakes (TLS, ALPN, HTTP/2, QUIC), background connection driver loops (driving H2 stream multiplexing, QUIC packet loops), wire codecs (LPM framing, QPACK, text/binary parsing), and streaming pipe strategies (`buffered`, `server_stream`, `client_stream`, `duplex`).
  - **Strict Invariants**:
    - Protocol subsystems **MUST NEVER** decide load balancing or select physical backend targets.
    - Protocol subsystems **MUST NEVER** manage persistent connection pool lifecycle, endpoint health states, or cross-endpoint failover.
    - Protocol connectors expose a narrow, uniform asynchronous contract: `connect(target: SocketAddr, ...)` returning an active client connection handle (`UpstreamHttp1Stream`, `SendRequest<Bytes>`, `Http3Client`, `GrpcUpstreamConnector`, `GrpcUdpClient`), which Upstream registers into its pool.
    - Protocol pipe strategies execute purely on established protocol connection/stream handles, never creating or binding sockets ad-hoc per request.

---

## 3. Subsystem Invariants & Crate Boundaries

### 3.1 Rust Data Plane (`crates/`)
- `velda-core`: Shared vocabulary and primitive contracts only (`RequestContext`, `RequestState`, `L4Request`/`Response`, `L7Request`/`Response`, `Action`, `Error`, strongly typed IDs, and canonical `Endpoint`). No business logic, no routing, no upstream logic.
- `velda-transport`: Edge Traffic Ingress Engine (Traffic ingress, L4 connection lifecycle, TCP/UDP sockets, accept loop, L4 bidirectional byte forwarding, path classification, and L7 protocol handoff). Strictly ingress-oriented.
- `velda-tls`: Owns TLS termination, handshake, ALPN negotiation, and certificate state.
- `velda-http1`: Owns L7 HTTP/1.1 protocol lifecycle (RFC 9112: text streaming, keep-alive, zero-copy parsing, and downstream connection handling).
- `velda-http2`: Owns L7 HTTP/2 protocol engine (RFC 9113: binary framing, flow control, multiplexed stream lifecycle, and responder).
- `velda-http3`: Owns L7 HTTP/3 protocol engine (RFC 9114: QUIC datagrams, frame encoding/decoding, packet-driven state machine).
- `velda-grpc`: Owns L7 gRPC protocol engine (length-prefixed message framing, canonical status codes, server/client H2 stream lifecycle, and bidirectional streaming proxying). Completely decoupled from HTTP crates.
- `velda-router`: Owns route matching (Path, Host, Method, Headers) and route selection.
- `velda-plugin`: Owns hook registration and execution order. Hooks have constrained authority: `Action::Continue`, `Action::Respond`, `Action::Reject`.
- `velda-discovery`: [Stage 1] Backend Topology Discovery (DNS / static endpoints, in-memory cache, LKG resilience, zero-IO hot path).
- `velda-upstream`: [Stage 2] Logical backends, endpoint lifecycle, passive health tracking, and eligible candidate management. 100% protocol-blind (zero socket syscalls, zero libc setsockopt, zero wire framing).
- `velda-lb`: [Stage 3] Pure in-memory load balancing algorithms (RoundRobin, WRR, LeastConn, Maglev, RingHash, P2C, Random, Hash). Zero allocations on hot path.
- `velda-connection-pool`: [Stage 4] Generic, protocol-agnostic connection reuse, sharded containers, idle eviction, and RAII leases. Zero connection establishment logic.
- `velda-observability`: Owns metrics, tracing, and access logging.
- `velda-sync`: Connects to Go Control Plane, stages candidate configs, and compiles domain-isolated binary artifacts into LKG.
- `velda-edge`: Bootstrap, composition root, and binary entrypoint (loads `config.bin`, pre-computes protocol-specialized upstreams in `pre_compile/upstream/`, and starts `velda-transport` engine).

### 3.2 Go Control Plane (`control-plane/`)
- Follows **Clean Architecture / DDD**:
  - `cmd/`: Application entrypoints.
  - `infra/`: Database pool (`pgxpool.Pool`) and low-level drivers.
  - `internal/domain/`: Entities, repository interfaces, and service interfaces.
  - `internal/repository/`: PostgreSQL implementations.
  - `internal/service/`: Core business workflows.
  - `internal/transport/`: HTTP (Gin) and gRPC handlers.
- **CTE-First Repository**: Prioritize Common Table Expressions (CTEs) to express target, authority, latest version, and mutation projections within a single traceable query.
- Use explicit multi-statement transactions for durable multi-table transitions.
- Store configuration state in PostgreSQL using native `JSONB` with appropriate GIN/B-tree indexes.

### 3.3 React Console (`ui/`)
- Tech Stack: React 19.3, Vite 8.3, TypeScript 7.0, Tailwind CSS v4.3, Shadcn UI (`radix-nova`).
- **Unified Design System**: Use shared styling in `src/style.css` without ad-hoc typo-prone utility classes. Maintain Dark/Light theme consistency through CSS variables.
- Type-safe API communication via `@/lib/fetcher.ts` and `@/hooks/useApi.ts`.

---

## 4. Concurrency, Ownership & Async Rules

- Do not introduce `Arc<Mutex<_>>`, `Box<dyn _>`, global mutable state, or broad trait objects merely to appease the compiler.
- Explain non-obvious ownership, locking, or lifetime choices in comments.
- Async code: A Tokio task is not an OS thread. Always handle task/stream cancellation cleanly (e.g., client disconnect must propagate and release upstream connections).

---

## 5. Development & Verification Workflow

Always work in **bounded slices** (one subsystem/file at a time). Before moving on, verify continuously:

### Rust Data Plane
```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

### Go Control Plane
```bash
cd control-plane
go vet ./...
go test ./...
go build -o /dev/null ./cmd/main.go
cd ..
```

### React Console
```bash
cd ui
npm run build
cd ..
```

---

## 6. Decision Hierarchy

When design or implementation choices conflict, strictly follow this order:
1. **Correctness**
2. **Explicit ownership & dependency direction**
3. **Predictable runtime behavior**
4. **Simplicity**
5. **Observability & testability**
6. **Performance optimization based on evidence**
7. **Abstraction / reuse**
