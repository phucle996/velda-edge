# Velda Edge — Agent Engineering Rules

This document specifies the engineering contract, architectural invariants, and workflow constraints for AI agents and human contributors working on **Velda Edge**.

---

## 1. Mission & Architectural Mindset

Velda Edge is a performance-oriented, polyglot edge platform built around:
- **Flat workflows** (`decode -> pre_route -> route -> pre_upstream -> upstream -> post_response -> encode`)
- **Flat entities** (`Session`, `Stream`, `RequestContext`, `RequestState`, `Route`, `Upstream`, `Endpoint`)
- **Explicit ownership & clean dependency boundaries**
- **In-memory hot paths** (Zero JSON parsing, zero disk I/O, zero RPC calls in request serving)
- **Autonomous Data Plane** (Operates independently from persisted LKG even if Control Plane is offline)
- **Unified Polyglot Monorepo**:
  - `crates/`: High-performance Rust 1.98 / Edition 2024 Data Plane (11 bounded crates).
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

### 2.2 Workflow Isolation over Helpers
- Do not create helper functions by default.
- Keep logic at the workflow owner and call site when it clarifies ownership, state transitions, and failure paths.
- Only introduce a helper when strictly required for correctness or security, or when an invariant cannot be maintained consistently across call sites without operational risk.
- Helpers must have the narrowest possible scope (module-private before package-local). Never move helpers into shared/global utilities without proven identical contracts across multiple consumers.

### 2.3 Flat Workflow & Flat Entity
- A request path must be readable from top to bottom.
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

---

## 3. Subsystem Invariants & Crate Boundaries

### 3.1 Rust Data Plane (`crates/`)
- `velda-core`: Shared vocabulary and primitive contracts only (`RequestContext`, `RequestState`, `L4Request`/`Response`, `L7Request`/`Response`, `Action`, `Error`, strongly typed IDs, and canonical `Endpoint`). No business logic, no routing, no upstream logic.
- `velda-transport`: Edge Traffic Engine (Traffic ingress, L4 connection lifecycle, TCP/UDP sockets, accept loop, L4 bidirectional byte forwarding, path classification, and L7 protocol handoff).
- `velda-tls`: Owns TLS termination, handshake, ALPN negotiation, and certificate state.
- `velda-http`: Owns L7 HTTP protocol lifecycle (HTTP/1.1 keep-alive, HTTP/2 multiplexing, HTTP/3 streams, and request/response codec).
- `velda-router`: Owns route matching (Path, Host, Method, Headers) and route selection.
- `velda-plugin`: Owns hook registration and execution order. Hooks have constrained authority: `Action::Continue`, `Action::Respond`, `Action::Reject`.
- `velda-discovery`: [Stage 1] Backend Topology Discovery (DNS / static endpoints, in-memory cache, LKG resilience, zero-IO hot path).
- `velda-upstream`: [Stage 2] Logical backends, endpoint lifecycle, passive health tracking, and eligible candidate management.
- `velda-pool`: [Stage 4] Generic, protocol-agnostic connection reuse, sharded containers, idle eviction, and RAII leases. Zero connection establishment logic.
- `velda-observability`: Owns metrics, tracing, and access logging.
- `velda-sync`: Connects to Go Control Plane, stages candidate configs, and compiles domain-isolated binary artifacts into LKG.
- `velda-edge`: Bootstrap, composition root, and binary entrypoint (loads `config.bin`, initializes subsystem states, and starts `velda-transport` engine).

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
