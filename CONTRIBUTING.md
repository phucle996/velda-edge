# Contributing to Velda Edge

Thank you for your interest in contributing to **Velda Edge**! This document provides guidelines, architectural contracts, and workflow procedures for contributing code and documentation.

---

## 1. Engineering Philosophy & Invariants

Velda Edge is built around strict performance and architectural invariants. Before submitting a pull request, ensure your contribution respects these core principles:

### 1.1 Data + Functions + Narrow Contracts
- Structs own state/data. Enums represent finite states. Functions own behavior.
- Avoid deep inheritance hierarchies (`Base -> Abstract -> Manager -> Coordinator -> Handler -> Adapter`).
- **Duplicate first, abstract second**: A few lines of duplication are far better than accidental coupling across subsystems.

### 1.2 Workflow Isolation over Helpers
- Do not introduce helper functions by default.
- Keep logic at the workflow owner and call site when it clarifies ownership, state transitions, and failure paths.
- Only introduce a helper when strictly required for correctness or security.

### 1.3 Hot-Path Invariant (Zero-IO in Serving)
Code in `crates/` on the request serving path must **never**:
- Parse JSON or dynamic schemas.
- Perform disk I/O.
- Execute synchronous RPC calls.
- Dynamically compile configurations.

### 1.4 Strict Protocol & Pipeline Isolation
- HTTP listeners strictly serve HTTP/1.1 and HTTP/2. They do not evaluate gRPC routes.
- gRPC listeners are dedicated RPC pipelines. They do not evaluate HTTP routes.
- Never use dynamic payload sniffing on the hot path.

---

## 2. Development Setup

### Prerequisites
- **Rust Toolchain**: `1.98` (Nightly or Edition 2024 support)
- **Go**: `1.27+`
- **Node.js**: `22+` with `pnpm`
- **Git**

### Cloning & Building
```bash
# Clone the repository
git clone https://github.com/phucle996/velda-edge.git
cd velda-edge

# Check Rust workspace compilation
cargo check --workspace

# Run Rust test suite
cargo test --workspace
```

---

## 3. Step-by-Step Contribution Workflow

### 3.1 Work in Bounded Slices
Work on one bounded subsystem or file at a time. Avoid massive monolithic PRs that refactor multiple crates simultaneously.

### 3.2 Pre-Commit Verification Checklist
Before submitting a pull request, execute the full verification sequence across all modified subsystems:

#### Rust Data Plane (`crates/`)
```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

#### Go Control Plane (`control-plane/`)
```bash
cd control-plane
go vet ./...
go test -v ./...
go build -o /dev/null ./cmd/main.go
cd ..
```

#### React Console (`ui/`)
```bash
cd ui
npm run build
cd ..
```

### 3.3 Commit Message Convention
We adhere to standard Conventional Commits:

- `feat(upstream)`: Add new load balancing algorithm
- `fix(http1)`: Resolve chunked trailer parsing edge case
- `perf(router)`: Optimize prefix matching lookup
- `docs(arch)`: Update request pipeline sequence diagram
- `refactor(pipeline)`: Flatten L7 stream handoff

---

## 4. Coding Standards

### Rust
- Use `thiserror` for domain and crate-local error definitions.
- Avoid broad catch-all trait objects (`Box<dyn Error>`, `Arc<Mutex<dyn Any>>`).
- Handle task and stream cancellations cleanly (e.g. client disconnects must release upstream pooled connections).

### Go
- Follow Clean Architecture: domain models in `internal/domain`, database CTEs in `internal/repository`, transport handlers in `internal/transport`.
- Prioritize Common Table Expressions (CTEs) for atomic state queries.

### TypeScript / React
- Tech stack: React 19, Tailwind CSS v4, TypeScript 7, Shadcn UI.
- Maintain consistent theme CSS variables in `src/style.css` without ad-hoc typo-prone utility classes.

---

## 5. Community & Conduct

All participants must abide by the [Code of Conduct](CODE_OF_CONDUCT.md). Please report any unacceptable behavior to the project maintainers.
