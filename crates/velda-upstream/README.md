# velda-upstream

Stage 2 — Logical Backend Lifecycle, Health Eligibility, and Load Balancing for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Upstream<LB> = Logical Backend Manager`.
> Upstream is neither a router, connection pool, nor transport engine.
> It orchestrates 5 core functions: **Identity, Discovery Integration, Health Eligibility, Load Balancer Selection, and Execution Failover**.
> Protocol connection establishment and pooling are completely decoupled and owned by protocol pipeline crates (`velda-http1`, `velda-http2`, `velda-grpc`, `velda-connection-pool`).

---

## 1. Subsystem Architecture & Responsibilities

```text
                                  Route Match (WHERE)
                                     velda-router
                                          │
                                          │ Upstream ID ("users")
                                          ▼
                      ╔═══════════════════════════════════════╗
                      ║            velda-upstream             ║
                      ║       (Logical Backend Manager)       ║
                      ╚═══════════════════════╤═══════════════╝
                                              │
      ┌───────────────────────────────────────┼──────────────────────────────────────┐
      ▼                                       ▼                                      ▼
1. Discovery                            2. Eligibility                        3. Timeouts
velda-discovery                         HealthTracker                         connect, idle, request
Arc<Discovery>                          consecutive fails < threshold         UpstreamTimeouts
  ├── Endpoint A (Active)               cooldown window = cooldown_ms                │
  ├── Endpoint B (Active)                     │                                      │
  └── Endpoint C (Draining)                   ▼                                      │
      │                                Eligible Slice                                │
      └───────────────────────────────────────┼──────────────────────────────────────┘
                                              │
                                              ▼
                                4. Endpoint Selection (WHICH)
                                          velda-lb
                                   LoadBalancer::select(...)
                                              │
                                              ▼
                                5. Execution & Candidate Failover
                                   upstream.execute(|ep| async { ... })
                                              │
                                              ▼
                                Protocol Connect & Pool (HOW)
                                velda-http1 / velda-http2 / velda-grpc / tcp
```

---

## 2. Core Functions

| # | Function | Subsystem Role | Delegated / Partner Subsystem |
|---|---|---|---|
| **1** | **Identity & Metadata** | Owns `id`, protocol tag (`"http1"`, `"http2"`, `"tcp"`, etc.), and `UpstreamTimeouts`. | Config parsed ahead-of-time by `velda-sync`. |
| **2** | **Discovery Integration** | Holds `Arc<Discovery>` (static endpoints or dynamic DNS background refresh). | Topology discovery owned by [`velda-discovery`](../velda-discovery). |
| **3** | **Health & Eligibility** | Passive failure counting and cooldown window; filters candidates for LB. | Endpoint health tracking distinct from connection health. |
| **4** | **Endpoint Selection** | Passes slice of eligible candidates to load balancer algorithm (`select_endpoint`). | Algorithmic strategies owned by [`velda-lb`](../velda-lb). |
| **5** | **Execution & Failover** | Executes caller's protocol operation with automatic failover across healthy candidates (`execute`). | Wire connection and pooling owned by protocol crates. |
| **6** | **Acceleration Path** | Pre-compiles `SocketAccelerationPath` from hardware topology, timeouts, and TLS status. | Kernel capability detection owned by [`velda-core`](../velda-core). |

---

## 3. Invariants & Guarantees

1. **Protocol-Agnostic Logical Core**:
   `velda-upstream` manages backend nodes without coupling to raw TCP sockets, TLS handshakes, or protocol codecs.
2. **Zero Socket I/O & Zero Syscalls**:
   `velda-upstream` does not open sockets, touch file descriptors, or perform `setsockopt` calls. Protocol crates own their own wire connection strategies.
3. **Zero-IO Hot Path**:
   Serving path never parses JSON, never reads disk, and never queries DNS synchronously. DNS resolution is performed exclusively by background discovery tasks.
4. **Canonical Endpoint Invariant**:
   Strictly uses [`velda_core::Endpoint`](../velda-core) (`address: SocketAddr`, `weight: u32`).
5. **Passive Health & Failover**:
   - Consecutive failures increment until threshold, marking the node unhealthy.
   - When a node fails during `execute()`, Upstream marks it and transparently fails over to the next eligible candidate.
   - When the cooldown window expires, the node enters half-open recovery.
6. **Timeouts Semantics**:
   - `connect`: Backend socket connect timeout.
   - `idle`: Maximum idle lifetime for connection pooling.
   - `request`: Optional L7 request/stream timeout (`None` for unbounded L4 streams).

---

## 4. Verification

```bash
# Code Style & Lints
cargo fmt --check -p velda-upstream
cargo clippy -p velda-upstream --all-targets --all-features -- -D warnings

# Unit & Integration Tests (11 tests)
cargo test -p velda-upstream
```
