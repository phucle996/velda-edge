# velda-upstream

Stage 2 — Backend Lifecycle, Eligibility, and Resource Management for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Upstream = Logical Backend Resource Manager`.
> Upstream is neither a router, load balancer, connection pool, nor transport engine.
> It orchestrates 7 core functions: **Identity, Discovery Integration, Endpoint Lifecycle, Health Eligibility, LB Selection, Connection Intent, and Pool Acquisition**.

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
                      ║  (Logical Backend Resource Manager)   ║
                      ╚══════════════════════╤════════════════╝
                                             │
      ┌──────────────────────────────────────┼──────────────────────────────────────┐
      ▼                                      ▼                                      ▼
1. Discovery Topology                  2. Eligibility                         3. Connection Intent
velda-discovery                        HealthTracker                          protocol = "http1"
Arc<Discovery>                         consecutive fails < 3                  timeouts = UpstreamTimeouts
  ├── Endpoint A (Active)              cooldown window = 10s                  ConnectionKey
  ├── Endpoint B (Active)                    │                                      │
  └── Endpoint C (Draining)                  ▼                                      │
      │                               Eligible Slice                                │
      └──────────────────────────────────────┼──────────────────────────────────────┘
                                             │
                                             ▼
                               4. Endpoint Selection (WHICH)
                                          velda-lb
                                  LoadBalancer::select(...)
                                             │
                                             │ Selected Endpoint: B
                                             ▼
                                5. Pool Acquisition (REUSE)
                                    velda-connection-pool
                                  PoolManager(upstream_id)
                                             │
                                   ┌─────────┴─────────┐
                                 [HIT]               [MISS]
                                   │                   │
                                   │              Connector::connect(B)
                                   │                   │
                                   └─────────┬─────────┘
                                             │
                                             ▼
                                     BackendLease (RAII)
                               (Auto-Return or Auto-Drain)
                                             │
                                             ▼
                                   Transport / Execution
                                   velda-transport / HTTP
```

---

## 2. Seven Core Functions

| # | Function | Subsystem Role | Delegated / Partner Subsystem |
|---|---|---|---|
| **1** | **Identity & Config** | Owns `id`, default `protocol` (`"http1"`, `"http2"`, `"tcp"`), and `UpstreamTimeouts`. | Config parsed ahead-of-time by `velda-sync`. |
| **2** | **Discovery Integration** | Holds `Arc<Discovery>` (static endpoints or dynamic DNS background refresh). | Topology discovery owned by [`velda-discovery`](../velda-discovery). |
| **3** | **Endpoint Lifecycle** | Tracks `EndpointState`: `Active`, `Unhealthy`, `Draining`, `Removed`. | Node draining prevents new connections while draining existing leases. |
| **4** | **Health & Eligibility** | Passive failure counting and cooldown window; filters candidates for LB. | Endpoint health tracking distinct from connection health. |
| **5** | **Endpoint Selection** | Passes slice of eligible candidates to load balancer algorithm. | Algorithmic strategies owned by [`velda-lb`](../velda-lb). |
| **6** | **Connection Intent** | Compiles upstream protocol and target into `ConnectionKey` & `AcquireTarget`. | Reuses protocol profile semantics from `velda-connection-pool`. |
| **7** | **Pool Acquisition** | Owns identity-isolated `PoolManager`; returns RAII `BackendLease`. | Pool sharding, idle queues, and eviction owned by [`velda-connection-pool`](../velda-connection-pool). |

---

## 3. Invariants & Guarantees

1. **Protocol-Agnostic Core**:
   `velda-upstream` manages connections generically through `ConnectionKey` identifiers and the `BackendConnection` trait.
2. **Zero-IO Hot Path**:
   Serving path never parses JSON, never reads disk, and never queries DNS. DNS resolution is performed exclusively in background workers.
3. **Canonical Endpoint Invariant**:
   Strictly re-exports and uses [`velda_core::Endpoint`](../velda-core) (`address: SocketAddr`, `weight: u32`).
4. **Endpoint Lifecycle & Draining**:
   - `Active`: Eligible for new connections and load balancing.
   - `Unhealthy`: Excluded from new traffic due to consecutive connection failures.
   - `Draining`: Removed from active discovery; existing leases finish cleanly, but no new work is admitted.
   - `Removed`: Completely decommissioned once drained.
5. **Safe RAII Leases**:
   Acquiring a backend returns a `BackendLease`. When dropped, healthy connections are returned to the pool; draining or broken connections are closed automatically.
6. **Timeouts Semantics**:
   - `connect`: Socket connect timeout.
   - `idle`: Maximum idle lifetime in pool before eviction.
   - `request`: Optional L7 request/stream timeout (`None` for unbounded L4 streams).

---

## 4. Benchmark Highlights

| Operation | Latency / op | Throughput | Allocations | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Endpoint Selection** | **10 ns** | **97.7M ops/s** | **0 B (Zero-Alloc)** | Round-robin across 10,000 backends |
| **Pool Cache Hit** | **211 ns** | **4.72M ops/s** | **0 B (Zero-Alloc)** | Subpool LIFO lock-free checkout |
| **Pool Miss + Wrap** | **93 ns** | **10.6M ops/s** | **0 B (Zero-Alloc)** | Fast fallback to connection creator |
| **Full Acquire Pipeline** | **524 ns** | **1.91M req/s** | Minimal | Concurrent acquisition with 100% pool hit rate |
| **Passive Failure Failover** | **534 ns** | **1.87M req/s** | Automatic | Transparent fallback to healthy secondary node |

---

## 5. Verification

```bash
# Code Style & Lints
cargo fmt --check -p velda-upstream
cargo clippy -p velda-upstream --all-targets --all-features -- -D warnings

# Unit & Integration Tests (6 tests)
cargo test -p velda-upstream

# Performance Benchmarks
cargo bench -p velda-upstream
```
