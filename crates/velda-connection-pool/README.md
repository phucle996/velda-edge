# velda-connection-pool

Generic, protocol-agnostic, sharded connection pooling provider for the Velda Edge Data Plane.

---

## Internal Topology

```text
                  PoolManager::acquire(&ConnectionKey)
                                  │
                                  ▼
                        hash_one(key) & mask
                                  │
          ┌───────────────────────┼───────────────────────┐
          ▼                       ▼                       ▼
   PoolShard [0]           PoolShard [1]          PoolShard [N-1]
   #[repr(align(64))]      #[repr(align(64))]     #[repr(align(64))]
   ┌─────────────────┐     ┌─────────────────┐    ┌─────────────────┐
   │ Mutex<HashMap>  │     │ Mutex<HashMap>  │    │ Mutex<HashMap>  │
   └────────┬────────┘     └────────┬────────┘    └────────┬────────┘
            │                       │                      │
      key lookup              key lookup             key lookup
            │                       │                      │
            ▼                       ▼                      ▼
     ┌──────────────┐        ┌──────────────┐       ┌──────────────┐
     │ SubPool[Key] │        │ SubPool[Key] │       │ SubPool[Key] │
     │ (LIFO Queue) │        │ (LIFO Queue) │       │ (LIFO Queue) │
     │ ┌──────────┐ │        │ ┌──────────┐ │       │ ┌──────────┐ │
     │ │Conn (Hot)│ │        │ │Conn (Hot)│ │       │ │Conn (Hot)│ │
     │ ├──────────┤ │        │ ├──────────┤ │       │ ├──────────┤ │
     │ │Conn ...  │ │        │ │Conn ...  │ │       │ │Conn ...  │ │
     │ ├──────────┤ │        │ ├──────────┤ │       │ ├──────────┤ │
     │ │Conn (Old)│ │        │ │Conn (Old)│ │       │ │Conn (Old)│ │
     │ └──────────┘ │        │ └──────────┘ │       │ └──────────┘ │
     └──────┬───────┘        └──────────────┘       └──────────────┘
            │
            ▼ pop_back() O(1) [HIT]
       PoolLease (RAII Handle)
```

---

## Checkout & Release Flow

```text
                   Request Ingress
                         │
                         ▼
             PoolManager.acquire(&key)
                         │
        ┌────────────────┴────────────────┐
     [HIT]                             [MISS]
        │                                 │
        ▼                                 ▼
   pop_back() (253 ns, 0 allocs)      Upstream.connect(target_addr)
        │                                 │
        └────────────────┬────────────────┘
                         ▼
                  PoolLease (RAII)
                         │
                    Execute I/O
                         │
                  Lease Dropped
                         │
                 is_healthy && !draining?
                  ├── YES ──► push_back() vào SubPool (capping max_idle)
                  └── NO  ──► conn.close() lập tức (chống leak FD)
```

---

## Component Roles

| Component | Responsibility | Invariant |
| :--- | :--- | :--- |
| [`ConnectionKey`](src/key.rs) | Lookup identity (`target_addr`, `protocol`, `sni`, `alpn`) | 0 heap allocs on hot path via `Arc<str>` |
| [`PoolManager`](src/manager/mod.rs) | Top-level orchestrator & metrics facade | Auto-probes CPU cores, clamps shards in `[8, 1024]` |
| [`ConnectionProfile`](src/connection/profile.rs) | Protocol-driven checkout policy | Supports `Sequential` (HTTP/1.1), `Exclusive` (TCP), `Multiplexed` (HTTP/2/3) |
| [`MultiplexedPool`](src/connection/multiplexed.rs) | Sharded 1:N stream slot multiplexer | 100% lock-free stream release via atomic timestamp epoch |
| [`PoolShard`](src/container.rs) | Concurrency partition lock | `#[repr(align(64))]` prevents false sharing; poison-resilient |
| [`SubPool`](src/container.rs) | Per-key LIFO queue container | LIFO order; `empty_since` protects active pools from prune |
| [`SequentialLease`](src/connection/sequential.rs) | HTTP/1.1 transactional loan handle | Auto-returns healthy sockets on drop; closes dirty sockets (0 B alloc) |
| [`ExclusiveLease`](src/connection/exclusive.rs) | L4 Raw TCP exclusive loan handle | Zero heap allocation via `with_pool` zero-alloc return |
| [`StreamLease`](src/connection/multiplexed.rs) | HTTP/2 concurrent stream slot handle | Atomic ref-counting; zero allocation checkout |

---

## Benchmark Highlights

- **Zero-Allocation Invariant**: **0 Bytes / 0 Heap Allocs** across ALL 3 profiles (`SequentialLease`, `ExclusiveLease`, `StreamLease`).
- **HIT Latency**: 201 ns (0 bytes / 0 heap allocations).
- **MISS Latency**: 96 ns (0 bytes / 0 heap allocations).
- **Sequential (HTTP/1.1)**: **4.66 Million ops/s** (214 ns latency, 0 B allocs).
- **Exclusive (Raw TCP)**: **4.60 Million ops/s** (217 ns latency, 0 B allocs - down from 500KB heap allocs).
- **Multiplexed (HTTP/2)**: **5.34 Million ops/s** (187 ns latency, 0 B allocs, 100% lock-free stream release).
- **Multicore Scaling**: **15.81 Million ops/s** (63 ns avg latency across 256 threads).
- **Realistic Gateway Workload**: **12.93 Million ops/s** with 32 workers + periodic sweeps (100% reuse, Zero Deadlocks).
- **Subpool LIFO Depth Scaling**: Flat 183 - 187 ns from N = 10 to 10,000 (Strictly $\mathcal{O}(1)$).
- **Idle Sweep**: 23 - 48 ns per connection.

---

## Verification

```bash
cargo clippy -p velda-connection-pool --all-targets --all-features -- -D warnings
cargo test -p velda-connection-pool
cargo bench -p velda-connection-pool --bench single_thread_bench -- --nocapture
cargo bench -p velda-connection-pool --bench multi_thread_bench -- --nocapture
cargo bench -p velda-connection-pool --bench profile_bench -- --nocapture
cargo bench -p velda-connection-pool --bench adversarial_bench -- --nocapture
```


