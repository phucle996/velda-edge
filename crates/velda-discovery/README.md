# velda-discovery

Stage 1 — Backend Topology Discovery for the Velda Edge Data Plane.

Dedicated to answering a single topological invariant: **"Where are the backends located?"**

---

## Internal Topology

```text
                                Background Reconciliation Loop
                                              │
    ┌─────────────────────────────────────────┼─────────────────────────────────────────┐
    ▼                                         ▼                                         ▼
Phase 1: Bootstrap                 Phase 2: Server Connect                   Phase 3: Resolver
HostsFileSource (/etc/hosts)       DnsServerTarget (IP / Host)               DnsResolverProvider
ResolvConf (/etc/resolv.conf)      StaticServerProvider                      UdpDnsTransport (RFC 1035 UDP)
(Zero disk I/O at runtime)         (Zero hardcoded nameservers)              SystemDnsTransport (OS Fallback)
    │                                         │                                         │
    │                                         │                              Singleflight Coalescing
    │                                         │                              Failover & LKG Resiliency
    └─────────────────────────────────────────┼─────────────────────────────────────────┘
                                              │
                                              ▼
                                   Phase 4: In-Memory Cache
                                           DnsCache
                              (Single Source of Truth in RAM)
                                ├── Positive Cache (TTL: 30s)
                                └── Negative Cache (NXDOMAIN: 5s)
                                              │
                                              ▼
                              Discovery.update_endpoints(...)
                                              │
                                              ▼
                                    ArcSwap<EndpointSet>
                                              │
                    ══════════════════════════╪══════════════════════════
                                              │ Request Hot Path (Serving)
                                              ▼
                                 discovery.current_endpoints()
                                    [ 20.0 ns | 0 Allocations ]
```

---

## Resolution & Hot-Path Serving Flow

```text
                     Request Ingress (Hot Path)
                                 │
                                 ▼
                   discovery.current_endpoints()
                                 │
                                 ▼
                     Arc<EndpointSet> Snapshot
                    (20.0 ns, 0 heap allocs)
                                 │
           ──────────────────────┼──────────────────────
                                 │ Background Sync Task
                                 ▼
                   resolver.resolve(host, port)
                                 │
            ┌────────────────────┴────────────────────┐
         [HIT]                                     [MISS]
            │                                         │
            ▼                                         ▼
    DnsCache.get(host)                       HostsFileSource (/etc/hosts)
    (53.7 ns, zero wire I/O)                          │
            │                                  ┌──────┴──────┐
            │                               [HIT]         [MISS]
            │                                  │             │
            │                         Insert DnsCache        ▼
            │                         (hosts_ttl: 300s) Singleflight Mutex
            │                                  │             │
            │                                  │    Execute Wire Query
            │                                  │    (UdpDnsTransport RFC 1035)
            │                                  │             │
            │                                  │      ┌──────┴──────┐
            │                                  │   [SUCCESS]     [OUTAGE]
            │                                  │      │             │
            │                                  │  Positive Cache   LKG Fallback
            │                                  │  (TTL: 30s)       (10.3M ops/s)
            └──────────────────────────────────┴──────┬─────────────┘
                                                      │
                                                      ▼
                                       EndpointSet::new(endpoints, gen)
                                                      │
                                                      ▼
                                           current.store(new_set)
```

---

## Component Roles

| Component | Responsibility | Invariant |
| :--- | :--- | :--- |
| [`DnsCache`](src/dns/cache.rs) | Single Source of Truth in RAM | Sub-microsecond cache hits; negative cache shield prevents query storms |
| [`HostsFileSource`](src/dns/bootstrap.rs) | `/etc/hosts` in-memory table | Eager zero-IO bootstrap; strictly $\mathcal{O}(1)$ lookup complexity |
| [`ResolvConfServerProvider`](src/dns/bootstrap.rs) | `/etc/resolv.conf` parser | Zero hardcoded nameservers; extracts upstream DNS dynamically |
| [`DnsServerTarget`](src/dns/server.rs) | Nameserver target resolution | Resolves IP literals and Host:Port targets via bootstrap hosts without deadlock |
| [`UdpDnsTransport`](src/dns/transport.rs) | RFC 1035 UDP wire transport | Pure zero-dependency wire serialization; dual A/AAAA query; atomic TX ID |
| [`SystemDnsTransport`](src/dns/transport.rs) | OS fallback resolver | Delegates domain resolution to `tokio::net::lookup_host` |
| [`DnsResolverProvider`](src/dns/resolver.rs) | Phase 3 resolver coordinator | Singleflight deduplication; sequential nameserver failover; bounded LKG resilience; `for_tier` adaptive sizing |
| [`Endpoint`](src/endpoint.rs) | Canonical physical destination | Re-exported from `velda-core`; strictly represents `address: SocketAddr` + `weight` |
| [`EndpointSet`](src/endpoint.rs) | Immutable generational snapshot | Generation-tracked topology; lock-free iteration via `.ips()` and `.addresses()` |
| [`Discovery`](src/service.rs) | Top-level lifecycle orchestrator | Background refresh task; lock-free `ArcSwap` snapshot publication |

---

## Adaptive Resource Tiers (`DnsResolverConfig::for_tier`)

To adapt seamlessly from constrained edge routers (256 MB RAM) to high-throughput bare-metal edge servers (128 GB RAM):

| Profile | Target Environment | DNS Cache Capacity | Bounded LKG Capacity | Query Timeout |
| :--- | :--- | :--- | :--- | :--- |
| **Constrained** | RAM < 512 MB (IoT gateways, OpenWrt, CPE) | **1,000 entries** | **500 entries** | 3.0s |
| **Small** | RAM 512 MB – 2 GB (Micro VMs, Raspberry Pi 4/5) | **10,000 entries** | **2,000 entries** | 2.0s |
| **Medium** | RAM 2 GB – 8 GB (Standard edge compute nodes) | **50,000 entries** | **10,000 entries** | 2.0s |
| **Large** | RAM 8 GB – 32 GB (Regional edge nodes, Telco MEC) | **150,000 entries** | **30,000 entries** | 1.5s |
| **XLarge** | RAM 32 GB – 64 GB (Heavy edge compute instances) | **400,000 entries** | **80,000 entries** | 1.0s |
| **2XLarge** | RAM 64 GB – 128 GB (High-density edge clusters) | **1,000,000 entries** | **200,000 entries** | 1.0s |
| **Ultra** | RAM > 128 GB (Massive bare-metal datacenters) | **2,500,000 entries** | **500,000 entries** | 1.0s |

---

## Benchmark Highlights

- **Zero-Allocation In-Memory Lookups**: **0 Bytes / 0 Heap Allocations** across Positive Cache Hit, Negative Hit, and Cache Miss.
- **Hot-Path Snapshot Load**: **20.24 ns** latency (**49.41 Million ops/s** throughput, 0 allocs).
- **Positive Cache Hit**: **53.81 ns** (**18.58 Million ops/s** in-memory throughput, **0.00 allocs / 0.00 B**).
- **Negative Cache Shield (NXDOMAIN)**: **76.38 ns** (**13.09 Million ops/s**, **0.00 allocs / 0.00 B**; 100% of query storms absorbed in RAM with **zero upstream wire packets**).
- **Cache Miss**: **55.11 ns** (**18.14 Million ops/s**, **0.00 allocs / 0.00 B**).
- **Hosts File Lookup Scaling**: Strictly $\mathcal{O}(1)$ (flat 30 - 49 ns from $N = 10$ to $10,000$ entries).
- **Direct IP Target Resolution**: **7.14 ns** (**139.99 Million ops/s**, 0 heap allocs).
- **Multicore Concurrency Scaling**: **18.75 Million ops/s** single-thread scaling to **9.74 Million ops/s** under 64-thread contention.
- **Lock-Free Readers Under Refresh**: **25.77 Million ops/s** read throughput while background writer continuously updates snapshots.
- **Singleflight Stampede Defense**: **99.5% coalescing ratio** under concurrent storm (199 / 200 tasks coalesced into 1 upstream query).
- **Resilient Failover**: Primary nameserver failure automatically fails over to secondary backup in **~2.07 ms** (100% success).
- **Total Nameserver Blackout (LKG Active)**: Serves uninterrupted at **10.16 Million ops/s** (**98.41 ns**, 2 allocs) via RFC 5861 `stale-if-error` grace period caching.
- **Zero Memory Leaks Audit**: Net delta **0 B / 0 allocs** across 4 lifecycle stages in `memory_leak_bench`.
- **Wire Parsing Performance**: RFC 1035 packet encoding (**16.2 ns**, 0 B) and pointer decompression (**23.8 ns**, 0 B) in `wire_bench`.

---

## Verification

```bash
# Code Style & Lints
cargo fmt --check
cargo clippy -p velda-discovery --all-targets --all-features -- -D warnings

# Unit & Integration Tests (39 tests)
cargo test -p velda-discovery

# Performance, Scalability & Resilience Benchmarks (5 suites)
cargo bench -p velda-discovery --bench single_thread_bench
cargo bench -p velda-discovery --bench multi_thread_bench
cargo bench -p velda-discovery --bench adversarial_bench
cargo bench -p velda-discovery --bench memory_leak_bench
cargo bench -p velda-discovery --bench wire_bench
```
