# velda-discovery

`velda-discovery` is Stage 1 of the backend lifecycle in the Velda Edge Data Plane.

It is dedicated to answering a single topological question: **"Where are the backends located?"**

> [!IMPORTANT]
> **Stage 1 Invariants**:
> - **Topology Only**: Discovery resolves and tracks backend IP addresses and ports (`EndpointSet`). It has no awareness of health checks, circuit breakers, draining states, or load balancing algorithms.
> - **Zero DNS on Request Hot Path**: Request serving reads lock-free in-memory snapshots (`ArcSwap<EndpointSet>`). DNS queries execute strictly within background refresh tasks.
> - **Unified Cache as Single Source of Truth**: All resolved addresses (from static `/etc/hosts` or upstream DNS queries) are cached in `DnsCache`.
> - **Zero Hardcoded Nameservers**: Upstream nameservers are resolved dynamically from `/etc/resolv.conf` or user configuration manifests.
> - **Resilient LKG Fallback**: If upstream nameservers fail, time out, or return errors, the Last-Known-Good (LKG) endpoint set is preserved.
> - **Negative Caching**: NXDOMAIN responses are cached with a dedicated negative TTL to prevent DNS query storms.

---

## 1. Architectural Role in the 4-Stage Pipeline

```text
                RoutePlan
                   │
                UpstreamId
                   │
                   ▼
┌──────────────────────────────┐
│ 1. velda-discovery           │
│                              │
│ DNS / static discovery       │
│ resolver + in-memory cache   │
└──────────────┬───────────────┘
               │
          EndpointSet (Topology)
               │
               ▼
┌──────────────────────────────┐
│ 2. velda-upstream            │
│                              │
│ logical upstream domain      │
│ endpoint health & state      │
└──────────────────────────────┘
```

---

## 2. Core Capabilities

- **Bootstrap Fast Path**: Prioritizes local `/etc/hosts` entries for zero-network resolution of cluster-local names.
- **Unified In-Memory Cache**: Positive caching respects TTL; negative caching prevents query storms during transient outages or typos.
- **Nameserver Failover**: Queries primary and secondary nameservers sequentially before engaging LKG fallback.
- **Lock-Free Atomic Updates**: Publishes generation-tracked `EndpointSet` snapshots via `ArcSwap` with zero read locks.

---

## 3. 4-Phase DNS Subsystem Architecture

The DNS discovery subsystem is structured cleanly into 4 sequential processing phases across 4 dedicated files:

- **Phase 1: Bootstrap** ([`src/dns/bootstrap.rs`](src/dns/bootstrap.rs)): Eagerly loads local `/etc/hosts` ([`HostsFileSource`](src/dns/bootstrap.rs)) and `/etc/resolv.conf` ([`ResolvConfServerProvider`](src/dns/bootstrap.rs)) ahead-of-time into memory.
- **Phase 2: DNS Server Connect** ([`src/dns/server.rs`](src/dns/server.rs)): Manages upstream DNS nameservers ([`DnsServer`](src/dns/server.rs)), target specifications ([`DnsServerTarget`](src/dns/server.rs)), provider contracts ([`DnsServerProvider`](src/dns/server.rs), [`StaticServerProvider`](src/dns/server.rs)), and UDP socket connectivity.
- **Phase 3: Resolver** ([`src/dns/resolver.rs`](src/dns/resolver.rs)): Coordinates wire transport ([`DnsTransport`](src/dns/resolver.rs)), sequential nameserver failover, and Last-Known-Good (LKG) fallback ([`DnsResolverProvider`](src/dns/resolver.rs)).
- **Phase 4: Cache** ([`src/dns/cache.rs`](src/dns/cache.rs)): Provides the in-memory Single Source of Truth ([`DnsCache`](src/dns/cache.rs), [`CacheLookup`](src/dns/cache.rs)) with positive TTL and negative TTL (NXDOMAIN protection).

Additional components:
- [`Endpoint`](src/endpoint.rs) and [`EndpointSet`](src/endpoint.rs): Canonical topological destinations re-exported from `velda-core`.
- [`Discovery`](src/service.rs) and [`DiscoveryMode`](src/service.rs): Background reconciliation service managing periodic updates and atomic snapshot swaps.


---

## 4. Integration & Testing

For executable implementations and test verification, refer directly to:

- [`tests/static_test.rs`](tests/static_test.rs): Explicit static endpoint discovery and snapshot retrieval.
- [`tests/dns_test.rs`](tests/dns_test.rs): Fast-path bootstrap, nameserver failover, positive/negative caching, and LKG fallback under server failure.

---

## 5. Performance Benchmarks

Detailed empirical benchmarks measuring allocations, latency, and Big-O scaling are documented in:

- [`benchmark.md`](benchmark.md): Comprehensive empirical benchmark report across 6 stages.
- [`benches/discovery_bench.rs`](benches/discovery_bench.rs): Executable benchmark suite.

Summary of Results:
- **Hot-Path Snapshot Read** (`current_endpoints()`): 20.34 ns, 0.00 allocations (49.1M ops/s).
- **Direct IP Target Resolution**: 7.81 ns, 0.00 allocations (128.0M ops/s).
- **In-Memory DnsCache Hit**: 79.02 ns (12.6M ops/s).
- **Negative Cache Shield** (NXDOMAIN): 76.73 ns (13.0M ops/s).
- **Static Hosts Lookup Scaling**: Verified O(1) across N = 10 .. 10,000 domains.
- **Multicore Concurrency (64 Workers)**: 9.53M ops/s aggregate throughput (104.89 ns).
- **End-to-End Cached Resolution**: 151.54 ns (6.6M ops/s).

---

## 6. Verification Commands

Run standard quality gate checks from the repository root:

- Formatting: `cargo fmt --check`
- Linter: `cargo clippy -p velda-discovery --all-targets --all-features -- -D warnings`
- Tests: `cargo test -p velda-discovery`
- Benchmarks: `cargo bench -p velda-discovery`

