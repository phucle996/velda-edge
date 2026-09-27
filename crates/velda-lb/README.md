# velda-lb

`velda-lb` is the high-performance load balancing subsystem of the Velda Edge Data Plane.

It is dedicated to answering a single routing question: **"Which candidate endpoint should serve this request?"**

> [!IMPORTANT]
> **Subsystem Invariants**:
> - **Zero-Copy & Zero Heap Allocation**: Operates directly over borrowed `&[Endpoint]` slices and returns borrowed `Option<&Endpoint>`, index `usize`, or physical `Option<SocketAddr>`.
> - **In-Memory Pure Algorithms**: Zero disk I/O, zero network calls, zero JSON parsing during selection.
> - **Per-Upstream Isolated Ownership**: Balancer instances are owned independently per upstream entity, eliminating cross-service traffic interference and CPU cache-line contention.
> - **Ahead-of-Time In-RAM Binding**: Algorithms are instantiated and wired into memory at startup or hot-reload; hot-path serving invokes direct memory pointers with zero string matching or dictionary lookup overhead.
> - **Lock-Free Concurrency**: State-heavy consistent hashing algorithms (Maglev, RingHash) utilize atomic pointer swaps (`ArcSwap`) to eliminate read lock contention across multicore workers.

---

## 1. Architectural Soul: Isolated Ownership & In-RAM Dispatch

The core design philosophy of `velda-lb` centers on two fundamental principles:

### Per-Upstream Isolated Instance Ownership
In Velda Edge, `velda-lb` does **not** operate as a shared global singleton registry where all services compete for a single balancer instance:
- **No Cross-Talk / Traffic Contamination**: Each logical upstream owns its own dedicated balancer instance. Traffic surges or requests to one service cannot perturb, skip, or scramble the cyclic sequence, weighted distribution, or hash tables of another service.
- **Dedicated Algorithm per Upstream**: Services independently select their optimal strategy (e.g. Round-Robin for stateless APIs, Power-of-Two-Choices for latency-sensitive services, Maglev Consistent Hashing for stateful cache tiers).
- **Zero Cache-Line Contention (False Sharing)**: CPU cores serving different upstreams update isolated atomic counters in memory, eliminating multi-core cache-line bouncing.

### Ahead-of-Time In-RAM Memory Dispatch
- **Zero String Matching on Hot Path**: Configuration strings (e.g. `"round_robin"`, `"maglev"`) are parsed and validated strictly during bootstrap or configuration reload. The corresponding algorithm struct is instantiated into RAM once and held directly by the `Upstream` entity.
- **Direct Pointer Execution**: When a request arrives, the router resolves the `Upstream` in RAM, which immediately calls its owned balancer via direct memory pointer or vtable dispatch (~2 ns). Request serving executes with zero string comparison, zero dictionary lookups, and zero allocations.

---

## 2. Rationale on `ConnectionKey` Decoupling

In Velda Edge, `velda-lb` intentionally has **zero awareness** of `ConnectionKey`.

- **Pure Physical Selection**: The load balancer's sole responsibility is algorithmic selection—given candidate endpoints, determine which physical destination (`SocketAddr`) receives the request.
- **Transport & Protocol Independence**: A `ConnectionKey` requires transport protocol details (`"http1"`, `"http2"`, `"tcp"`) and TLS metadata (`SNI`, `ALPN`). The load balancer does not and should not know whether the request is HTTP/1.1, HTTP/2, or what SNI is configured.
- **Architectural Boundary**: The caller (`velda-upstream`) receives the physical `SocketAddr` from `velda-lb` via [`select_addr`](src/balancer.rs), combines it with its own configured protocol and TLS settings to construct the `ConnectionKey`, and queries the connection pool (`velda-connection-pool`). This keeps `velda-lb` pure, stateless, and free from transport coupling.

---

## 3. Supported Load Balancing Algorithms

- **Round-Robin** ([`src/algorithm/round_robin.rs`](src/algorithm/round_robin.rs)): Atomic cyclic distribution across backends (~9 ns).
- **Smooth Weighted Round-Robin** ([`src/algorithm/weighted_round_robin.rs`](src/algorithm/weighted_round_robin.rs)): Smooth interleaving respecting backend weights without clustering.
- **Power of Two Choices (P2C)** ([`src/algorithm/p2c.rs`](src/algorithm/p2c.rs)): Samples 2 random candidates and routes to the least loaded, avoiding thundering herd.
- **Least Connections & Least Requests** ([`src/algorithm/least_conn.rs`](src/algorithm/least_conn.rs)): Routes to the backend with minimum active connections or in-flight requests.
- **Peak EWMA** ([`src/algorithm/peak_ewma.rs`](src/algorithm/peak_ewma.rs)): Response latency moving average combined with in-flight penalty to minimize tail latency.
- **Google Maglev Consistent Hashing** ([`src/algorithm/maglev.rs`](src/algorithm/maglev.rs)): O(1) lookup table (65,537 slots) providing consistent session/cache affinity.
- **Consistent Hash Ring** ([`src/algorithm/ring_hash.rs`](src/algorithm/ring_hash.rs)): Ketama-style binary search ring with virtual nodes proportional to endpoint weights.
- **Random & Weighted Random** ([`src/algorithm/random.rs`](src/algorithm/random.rs)): Ultra-fast (~2 ns) thread-local PRNG with zero syscalls.
- **IP & Generic Hash** ([`src/algorithm/hash.rs`](src/algorithm/hash.rs)): Deterministic 64-bit FNV-1a hashing over client IP, request headers, cookies, or path.

---

## 4. Performance Summary

Detailed empirical verification results are documented in [`benchmark.md`](benchmark.md).

Highlights:
- **Zero Allocations**: 0.00 bytes allocated across all algorithms on request hot paths.
- **Sub-Microsecond Latency**: RoundRobin runs in 9.19 ns; Random runs in 2.63 ns; Maglev consistent hash runs in 25.64 ns.
- **Multicore Scalability**: Reaches 121.4 Million operations per second on 64 worker threads.

---

## 5. Verification Commands

Run standard quality gate checks from the repository root:
- Linter: `cargo clippy -p velda-lb --all-targets --all-features -- -D warnings`
- Tests: `cargo test -p velda-lb`
- Benchmarks: `cargo bench -p velda-lb`
