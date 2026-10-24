# Velda Edge — Architectural Issues in Modern Proxies & How Velda Resolves Them

This document specifies the critical architectural flaws, operational bottlenecks, and design traps found in traditional edge proxies (**NGINX**, **Envoy**, **HAProxy**, **Pingora**, **Kong**, and **Traefik**), and documents how **Velda Edge** was engineered from first principles to resolve each issue.

---

## 1. Executive Summary: The Legacy Proxy Crisis

For decades, the edge infrastructure ecosystem has relied on first- and second-generation proxies:
- **First Generation (NGINX, HAProxy)**: C-based, event-driven, multi-process architectures designed for plain HTTP/1.1 and static file serving.
- **Second Generation (Envoy, Traefik, Kong)**: Cloud-native, microservice-oriented service meshes and API gateways designed for dynamic configuration and polymorphic filter chains.
- **Modern Rust Frameworks (Pingora)**: Low-level async Rust libraries providing primitives for custom proxy construction.

While these systems solved critical problems in their time, modern cloud environments (high-churn serverless traffic, multi-protocol pipelines with HTTP/3 and gRPC, dense multi-core CPUs) expose **fundamental architectural issues** in their designs.

Velda Edge does not seek to clone these systems. Instead, it was designed specifically to confront and eliminate their failure modes.

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│                   THE 6 ARCHITECTURAL ISSUES IN MODERN PROXIES                   │
├──────────────────────────────────────────────────────────────────────────────────┤
│ 1. Dynamic Protocol Sniffing & Filter Chain Bloat   (Envoy, Kong, Traefik)       │
│ 2. Inline Connection Logistics & Lock Contention   (Envoy, Pingora)             │
│ 3. Cold Upstream Thrashing & TIME_WAIT Explosion    (NGINX, Naive Proxies)       │
│ 4. Hot-Path Disk, Database & JSON I/O              (Kong, APISIX)               │
│ 5. Leaky Protocol Abstractions (gRPC vs HTTP)       (Envoy, NGINX)               │
│ 6. Cross-Core Cache Ping-Pong in Work Stealing      (Tokio Default, Thread Pools)│
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Detailed Breakdown: Issues Faced & Velda Edge Resolutions

---

### Issue 1: Dynamic Protocol Sniffing & Filter Chain Bloat
*(Seen in Envoy, Kong, Traefik)*

#### What We Faced in Other Proxies
In systems like **Envoy** and **Kong**, incoming traffic flows through deeply nested, polymorphic filter chains (`Box<dyn Filter>`, `HandlerChain`, virtual function tables):
- To support multiple protocols, the proxy frequently inspects payload bytes, dynamic headers (e.g. sniffing `Content-Type: application/grpc`), or performs dynamic ALPN downcasting at runtime.
- Every request—even a simple static ping or high-speed L4 pass-through—pays the overhead of dynamic dispatch, heap allocations for generic request contexts (`HashMap<String, Box<dyn Any>>`), and CPU branch mispredictions.
- Adding features turns the central routing layer into a **"Fat Waist"**, where routing logic is tightly coupled with transformations, authentication, and wire decoding.

#### How Velda Edge Resolves It: The Hourglass Architecture
Velda Edge enforces **Declared Protocols** over dynamic sniffing and adopts the **Hourglass Model**:
1. **Pre-Bound Execution**: Every listener (`ListenerId`) declares its application protocol (`http`, `grpc`, `raw`) in configuration. At cold-start, it binds directly to a specialized runner (`DedicatedTcpRunner` or `DedicatedUdpRunner`).
2. **Zero Sniffing on Hot Path**: The request serving path never inspects payload bytes or headers to dynamically guess protocols. HTTP traffic never evaluates gRPC rules; gRPC traffic never executes HTTP routes.
3. **Razor-Thin Narrow Waist**: The Router does not transform bodies or allocate dynamic filter chains. It computes a single pure-RAM mathematical projection:
   $$\text{Router}(\text{listener\_id}, \text{request\_head}) \longrightarrow \text{upstream\_id}$$
4. **Flat Workflows**: Replaced OOP handler chains with linear, contiguous workflows (`decode -> pre_route -> route -> pre_upstream -> upstream -> post_response -> encode`) without virtual function indirection.

---

### Issue 2: Inline Connection Logistics & Lock Contention
*(Seen in Envoy, Pingora)*

#### What We Faced in Other Proxies
In **Envoy** and **Pingora**, when a frontline worker serving downstream traffic needs an upstream connection:
- If pool capacity is depleted, the serving thread is forced to synchronously create sockets, execute system calls (`socket()`, `connect()`), or perform cryptographic handshakes (TLS 1.3 key exchange) **inline on the request hot path**.
- In multi-threaded connection pools, worker threads must coordinate over shared locks, or dispatch tasks across thread boundaries via event loops (`Dispatcher::post`).
- Under traffic surges, this causes severe lock contention, thread context-switching overhead, and tail-latency ($p99$) spikes.

#### How Velda Edge Resolves It: Frontline vs. Shadow Logistics Separation
Velda Edge establishes an inviolable architectural boundary between traffic serving and connection logistics:
1. **Frontline Traffic Worker (`velda-worker-{k}`)**:
   - Pinned to CPU core $k$. Exclusively serves downstream traffic on the hot path.
   - Hot-path connection acquisition (`upstream.acquire()`) is **100% lock-free and takes $< 10\text{ ns}$**: it simply pops an idle handle from its private lane shard $k$.
   - **Strict Invariant**: The Frontline Worker is strictly forbidden from spawning warmup tasks, boxing future closures, or waiting on logistics tasks on the hot path.
2. **Shadow Logistics Worker (`velda-shadow-{k}`)**:
   - An autonomous background companion thread pinned to the same CPU core $k$, running under Linux `SCHED_IDLE` (lowest thread priority).
   - Operates sequentially on its assigned lane ($k$), executing `prewarm_lane(k)` at startup, upon config reload, and periodically every 2 seconds.
   - Performs all proactive socket allocations, TLS 1.3 handshakes, and deposits pre-warmed connection handles into the lane shard.

---

### Issue 3: Downstream Connection Storms & Cold Upstream Thrashing
*(Seen in NGINX, Naive Reverse Proxies)*

#### What We Faced in Other Proxies
In **NGINX** (multi-process architecture) and standard reverse proxies:
- Upstream connection pooling across worker processes requires complex shared-memory slabs (`ngx_slab`), which frequently suffers from lock contention (`ngx_shmtx_lock`).
- When downstream clients generate aggressive connection churn (browsers, mobile disconnects, microservices scale-from-zero, or L7 DDoS with `--disable-keepalive`), naive proxies open a new backend connection for each downstream connection.
- **The Result**: The backend application server is overwhelmed by **hundreds of thousands of `TIME_WAIT` sockets**, ephemeral port depletion (`EADDRNOTAVAIL`), and kernel SYN backlog packet drops. The proxy effectively weaponizes downstream churn against its own backend.

#### How Velda Edge Resolves It: Absolute Connection Decoupling & Lane-Striped Pooling
1. **Complete Lifecycle Isolation**: Downstream socket closure (client disconnect, no keep-alive) is strictly intercepted at the gateway. The upstream connection is **never closed**; it is immediately returned to the lane pool.
2. **Lane-Striped Keep-Alive Preservation**: Upstream connections remain permanently established (`ESTABLISHED`) and warm.
3. **Empirical Validation (Azure Cluster)**:
   - When downstream was bombarded with **168,398 new TCP connections at 11,229 conn/s** and **37,875 full TLS 1.3 handshakes at 2,530 hs/s**:
   - The Upstream Backend experienced **`TIME-WAIT = 0`** and a perfectly steady pool of **~108 to 112 established connections**. The backend CPU remained under 5%.

---

### Issue 4: Dynamic JSON Parsing, Database Lookups & Disk I/O on Hot Path
*(Seen in Kong, APISIX)*

#### What We Faced in Other Proxies
In Lua/OpenResty-based proxies like **Kong** and **APISIX**:
- Plugins and routing engines frequently execute runtime JSON schema validation, dynamic regex compilation, or query external datastores (PostgreSQL, etcd) upon cache misses.
- Lua's garbage collector (LuaJIT GC) induces periodic latency spikes ("stop-the-world" pauses) when high throughput allocates thousands of temporary Lua tables.
- Route matching depends on dynamic table lookups that can degrade from $O(1)$ to $O(N)$ as route counts grow.

#### How Velda Edge Resolves It: Zero-IO Serving Invariant
Velda Edge enforces a strict contract across all hot-path code (`crates/velda-edge/`):
1. **Hot Path Zero-IO**: During request serving, the Data Plane **NEVER**:
   - Parses JSON strings.
   - Performs disk file I/O.
   - Makes RPC / HTTP calls to the Control Plane.
   - Compiles configuration or regexes.
2. **Pre-Compiled RAM Snapshots (`RuntimeSnapshot`)**:
   All routing tables, upstream endpoints, TLS certificates, and plugin pipelines are compiled, validated, and optimized into contiguous in-memory structures by `velda-config` and `velda-sync` prior to activation.
3. **Atomic Pointer Swapping**: Updates from the Go Control Plane are applied atomically via `ArcSwap`, taking $< 5\text{ ns}$ with zero disruption to in-flight requests.

---

### Issue 5: Leaky Protocol Abstractions (Treating gRPC as HTTP/2)
*(Seen in Envoy, NGINX)*

#### What We Faced in Other Proxies
Traditional proxies treat **gRPC** merely as an HTTP/2 sub-type with a special header (`Content-Type: application/grpc`):
- Errors generated by the proxy (e.g. 502 Bad Gateway, 504 Gateway Timeout, 404 Route Not Found) are emitted as HTTP/1.1 or HTTP/2 HTML/JSON status bodies instead of canonical gRPC trailers.
- Mobile and microservice gRPC clients fail with deserialization errors (`ParseError: unexpected character '<'`) instead of receiving typed gRPC status codes (`UNAVAILABLE`, `DEADLINE_EXCEEDED`).
- Trailing metadata (`grpc-status`, `grpc-message`) is frequently dropped or mishandled during protocol conversions.

#### How Velda Edge Resolves It: Dedicated, Decoupled Protocol Subsystems
1. **Isolated gRPC Crate (`velda-grpc`)**: Completely decoupled from HTTP crates (`velda-http1`, `velda-http2`, `velda-http3`).
2. **Length-Prefixed Framing Engine**: Native 5-byte header parsing (`1-byte compressed flag + 4-byte big-endian length`) directly from network buffers without intermediate HTTP string representations.
3. **Canonical gRPC Status Engine**: If an upstream endpoint fails or a route is missing, Velda Edge natively generates HTTP/2 trailers containing standard gRPC wire status codes (`grpc-status: 14`, `grpc-status: 5`) with zero HTML bleed.

---

### Issue 6: Cross-Core Cache Ping-Pong in Work-Stealing Runtimes
*(Seen in default multi-threaded async frameworks)*

#### What We Faced in Other Proxies
Standard multi-threaded async runtimes (such as default Tokio or Go's M:N scheduler) employ global work-stealing queues:
- A connection accepted on Core 0 can have its subsequent packets processed by Core 1, then Core 3.
- Every thread switch invalidates the CPU's private **L1 Data Cache (32 KiB)** and **L2 Cache (512 KiB - 1 MiB)**.
- Memory bus cache coherency protocols (MESI/MOESI) spend heavy cycles synchronizing cache lines between CPU cores ("Cache Ping-Pong").

#### How Velda Edge Resolves It: Thread-Per-Core Pinning with Lane Striping
1. **OS Thread Affinity**: Every Frontline Worker and Shadow Worker is explicitly pinned to a specific physical/logical core (`pthread_setaffinity_np`).
2. **Lane-Striped Data Structures**: Connection pools, buffer allocators, and statistics counters are sharded by `core_id`.
3. **L1/L2 Cache Locality**: A request arriving on Core $k$ is decoded, routed, and forwarded entirely within Core $k$, preserving hot CPU caches and achieving maximum hardware instruction throughput.

---

## 3. Comprehensive Comparison Matrix

| Architectural Dimension | NGINX | Envoy | Kong / APISIX | Pingora | **Velda Edge** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Language & Safety** | C (Memory unsafe) | C++ (Complex memory model) | Lua / C / OpenResty | Rust (Safe async) | **Rust 1.98 (Edition 2024, Memory safe)** |
| **Request Hot Path** | Event-driven C callbacks | Deep Polymorphic Filter Chain | Dynamic Lua Handler Chain | Async Rust Trait hooks | **Flat Workflow (Contiguous, Zero-Abstraction)** |
| **Protocol Identification** | Port / Config-based | Dynamic Sniffing & Matching | Dynamic Lua Header Inspection | Custom Code | **Declared Protocol Ingress (Zero Sniffing)** |
| **Connection Pooling** | Multi-process shared memory | Multi-threaded global pool | NGINX upstream / lua-resty | Thread-shared pool | **Lane-Striped (Per-Core Shards, Lock-Free)** |
| **Upstream Pre-Warming** | None (Cold connect on demand) | Inline / Thread dispatcher | None (On demand) | Ad-hoc custom | **Autonomous Shadow Worker (SCHED_IDLE)** |
| **Upstream Churn Shield** | Poor (Requires keepalive tuning)| Moderate | Poor | Moderate | **100% Shielded (TIME-WAIT = 0 on Backend)** |
| **gRPC Architecture** | Layered on HTTP/2 module | Layered on HTTP/2 | Lua plugin transformation | Low-level H2 | **Dedicated `velda-grpc` Subsystem & Native Trailers** |
| **Configuration Ingestion**| Disk read / reload process | Dynamic xDS gRPC | Postgres / etcd polling | Static / Code | **RAM Snapshots (ArcSwap, Zero-IO Hot Path)** |
| **CPU Core Affinity** | Optional worker affinity | Multi-threaded work stealing | Optional worker affinity | Multi-threaded work stealing | **Strict Frontline + Shadow Core Pinning** |

---

## 4. Summary & North Star Contract

Velda Edge's superior performance is not the result of magic micro-optimizations or unsafe compiler flags. It is the natural consequence of **eliminating the 6 structural bottlenecks** that have burdened edge proxy design for the past two decades:

1. **Protocol Clarity over Sniffing**: Know what protocol is running before the first byte is parsed.
2. **Hot-Path Simplicity over Ubiquity**: Never execute logistics, allocations, or locking on the traffic path.
3. **Decoupled Lifecycles**: Protect backends from downstream connection volatility with lane-striped pooling.
4. **Zero I/O in Serving**: Compile everything into RAM before traffic arrives.
5. **Native Protocol Respect**: Treat gRPC as gRPC, HTTP as HTTP, and L4 as L4.
6. **Hardware Harmony**: Pin execution to CPU cores to exploit hardware cache hierarchies.
