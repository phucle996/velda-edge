# JWT Authentication Plugin (`jwt_auth`) — Performance Benchmark Report

```text
+-----------------------------------------------------------------------------------+
|                        JWT Authentication Benchmark Topology                      |
|                                                                                   |
|  [Traffic Generator (Client Node)]          [Edge Gateway Node (velda-edge)]      |
|   - wrk2 / k6 Load Generator                 - 4 Frontline Serving Cores (Pinned) |
|   - 64–256 TCP Keep-Alive Connections        - Dual Cache (Positive / Negative)   |
|   - Bearer JWT Request Flooding     ───────► - Contiguous 8-Slot JwksKeyTable     |
|                                              - Zero Heap Allocations on Hot Path  |
|                                                              │                    |
|                                                              ▼ Forward            |
|                                             [Backend Upstream (vm2:8081)]         |
|                                              - Mock Upstream (/healthz, 4B pong)  |
+-----------------------------------------------------------------------------------+
```

---

## 1. Test Environment & Hardware Specification

The performance measurement harness assesses the cryptographic verification capacity, throughput scaling, latency percentiles, and hardware cache efficiency of the `jwt_auth` plugin.

### 1.1 Node Hardware Profile

| Hardware Attribute | Specification |
| :--- | :--- |
| **CPU Architecture** | x86_64 (AVX2 / AVX-512 Support) |
| **Core Topologies** | 8 Physical Cores (Pinned 1:1, Hyperthreading disabled) |
| **L1D Cache** | 32 KiB / 48 KiB per core (64-byte line size) |
| **L2 Cache** | 1 MiB / 1.25 MiB per core |
| **L3 Cache (Shared)**| 16 MiB – 32 MiB Unified Cache |
| **Memory** | 32 GiB DDR4/DDR5 ECC RAM |
| **Operating System** | Linux Kernel 6.8+ (x86_64, `PREEMPT_NONE` / Datacenter profile) |
| **Memory Allocator** | Jemalloc (Static 64-byte alignment profile) |

### 1.2 System Tuning & Network Stack

- `RLIMIT_NOFILE`: `1048576` (unlimited file descriptors)
- `net.core.somaxconn`: `65535`
- `net.ipv4.tcp_max_syn_backlog`: `65535`
- TCP Kernel Optimizations: `TCP_NODELAY`, `SO_REUSEPORT` enabled on listener sockets.

---

## 2. Benchmark Scenarios & Evaluation Profiles

### Scenario 1: Baseline Route (Zero Plugins Active)
- **Objective**: Measure the pure edge proxy transport capacity without authentication overhead.
- **Traffic**: Valid HTTP/1.1 requests forwarded directly to the backend upstream.

### Scenario 2: Dual Cache Positive Hit (Fast Path)
- **Objective**: Measure request throughput and latency when identical valid tokens hit the 64-shard positive cache (`cache_ttl_secs > 0`).
- **Mechanism**: Cryptographic signature verification is bypassed; claims are extracted from pre-built header buffers.

### Scenario 3: Cold Verification — Single Static Key (Slot 0 Hit)
- **Objective**: Measure cryptographic verification throughput with caching disabled (`cache_ttl_secs = 0`), testing the fast-path single-key verification where the token's key identifier matches Slot 0 immediately.

### Scenario 4: Multi-Key Table Traversal Depth (Slot 0 vs Slot 3 vs Slot 7 Hit)
- **Objective**: Quantify the latency variance when tokens match different slots in the 8-slot table (`JwksKeyTable`), evaluating whether the unrolled loop traversing Cache Line 0 introduces any measurable deviation.

### Scenario 5: Adversarial Rejection Throughput
- **Objective**: Measure the edge gateway's capacity to absorb and reject malformed, unverified, or expired tokens without upstream forwarding.
- **Sub-profiles**:
  - `401 Unauthorized`: Missing `Authorization` header.
  - `401 Unauthorized`: Invalid signature / tampered payload.
  - `401 Unauthorized`: Negative cache hit (fast-fail rejection).
  - `403 Forbidden`: Mismatched `assert_claims`.

### Scenario 6: Active Key Rotation Under Full Load
- **Objective**: Measure throughput and latency stability while the companion `jwks_rotator` plugin actively polls an external IdP and updates the table via atomic pointer swaps (`lock cmpxchgq`) during sustained client traffic.

---

## 3. Load Generation Commands & Profiling Harness

### 3.1 Standard 2-Thread / 64-Connection Profile (`wrk2`)

```bash
# Valid token verification (Clean traffic)
wrk2 -t2 -c64 -d30s -R50000 \
  -H "Authorization: Bearer <VALID_SIGNED_JWT>" \
  http://127.0.0.1:8080/api/v1/orders

# High-concurrency 4-thread / 256-connection burst profile
wrk2 -t4 -c256 -d60s -R100000 \
  -H "Authorization: Bearer <VALID_SIGNED_JWT>" \
  http://127.0.0.1:8080/api/v1/orders
```

### 3.2 Adversarial Flood Generation

```bash
# Malformed token flood (Testing fast reject & negative cache)
wrk2 -t4 -c128 -d30s -R150000 \
  -H "Authorization: Bearer invalid.tampered.token" \
  http://127.0.0.1:8080/api/v1/orders
```

### 3.3 Zero-Allocation Profiling (`dhat` / Valgrind)

```bash
cargo build --release --features dhat-heap
valgrind --tool=dhat target/release/velda-edge --config config.bin
```

---

## 4. Empirical Benchmark Data (To Be Measured)

### 4.1 Throughput & Latency Distributions

| Benchmark Profile | Peak Throughput (RPS) | P50 Latency | P75 Latency | P90 Latency | P99 Latency | P99.9 Latency | Error Rate |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Scenario 1: Baseline Route (Zero Plugins)** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 2: Dual Cache Positive Hit** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 3: Cold Verification (Slot 0 Hit)** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 4: Multi-Key Traversal (Slot 3 Hit)**| `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 4: Multi-Key Traversal (Slot 7 Hit)**| `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 5: Adversarial Rejection (Invalid)** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 5: Negative Cache Hit (Fast Reject)**| `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |
| **Scenario 6: Active Key Rotation Under Load**  | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` |

---

### 4.2 Hardware Resource & Micro-Architectural Metrics

| Metric | Measured Baseline | Clean Traffic Serving | Adversarial Flood | Invariant Contract |
| :--- | :---: | :---: | :---: | :--- |
| **Heap Allocations per Request (Hot Path)** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | **Strictly 0 bytes (Stack Only)** |
| **Heap Deallocations per Request (Hot Path)** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | **Strictly 0 bytes** |
| **Mutex Lock Contention on Serving Path** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | **0 Lock acquisitions (Lock-Free)** |
| **L1D Cache Load Misses per Request** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | Contained within L1D Cache Line 0 |
| **Store Queue Micro-Ops for 8-Key Commit** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | **2 Vector Stores (`vmovaps`)** |
| **Branch Mispredictions on `kid_index`** | `[Pending Run]` | `[Pending Run]` | `[Pending Run]` | Near-zero via unrolled `cmpq` loop |

---

## 5. Result Interpretation Guidelines

1. **Hot-Path Zero-Allocation Invariant & Verification**:
   - The memory profiler must verify 0 heap allocations on the hot path for both cache hits and cache misses. Any non-zero allocation indicates an unintended dynamic heap escape.
2. **Cache Line Traversal Invariant**:
   - The latency delta between Slot 0 and Slot 7 should remain negligible because all 8 integer hashes reside in the exact same 64-byte L1D cache line.
3. **Rotation Isolation**:
   - The throughput delta between *Scenario 3 (Cold Verification)* and *Scenario 6 (Active Key Rotation)* confirms whether background atomic pointer swaps introduce any observable thread contention on frontline workers.
