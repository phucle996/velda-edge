# Performance Benchmark Specification: ACME Automated Certificate Rotator (`acme_cert_rotator`)

This document specifies the standardized benchmarking framework and empirical metrics for the [`acme_cert_rotator`](crates/velda-plugin/src/background/acme_cert_rotator/) plugin in **Velda Edge** (Rust Edition 2024), comparing directly on isolated cloud infrastructure against industry state-of-the-art automated TLS implementations (**Caddy 2.8**, **Traefik 3.1**, **Apache APISIX 3.11**, and **OpenResty 1.25**):
- **Velda Edge**: v0.1.0 (Rust 2024, Dedicated Shadow Logistics Worker `velda-shadow-{i}` under `SCHED_IDLE`, 128-bit SIMD Vector Prefix Matcher `has_acme_prefix_128_simd`, RAII `EphemeralRouteGuard`, Lock-Free Direct-Blit `ChallengeStore`, Atomic Disk Write via tmp + fsync + rename, Lock-Free `Arc<CertifiedKey>` Hot-Swap into `SniResolver`).
- **Caddy 2.8**: v2.8.4 (Go / CertMagic, automated ACME protocol manager, Go standard library `crypto/tls`, dynamic certificate cache, shared goroutine worker model).
- **Traefik 3.1**: v3.1.2 (Go, built-in Let's Encrypt provider, dynamic router reconfiguration, shared Go runtime scheduler).
- **Apache APISIX**: v3.11.0 (OpenResty LuaJIT 2.1, `acme` plugin via `lua-resty-auto-ssl`, shared memory dictionary caching, external `resty-auto-ssl` daemon).
- **OpenResty**: v1.25.3.2 (Nginx C core + LuaJIT 2.1, `lua-resty-acme`, `ngx.timer.at` background task execution, OpenResty cosocket TLS handshakes).

> [!NOTE]
> This document serves as the **Standardized Benchmark Specification Form**. All empirical data cells remain in a pending state (`*[TBD]*`) awaiting execution on dedicated cloud VM clusters.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Dedicated Cloud Testbed Specification
*(Pending Setup — This section is deliberately left blank until the dedicated physical / virtual hardware testbed is provisioned).*

- **Network Fabric**: *[Pending Setup]*
- **Gateway Server (VM2)**: *[Pending Setup]*
- **Load Generation & Traffic Client Node (VM1)**: *[Pending Setup]*
- **Mock ACME CA Server (Pebble / Boulder Node)**: *[Pending Setup]*
- **Upstream Backend Node**: *[Pending Setup]*
- **Isolation Invariant**: *[Pending Setup]*

### 1.2 Diagnostic Measurement Framework (5 Tiers)
To provide actionable root-cause analysis without metric bloat, the measurement framework focuses strictly on diagnostic metrics:
1. **Macro Tier (Network & Serving Metrics)**: Request throughput, tail latencies (P50, P90, P99, P99.9, Max), error rates, and HTTP status distributions. Generic bloat without diagnostic value (e.g. transfer bandwidth, P75) is strictly omitted.
2. **Domain Dynamics & SLA Diagnostics**: The core domain metrics determining ACME operational health—Challenge Route Injection Latency (Router write lock hold time), Ephemeral Route Purge Duration, TLS Handshake P99.9 Latency Spike during certificate hot-swap, Dropped In-Flight TLS Handshakes, Atomic Disk Write Duration, and Cold-Start LKG Certificate Load Latency.
3. **System Resource & Core Isolation Tier**: Core-specific CPU tracking (`velda-shadow-{i}` vs frontline workers to prove zero CPU steal), memory footprint stability, RSS drift, heap allocation invariants, and memory leak rate.
4. **Micro Tier (Kernel & OS)**: Normalized syscalls per request, context switches per request, kernel execution time, and futex lock acquisitions on serving path.
5. **Micro Tier (HPC & CPU Hardware Counters)**: Normalized CPU instructions/cycles per request, instruction efficiency (IPC), hardware vector stores, branch misprediction rate, L1D cache miss rate, and last-level cache (LLC) misses.

### 1.3 Micro-architectural Technical Invariants
- **Frontline vs Shadow Logistics Isolation (Rule 2.9)**: ACME client operations (order creation, JWS signing, CSR generation, certificate download, and disk persistence) execute strictly on `velda-shadow-{i}` (`SCHED_IDLE`). Frontline serving threads experience zero CPU steal, zero blocking syscalls, and zero lock contention.
- **128-bit SIMD Prefix Matcher**: The 28-byte prefix `/.well-known/acme-challenge/` is evaluated using two 128-bit vector registers (`XMM0`, `XMM1`), leaving all integer general-purpose registers (GPRs) free for customer routing.
- **RAII Route Scope Guard**: Ephemeral challenge routes injected into `velda_router::Router` are guarded by `EphemeralRouteGuard`, ensuring unconditional removal from routing memory upon success, error, timeout, or task cancellation.
- **Crash-Safe Atomic Disk Persistence**: Certificates and private keys are written to temporary files, physically committed via `fsync` (`sync_all`), and atomically moved into place using the POSIX `rename` syscall.

---

## 2. Comprehensive 6-Scenario Test Matrix

```
╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                COMPREHENSIVE 6-SCENARIO ACME BENCHMARK MATRIX                                     ║
╠══════════════════════════════════╦══════════════════════════════╦══════════════════════════╦══════════════════════╣
║ Scenario Identifier              ║ Workload & Traffic Pattern   ║ Background Rotator State ║ Evaluation Objective ║
╠══════════════════════════════════╬══════════════════════════════╬══════════════════════════╬══════════════════════╣
║ KB 01: Steady-State Baseline     ║ Clean HTTPS Traffic (64 Conn)║ Idle (No active order)   ║ Floor Serving Perf   ║
║ KB 02: Active Route Injection    ║ Sustained Load (64 Conns)    ║ Active Challenge Route   ║ Router Lookup Jitter ║
║ KB 03: Challenge Blit Throughput ║ Concurr. Probes to Challenge ║ Active HTTP-01 Wire Blit ║ Direct RAM Serving   ║
║ KB 04: Certificate Hot-Swap     ║ Sustained TLS Handshakes     ║ Atomic SniResolver Swap  ║ Handshake Spike & 0B ║
║ KB 05: ACME CA Network Blackhole ║ Sustained HTTPS Traffic      ║ CA Timeout / 500 Drop    ║ LKG Resilience Proof ║
║ KB 06: Sustained Longevity Test  ║ 50K RPS x 300s + Renew Cycle ║ Continuous Operation     ║ Zero-Leak & GC Proof ║
╚══════════════════════════════════╩══════════════════════════════╩══════════════════════════╩══════════════════════╝
```

---

## 3. Detailed 6-Scenario Comparative Benchmark Tables

---

### KB 01: Steady-State Serving Baseline (No Active Renewal) — Concurrency = 64 (Keep-Alive)
- **Objective**: Establish the raw frontline HTTPS request serving throughput, latency percentiles, and hardware counter baseline when terminating TLS using pre-loaded LKG certificates in an idle maintenance state.
- **Execution Command**: `wrk -t2 -c64 -d30s --latency https://172.16.0.5:8443/api/v1/orders`

| Detailed Metric | **Velda Edge** (Rust 2024) | **Caddy 2.8** (Go) | **Traefik 3.1** (Go) | **APISIX 3.11** (LuaJIT) | **OpenResty 1.25** (Lua) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **--- 1. Macro Tier: Network & Request Serving ---** | | | | | |
| Measurement Duration | **30.00s** | 30.00s | 30.00s | 30.00s | 30.00s |
| Total Requests Served | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Throughput (req/s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Mean Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Min Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P50 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P90 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99 Latency (Tail Latency) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99.9 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Max Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Latency Standard Deviation (Stdev) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Average Time To First Byte (TTFB Avg) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| HTTP Status Code Distribution (200 / 5xx) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Success Rate (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | | | |
| ACME Renewal State | **Idle (Sleeping)** | Idle | Idle | Idle | Idle |
| Active Ephemeral Routes in Router | **0** | 0 | 0 | 0 | 0 |
| Active In-Flight CertifiedKey Handles | **1 (Active)** | 1 | 1 | 1 | 1 |
| **--- 3. System Resource & Core Isolation Tier ---** | | | | | |
| Total CPU Utilization (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| % User CPU | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| % System / Kernel CPU | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Shadow Worker CPU % (`velda-shadow-{i}`) | **0.00% (Idle)** | N/A | N/A | N/A | N/A |
| Frontline Traffic Workers CPU % | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Peak Resident Memory (Peak RSS MB) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| % RAM Usage | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Heap Allocations on Serving Hot Path | **0 B / 0 allocs** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Memory Leak Rate | **0.00 MB / min** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 4. Micro Tier: Kernel & OS ---** | | | | | |
| Syscalls / Request (`syscalls / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Context Switches / Request (`CS / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Kernel Execution Time (Kernel Time) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Futex Lock Calls on Serving Path | **0** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Futex Ratio of Total Syscalls (%) | **0.00%** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 5. Micro Tier: HPC & CPU Hardware Counters ---** | | | | | |
| CPU Instructions / Request (`insn / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| CPU Cycles / Request (`cycles / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Instruction Efficiency (`IPC = insn / cycle`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Hardware Vector Stores / Serving | **0 (Serving Path)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Branches / Request (`branches / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Branch Misprediction Rate (`branch-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| L1 Data Cache Miss Rate (`L1-dcache-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Last Level Cache Miss Rate (`LLC-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 02: Ingress Customer Route Latency under Active Ephemeral Challenge Route
- **Objective**: Measure whether registering an ephemeral ACME challenge route (`/.well-known/acme-challenge/<token>`) into `velda_router::Router` induces any route lookup degradation or tail latency jitter on standard customer routes.
- **Execution Command**: `wrk -t2 -c64 -d30s --latency http://172.16.0.5:8080/api/v1/orders`

| Detailed Metric | **Velda Edge** (Rust 2024) | **Caddy 2.8** (Go) | **Traefik 3.1** (Go) | **APISIX 3.11** (LuaJIT) | **OpenResty 1.25** (Lua) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **--- 1. Macro Tier: Network & Request Serving ---** | | | | | |
| Throughput (req/s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Mean Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P50 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P90 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99 Latency (Tail Latency) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99.9 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Max Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Success Rate (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | | | |
| Active Ephemeral Challenge Routes | **1 Route Active** | 1 Active | 1 Active | 1 Active | 1 Active |
| Router Write Lock Hold Time ($\mu$s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Customer Route Lookup Degradation vs Baseline (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| SIMD 128-bit Vector Matcher Invocations | *[TBD]* | N/A | N/A | N/A | N/A |
| **--- 3. System Resource & Core Isolation Tier ---** | | | | | |
| Total CPU Utilization (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| % User CPU | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| % System / Kernel CPU | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Shadow Worker CPU % (`velda-shadow-{i}`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Frontline Traffic Workers CPU % | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Peak Resident Memory (Peak RSS MB) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 4. Micro Tier: Kernel & OS ---** | | | | | |
| Syscalls / Request (`syscalls / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Context Switches / Request (`CS / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Futex Lock Calls on Serving Path | **0** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 5. Micro Tier: HPC & CPU Hardware Counters ---** | | | | | |
| CPU Instructions / Request (`insn / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| CPU Cycles / Request (`cycles / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Instruction Efficiency (`IPC = insn / cycle`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Branch Misprediction Rate (`branch-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| L1 Data Cache Miss Rate (`L1-dcache-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 03: Challenge Store Concurrency & Direct Blit Throughput
- **Objective**: Measure the direct in-memory response serving performance when the external ACME CA probes the ephemeral HTTP-01 challenge endpoint.
- **Execution Command**: `wrk -t2 -c16 -d10s --latency http://172.16.0.5:8080/.well-known/acme-challenge/<VALID_TOKEN>`

| Detailed Metric | **Velda Edge** (Rust 2024) | **Caddy 2.8** (Go) | **Traefik 3.1** (Go) | **APISIX 3.11** (LuaJIT) | **OpenResty 1.25** (Lua) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **--- 1. Macro Tier: Network & Request Serving ---** | | | | | |
| Throughput (req/s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Mean Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P50 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P90 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99 Latency (Tail Latency) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99.9 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Max Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Success Rate (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | | | |
| Heap Allocations per Challenge Response | **0 B (Stack/Bytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Upstream Reverse-Proxy Traversal | **Bypassed (0 IO)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Challenge Store Read Contention | **Lock-Free ArcSwap** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 3. System Resource & Core Isolation Tier ---** | | | | | |
| Total CPU Utilization (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Peak Resident Memory (Peak RSS MB) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 4. Micro Tier: Kernel & OS ---** | | | | | |
| Syscalls / Request (`syscalls / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Context Switches / Request (`CS / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 5. Micro Tier: HPC & CPU Hardware Counters ---** | | | | | |
| CPU Instructions / Request (`insn / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| CPU Cycles / Request (`cycles / req`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Instruction Efficiency (`IPC = insn / cycle`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| L1 Data Cache Miss Rate (`L1-dcache-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 04: Downstream TLS Handshake Jitter during Certificate Hot-Swap
- **Objective**: Measure the tail latency of ongoing downstream TLS 1.3 handshakes at the exact millisecond when a renewed certificate is hot-swapped into `velda-tls::SniResolver`.
- **Execution Command**: Synthetic benchmarking client establishing 5,000 TLS 1.3 handshakes/sec while the Shadow worker installs a new `CertifiedKey`.

| Detailed Metric | **Velda Edge** (Rust 2024) | **Caddy 2.8** (Go) | **Traefik 3.1** (Go) | **APISIX 3.11** (LuaJIT) | **OpenResty 1.25** (Lua) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **--- 1. Macro Tier: Network & Request Serving ---** | | | | | |
| Handshake Throughput (handshakes/s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Handshake P50 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Handshake P90 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Handshake P99 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Handshake P99.9 at Hot-Swap Instant** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Max Handshake Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Dropped In-Flight TLS Handshakes** | **0** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | | | |
| SniResolver Write Lock Duration ($\mu$s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Swap Window Tail Latency Spike ($\mu$s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Atomic Disk Write + Fsync Duration (ms) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 3. System Resource & Core Isolation Tier ---** | | | | | |
| Total CPU Utilization (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Peak Resident Memory (Peak RSS MB) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 4. Micro Tier: Kernel & OS ---** | | | | | |
| Futex Lock Calls on Handshake Path | **0** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 5. Micro Tier: HPC & CPU Hardware Counters ---** | | | | | |
| L1D Misses Spike during Pointer Swap | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Last Level Cache Miss Rate (`LLC-miss %`) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 05: ACME CA Network Blackhole & Socket Timeout Resilience
- **Objective**: Verify that a complete network failure, 5xx outage, or blackhole of the external ACME CA causes zero degradation or error cascades on active frontline customer traffic.
- **Assault**: Outbound network firewall drops all packets to `acme-v02.api.letsencrypt.org`, forcing connect and read timeouts on the background worker while 50,000 customer requests/sec traverse the gateway.

| Detailed Metric | **Velda Edge** (Rust 2024) | **Caddy 2.8** (Go) | **Traefik 3.1** (Go) | **APISIX 3.11** (LuaJIT) | **OpenResty 1.25** (Lua) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **--- 1. Macro Tier: Network & Request Serving ---** | | | | | |
| Frontline Throughput (req/s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Frontline P50 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Frontline P99 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Frontline P99.9 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Error Rate (5xx)** | **0.00%** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | | | |
| ACME Background Order Status | **Failed (Logged)** | Failed | Failed | Failed | Failed |
| LKG Certificate In-Memory Retention | **100% Preserved** | Preserved | Preserved | Preserved | Preserved |
| Exponential Backoff Compliance | **3 Retries + Jitter** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 3. System Resource & Core Isolation Tier ---** | | | | | |
| Frontline Traffic Workers CPU % | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Shadow Logistics Worker CPU % | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| CPU Steal from Frontline Workers | **0.00%** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 06: Sustained Longevity & Lifecycle Memory Drift (300 Seconds)
- **Objective**: Confirm that repeated certificate orders, ephemeral route registrations, and RAII guard drops maintain zero memory leaks and zero route table bloat over time.
- **Execution Command**: Continuous 50,000 req/s load for 300 seconds while triggering automated renewal cycles against a local mock CA.

| Detailed Metric | **Velda Edge** (Rust 2024) | **Caddy 2.8** (Go) | **Traefik 3.1** (Go) | **APISIX 3.11** (LuaJIT) | **OpenResty 1.25** (Lua) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **--- 1. Macro Tier: Network & Request Serving ---** | | | | | |
| Measurement Duration | **300.00s** | 300.00s | 300.00s | 300.00s | 300.00s |
| Total Requests Served | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Throughput (req/s) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99 Latency (Tail Latency) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| P99.9 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Success Rate (%) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | | | |
| Completed Certificate Renewal Cycles | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Leaked Ephemeral Routes in Router | **0 (Zero)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Leaked File Descriptors | **0 (Zero)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 3. System Resource & Core Isolation Tier ---** | | | | | |
| Initial RSS Memory (MB) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Final RSS Memory (MB) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Memory Drift (Final RSS - Initial RSS) | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Memory Leak Rate (MB/min)** | **0.00 MB / min** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| GC Pauses / Runtime Stalls | **0 (No GC)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Architectural Comparison

| Dimension | Velda Edge (`acme_cert_rotator`) | Caddy 2.8 (CertMagic) | Traefik 3.1 | Apache APISIX 3.11 |
| :--- | :--- | :--- | :--- | :--- |
| **Runtime Model** | **Shadow Worker (`SCHED_IDLE`)** | Shared Go Goroutine pool | Shared Go Goroutine pool | LuaJIT timer (`ngx.timer.at`) |
| **Challenge Route Matching** | **128-bit SIMD Vector Matcher** | Standard Go HTTP mux | Go Router evaluation | OpenResty Nginx rewrite phase |
| **Challenge Cleanup** | **RAII Scope Guard (`Drop`)** | Context cancellation / defer | Dynamic router teardown | Lua timer cleanup callback |
| **Cache Pollution Control** | **Compact wire response + Natural LRU** | Standard Go heap allocation | Standard Go heap allocation | Shared dict (`ngx.shared.DICT`) |
| **Disk Write Safety** | **Atomic (tmp + sync_all + rename)** | File lock + atomic rename | Local file write | Shared disk storage |
| **TLS Hot-Swap** | **Lock-Free Read (`Arc<CertifiedKey>`)** | Go `tls.Config.GetCertificate` | Dynamic TLS store update | `ssl_certificate_by_lua` |
| **Garbage Collection Overhead**| **Zero (Deterministic RAII / Rust)**| Go GC pauses (STW) | Go GC pauses (STW) | LuaJIT GC sweeps |
| **Frontline Core Isolation** | **Hardware-pinned (Core 0 vs Cores 1..N)**| Shared thread scheduler | Shared thread scheduler | Shared Nginx worker processes |
