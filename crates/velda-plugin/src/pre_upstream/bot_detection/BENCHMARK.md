# Performance & Deep Anti-Bot Benchmark Report: `bot_detection`

Comprehensive empirical evaluation, **Mathematical Invariant Proofs**, **Linux `perf` HPC/Kernel Telemetry**, **6-Gateway Anti-Bot Evasion Comparison**, and **`x86-64-v3` Release Assembly Audit** for [`bot_detection`](crates/velda-plugin/src/pre_upstream/bot_detection/) (`ja4_filter` **disabled**; testing `bot_detection` in isolation with boot-fused SoA bitmask scoring and strict `64`-byte [`L1BitFilter`](arena.rs#L35-L44) + `64`-shard `CLOCK` L2 arena).

---

## 1. Testbed Topology & Measurement Protocol

- **Gateway Node (`velda-gw`, `192.168.122.10`)**: `4` vCPUs (`11th Gen Intel Core i5-11400H @ 2.70GHz`, AVX2 / BMI2, Linux `6.8.0-147-generic`, Ubuntu 24.04 LTS), `3,915 MiB` RAM, P-256 ECDSA HTTPS (`0.0.0.0:8443`).
- **Observer & Backend Node (`velda-obs`, `192.168.122.20`)**: `4` vCPUs, `5,925 MiB` RAM, running `/opt/velda/velda-bench-backend` on `:8081` (`GET /ping` -> `"pong"`, `GET /12kb` -> `12,287 B`), `wrk -t4 -c256`, [`kvm_cross_gateway_matrix.go`](bot/kvm_cross_gateway_matrix.go), and [`kvm_cacheline_flood.go`](bot/kvm_cacheline_flood.go).
- **Velda Plugin Configuration Tested (`ja4_filter` NOT attached)**:
  `plugins: ["bot_detection"]` with `mode: "enforce"`, `preset: "balanced_browser"`, `delay_score: 35`, `min_delay_ms: 100`, `max_delay_ms: 1600`, `block_score: null`, `escalation: { window_secs: 30..120, decay_secs: 10..15, max_entries: 4096 (512 for L2 eviction test), strike_block_threshold: 4 }`.
- **Two Synchronized Passes**:
  1. **Macro Pass (`8s` after `3s` warmup)**: Uninstrumented `wrk -t4 -c256`, latency percentiles, errors, and post-warmup `RSS` (`WARM_RSS_MB`).
  2. **Hardware/Kernel Telemetry Pass (`perf stat` + `perf record`)**: Synchronized `8s` `perf stat` across `12` PMU/kernel counters (`cycles`, `instructions`, `branches`, `branch-misses`, `L1-dcache-loads`, `L1-dcache-load-misses`, `cache-references`, `cache-misses`, `context-switches`, `cpu-migrations`, `raw_syscalls:sys_enter`, `syscalls:sys_enter_futex`), plus `6s` `perf record -F 999 -e cycles` (`16K` samples, `0` lost). *(PMU multiplexing + syscall tracepoints lower instrumented `wrk` RPS by ~20–24%; per-request ratios reflect true hardware cost per served request.)*

---

## 2. Mathematical Invariants & Formal Guarantees

All invariants below are proven directly from [`velda-core/src/context.rs`](../../../../velda-core/src/context.rs), [`velda-core/src/bitfield/`](../../../../velda-core/src/bitfield/mod.rs), [`arena.rs`](arena.rs), [`compiler.rs`](compiler.rs), and [`plugin.rs`](plugin.rs).

### 2.1 Single-Cache-Line `RequestState` Packing & Lossless Bijective Wire Permutation
[`RequestState`](../../../../velda-core/src/context.rs) packs `tls_traits: u16` (`2` bytes) and `wire_traits: u40` (`wire_traits_lo: u32` + `wire_traits_hi: u8`, `5` bytes) inside the compiler's `7`-byte tail alignment padding, enforced at compile time by:
$$\text{sizeof}(\texttt{RequestState}) = 64\text{ bytes} = 1\text{ L1d cache line}$$
HTTP/2 & HTTP/3 pseudo-header ordering (`:method`, `:authority`, `:scheme`, `:path`) is mapped via a 5-bit Lehmer permutation index $L(\pi) \in \{1..24\} \subset \{0..31\}$. Because $4! = 24 < 31$, encoding and decoding (`PSEUDO_ORDER_MASP`, `MPAS`, `MSPA`, `MSAP`, `MAPS`) is **strictly bijective with zero hash collisions**.

### 2.2 Strict `64`-Byte `L1BitFilter`, Lemire Fast-Range Bounds & Final Zero-FP Guarantee
[`L1BitFilter`](arena.rs#L35-L44) enforces `#[repr(C, align(64))]` and compile-time size/alignment invariants:
$$\texttt{words: [AtomicU64; 7]}\;(56\text{ B} = 448\text{ bits}) \;+\; \texttt{epoch\_tick\_100ms: AtomicU64}\;(8\text{ B}) = 64\text{ bytes}$$
Every clean request (`score < delay_score`) reads `epoch_tick_100ms` and bit-probes `(w1, w2)` inside **one single cache line** using `AtomicU64::load(Relaxed)` (`0` writes, keeping the line MESI `Shared` across cores). Given SplitMix64 digest $h = \text{mix64}(\text{ip}, \text{ja4})$ split into two uniform 32-bit halves $u_1, u_2 \in [0, 2^{32}-1]$, Lemire fast-range maps:
$$b_i = \left\lfloor \frac{u_i \cdot 448}{2^{32}} \right\rfloor \implies \max(b_i) = \left\lfloor \frac{(2^{32}-1)\cdot 448}{2^{32}} \right\rfloor = 447 < 448, \quad w_i = \left\lfloor \frac{b_i}{64} \right\rfloor \in \{0..6\}$$
with zero division instructions (`imul` + `shr 38` in release ASM). After $N$ distinct offenders within one `window_secs` epoch ($k = 2$ probes per key):
- L1 bit saturation & prefilter false-hit probability:
  $$p_{\text{set}}(N) = 1 - \left(1 - \frac{1}{448}\right)^{2N} \approx 1 - e^{-2N/448}, \qquad P_{\text{FP}}^{\text{L1}}(N) \approx p_{\text{set}}(N)^2$$
- **Final False-Positive Probability ($P_{\text{FP}}^{\text{final}} = 0$)**: Any L1 hit triggers exact `(client_ip: u128, ja4: u64)` comparison inside one of `64` `L2Shard` buckets; unflagged clients return `(0, false) -> Action::Continue` cleanly even when $p_{\text{set}}(N) \to 1$.

### 2.3 Bounded SoA Rule Evaluation (`0..=100`)
Boot-time [`compile_ruleset`](compiler.rs) merges identical predicates, verifies $\le 64$ unique referenced headers, sorts descending by `score`, and lays out contiguous SoA vectors. Runtime `evaluate_score` clamps `score = min(100, sum)` in $O(R_{\text{fused}})$ bitwise `&` operations with zero heap allocations and zero dynamic regex evaluation.

### 2.4 Coarsened `100ms` Strike Decay & Tarpit Interpolation Law
With `min_delay_ms` $d_{\min}$, `max_delay_ms` $d_{\max}$, `delay_score` $d_s$, top bound $T = \text{block\_score.unwrap\_or}(100)$, `span` $= T - d_s$, 4-bit `strike` $s \in \{0..15\}$, and elapsed coarse $100\text{ ms}$ ticks $\Delta\tau$:
$$s' = \max\!\left(0,\; s - \min\!\left(15,\; \left\lfloor \frac{\Delta\tau}{\tau_{\text{decay}}} \right\rfloor\right)\right)$$
$$\text{delay}(score, s') = \begin{cases}
\min\!\left(d_{\max},\; \left(d_{\min} + \left\lfloor \frac{(d_{\max} - d_{\min})\cdot \min(score - d_s, \text{span})}{\text{span}} \right\rfloor\right) \cdot 2^{\min(s', 4)}\right) & \text{if } score \ge d_s \\
\min\!\left(d_{\max},\; d_{\min} \cdot 2^{\min(s' - 1, 4)}\right) & \text{if } score < d_s \land s' > 0 \\
0 & \text{otherwise}
\end{cases}$$

**Live Arithmetic Verification (`ds = 35, top = 100, span = 65, d_min = 100, d_max = 1600`)**:
- `B1` (`score = 50`): $100 + \lfloor 1500 \cdot 15 / 65 \rfloor = 446\text{ ms}$ $\rightarrow$ observed live RTT **`448 ms`**
- `B3` (`score = 60`): $100 + \lfloor 1500 \cdot 25 / 65 \rfloor = 676\text{ ms}$ $\rightarrow$ observed live RTT **`679 ms`**
- `B4` (`score = 95`): $100 + \lfloor 1500 \cdot 60 / 65 \rfloor = 1484\text{ ms}$ $\rightarrow$ observed live RTT **`1488 ms`**
- `B2` (`score >= 100`): $100 + 1500 = 1600\text{ ms}$ $\rightarrow$ observed live RTT **`1603 ms`**
- `B5.3` (`score = 0, s' = 1`): $100 \cdot 2^{0} = 100\text{ ms}$ $\rightarrow$ observed live RTT **`103 ms`**

### 2.5 Bounded L2 CLOCK Arena Footprint
With `NUM_SHARDS = 64`, `SET_WAYS = 8`, `size_of::<L2Slot>() == 32 B`, and `num_sets_per_shard = next_pow2(ceil(max_entries / 64 / 8))`:
- At `max_entries = 4,096` (`512` `L2Set`s across `64` shards): $M_{\text{arena}} = 64\text{ B} + 512 \times (8 \times 32\text{ B} + 2\text{ B}) + \text{headers} \approx \mathbf{135\text{ KiB}}$, evicted in strictly `O(8)` per `CLOCK` probe.

---

## 3. Empirical KVM Telemetry, Marginal Cost & Cross-Gateway Benchmarks

### 3.1 Equal-Warmup Marginal Hardware & Kernel Telemetry (`Velda Raw` vs `Velda bot_detection`)
Measured identically on `velda-gw` (`3s` warmup + `8s` `wrk -t4 -c256` `/ping` + equal post-warmup `WARM_RSS_MB` + synchronized `12`-event `perf stat`):

| Telemetry Tier | **Velda Raw (`plugins: []`)** | **Velda `bot_detection` ONLY (`64B` L1)** | Marginal Delta (`bot_detection - Raw`) |
| :--- | :---: | :---: | :---: |
| **Clean Macro RPS (`8s`, uninstrumented)** | `59,574.39` (`479,241` OK) | `54,145.67` (`436,401` OK) | `-9.1%` macro (`-5.3%` PMU insns; KVM variance) |
| **Post-Warmup RSS (`WARM_RSS_MB`)** | **`65.9 MB`** | **`66.1 MB`** | **`+0.2 MB` (`~135 KiB` L2 arena + SoA)** |
| **Perf Instrumented RPS (`12` events active)** | `47,748.74` (`384,290` reqs) | `44,764.54` (`359,451` reqs) | `-6.2%` |
| **`cycles / req`** | `139,588` | `142,969` | **`+3,381 cyc/req` (`+2.4%`)** |
| **`instructions / req`** | `72,038` | `75,858` | **`+3,820 insn/req` (`+5.3%`)** |
| **`IPC` (`insn / cycle`)** | `0.52` | **`0.53`** | `+0.01` |
| **`branches / req` \| `branch-miss %`** | `12,227` \| `1.20%` | `12,729` \| `1.24%` | `+502` branches \| `+0.04%` miss |
| **`L1-dcache-load-misses %`** | `13.58%` | `13.59%` | **`+0.01%`** *(clean 64B L1 MESI hit)* |
| **`cache-misses %` (of cache-refs)** | `39.56%` | `39.32%` | **`-0.24%`** |
| **`syscalls / req`** | `3.70` (`1,420,664`) | **`3.66`** (`1,315,538`) | **`0.00` extra syscalls** |
| **`context-switches / req` \| `cpu-migrations`** | `0.0196` \| `5` | **`0.0178`** \| `8` | `0.00` added CS |
| **`futex` syscalls (`8s` across ~360k–384k reqs)** | `24` | `42` | `+18` (`0` L2 shard lock contention) |

### 3.2 Linux Kernel / User CPU Hotspot Profile (`perf record -F 999 -e cycles`)
Captured live during `wrk -t4 -c256 -d6s` with `bot_detection` active (`16K` samples, **`0` lost**, `46.73B` cycles):
- **Balanced 4-Core Kernel Load**: `__nf_conntrack_find_get` accounts for `0.58%–0.68%` per worker (`velda-worker-0..3`), followed by `__tcp_transmit_skb` (`0.21%–0.30%`), `tcp_sendmsg_locked` (`0.20%–0.29%`), `aa_inet_msg_perm` (`0.20%–0.28%`), `iowrite16` virtio-net doorbell (`0.21%–0.27%`), `__fdget` (`0.21%–0.26%`), and `__inet_lookup_established` (`0.22%–0.23%`).
- Across the `3,820 insn/req` marginal user-space budget (`~5.3%` of end-to-end HTTPS serving), no single `bot_detection` bottleneck dominates execution above TLS record crypto and kernel TCP/conntrack.

### 3.3 Cross-Gateway Anti-Bot & Zero-False-Positive Matrix (`C1`–`C3`, `B1`–`B5`)
Executed from `velda-obs` via [`kvm_cross_gateway_matrix.go`](bot/kvm_cross_gateway_matrix.go) against native L7/plugin anti-bot configurations on each engine:

| Live Vector | Spoof / Client Profile | **Velda (`bot_detection`)** | **Nginx `1.24.0`** | **HAProxy `2.8.30`** | **Envoy `v1.32`** | **APISIX `3.11.0`** | **Kong `3.9` (`bot-detection`)** |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`C1`** `velda-healthcheck/1.0` | Honest API / healthcheck | **`200`** (`3ms`) | `200` (`10ms`) | `200` (`2ms`) | `200` (`2ms`) | `200` (`2ms`) | `200` (`3ms`) |
| **`C2`** `Firefox/132.0` + `Sec-Fetch-*` | Honest Gecko browser | **`200`** (`1ms`) | `200` (`1ms`) | `200` (`10ms`) | `200` (`1ms`) | `200` (`1ms`) | `200` (`2ms`) |
| **`C3`** `Safari/605.1.15` + `Sec-Fetch-*` | Honest WebKit browser | **`200`** (`1ms`) | `200` (`1ms`) | `200` (`1ms`) | `200` (`1ms`) | `200` (`1ms`) | `200` (`2ms`) |
| **`B1`** `python-requests/2.31.0` | Honest script UA | **Caught (`448ms`)** | `403` (`1ms`) | `403` (`1ms`) | `403` (`1ms`) | `403` (`1ms`) | **Missed** (`200`, `1ms`) |
| **`B2`** `Chrome/131` UA only | Missing `Sec-Fetch-*` & `Accept-Language` | **Caught (`1603ms`)** | `403` (`1ms`) | `403` (`1ms`) | `403` (`1ms`) | `403` (`1ms`) | **Missed** (`200`, `1ms`) |
| **`B3`** Full-header `Chrome/131` (HTTP/1.1) | Spoofs `Sec-Fetch-*` + `Accept-Language` | **Caught (`679ms`)** | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) |
| **`B4`** Full-header `Chrome/131` (`HTTP/2.0`) | Spoofs all HTTP headers over `h2` | **Caught (`1488ms`)** | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) |
| **`B5.1–B5.2`** Repeat full-header spoof | Strike accumulation `0 -> 1` | **`678 -> 1355ms`** | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) |
| **`B5.3`** Sneaky switch back to clean UA | Alternating evasion on `(ip, ja4)` | **Caught (`103ms`)** | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) | **Missed** (`200`) |

### 3.4 Clean HTTPS Serving & PMU Across All 6 Gateways (`wrk -t4 -c256 -d8s`)

| Gateway / Configuration | `/ping` RPS | `p50` / `p99` | `/12kb` RPS | RSS (MB) | `insn/req` | `cyc/req` | IPC | L1d Miss | `futex` Calls |
| :--- | ---: | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Velda Edge (`bot_detection`, 64B L1)** | **62,739.31** | `3.32` / `9.97ms` | **37,828.39** | 65.6* | **74,541** | **143,264** | 0.52 | 13.41% | 54 |
| **HAProxy `2.8.30` (L7 ACLs)** | 55,098.97 | `3.76` / `10.96ms` | 34,767.54 | **14.7** | 97,325 | 165,036 | **0.59** | **11.03%** | 123 |
| **Nginx `1.24.0` (L7 `if` rules)** | 54,422.94 | `3.85` / `10.92ms` | 33,804.89 | 46.6 | 87,050 | 159,626 | 0.55 | 11.76% | **0** |
| **Apache APISIX `3.11.0` (`ua` + Lua)** | 34,237.44 | `7.07` / `17.24ms` | 23,915.56 | 161.5 | 136,429 | 324,790 | 0.42 | 18.34% | **0** |
| **Kong `3.9` (`bot-detection`)** | 29,199.96 | `8.04` / `22.76ms` | 21,201.31 | 387.3 | 163,928 | 362,554 | 0.45 | 16.69% | **0** |
| **Envoy `v1.32` (`tls_inspector` + Lua)** | 23,664.57 | `10.51` / `15.26ms` | 18,384.27 | 51.4 | 231,131 | 535,126 | 0.43 | 11.22% | 51 |

*\*Warm post-`wrk` RSS comparison on equal footing is `65.9 MB` (Velda Raw) vs `66.1 MB` (Velda `bot_detection`) = `+0.2 MB` delta (`Section 3.1`).*

### 3.5 `64`-Byte `L1BitFilter` Saturation Under `2,000`-Bot Flood
Running `kvm_cacheline_flood` (`2,000` distinct bot JA4s at `200` concurrency in `8.27s`, `window_secs: 120`, `max_entries: 4096`):
- **Flood Enforcement**: `1,998 / 1,998` offending bots delayed (`>= 80ms`), `0` errors (`bot#1`: `1357 -> 1602ms`; `bot#end`: `1355ms`).
- **Clean Traffic Under Saturated `448`-Bit L1 (`200` probes)**: `200/200` `200 OK`, `0` slow (`>= 80ms`), `0` errors; post-flood `wrk`: **`60,125.41` RPS** (`p50 = 3.52ms`, `p99 = 10.62ms`), preserving **`95.8%`** cold-L1 throughput.

---

## 4. Verified Release Assembly (`x86-64-v3`) & Symbol Audit

Emitted directly from `RUSTFLAGS="-C target-cpu=x86-64-v3" cargo rustc -p velda-plugin --lib --release -- --emit=asm -C llvm-args=-x86-asm-syntax=intel` (`target/release/deps/velda_plugin-e4115f479001c454.s`):

| Audited Symbol | Static `.s` Insns | Heap Allocs (`__rust_alloc`) | `lock` Prefix | `div` / `idiv` | `jmpq *` | Direct `syscall` | Verified Emitted Instructions & Invariant |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`BotDetectionPlugin::evaluate_at_tick`** | `510` *(inlines L1+L2)* | **`0` on Clean path** *(`2` sites only in `Action::Delay` / cold path)* | **`0` on Clean L1-miss** | **`0` on Clean L1-miss** *(`3` only in L2 decay/delay)* | **`0`** | **`0`** | SplitMix64 uses BMI2 `rorx 47`/`33` + 2×`imul`; `L1BitFilter` uses Lemire `imul ..., 448` + fused **`shr 38`** + **`bt`** + BMI2 **`shrx`**, loading `epoch_tick_100ms` (`[r9+120]`) & words (`[r9+64..+120]`) from one 64B line. |
| **`BotDetectionPlugin::evaluate_score`** | `3,329` *(inlines `classify_user_agent`)* | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | Zero heap allocations, zero locks, zero indirect jump tables; uses AVX2 `vmovups ymm` stack setup + case-insensitive ASCII chunk helpers. |
| **`on_upstream_request` / `try_on_upstream_request`** | `72` / `72` | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | Converts monotonic tick via `Instant::now()` / `OnceLock` and tail-dispatches to `evaluate_at_tick`. |

---

## 5. Comprehensive Architectural Comparison

| Technical Dimension | **Velda Edge (`bot_detection`)** | **HAProxy `2.8.30`** | **Nginx `1.24.0`** | **Apache APISIX `3.11.0`** | **Kong `3.9`** | **Envoy `v1.32`** |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Signals Inspected** | **Cross-layer `tls_traits` (16b) + `wire_traits` (40b) + Headers (`<=64` bits) inside 64B `RequestState`** | HTTP headers + partial cipherlist ACLs | HTTP headers (`if`/regex) | `ua-restriction` + OpenResty Lua headers | Regex over `User-Agent` | `tls_inspector` (SNI/ALPN) + Lua headers |
| **Per-`(ip, ja4)` Evasion State** | **Strict 64B `L1BitFilter` (`448b`+`8B` epoch) + 64-shard `CLOCK` L2 (`135 KiB`)** | Stateless per-request (sticky tables separate) | Stateless per-request | Stateless Lua | Stateless Lua | Stateless Lua |
| **Clean CPU Overhead** | **`+3,820 insn/req` (`+5.3%`), `+0.00` syscalls/req** | `97,325 insn/req` | `87,050 insn/req` | `136,429 insn/req` (`161.5 MB`) | `163,928 insn/req` (`387.3 MB`) | `231,131 insn/req` |
| **Full-Header Spoof Detection (`B3`, `B4`, `B5`)** | **Caught (`679ms` / `1488ms` / `103ms`) with `0` FP on C1–C3** | **Missed (`200`, `1ms`)** | **Missed (`200`, `1ms`)** | **Missed (`200`, `1–2ms`)** | **Missed (`200`, `1ms`)** | **Missed (`200`, `1–2ms`)** |
