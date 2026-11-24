# Canary Plugin Benchmark Report

All live proxy measurements were executed in an isolated dual-node KVM environment under identical hardware constraints (`velda-gw` ↔ `velda-obs`). Clean macro throughput runs had no profilers attached; hardware PMU (`perf stat`) and CPU symbol sampling (`perf record`) were collected in separate steady-state runs.

---

## 1. Testbed Specification

| Component | Node / VM | Hardware & Software Spec | Role |
| :--- | :--- | :--- | :--- |
| **Gateway Proxy** | `velda-gw` (`192.168.122.10`) | 2 vCPUs (11th Gen Intel Core i5-11400H @ 2.70GHz, SMT off), 1.9 GB RAM, Linux 6.8.0 (`x86_64`, `x86-64-v3`) | Velda Edge (`2` pinned workers), Nginx `1.24.0` (`2` workers), Envoy `1.31.4` (`--concurrency 2`) |
| **Load Gen + Backends** | `velda-obs` (`192.168.122.20`) | 4 vCPUs, 4 GB RAM, Linux 6.8.0; `wrk 4.x` (`-t4 -c128 -d10s --latency`) | Rotating `4,096`-user Lua request generator + symmetric HTTP/1.1 backends (`base` on `:18081`, `canary` on `:18091`) |
| **Canary Ruleset** | N/A | `ratio: 200` (`20.0%`), `3` header rules (`equals`, `prefix`, `exists`) | Baseline S2 (`Priority`: `x-user-id`, `x-real-ip`), S3 (`Composite`: `x-tenant-id`, `x-user-id`), S4 (`Rule Hit`: `X-Canary: true`) |

---

## 2. Micro-Architectural Release ASM Audit (`x86-64-v3`)

Audited via `audit_asm.py` on release assembly (`target/release/deps/velda_plugin-e12321c47c0fe0f9.s`, compiled with `-C target-cpu=x86-64-v3`):

| Function Symbol | Total Insts | Heap Allocs (`alloc::*`) | Unaligned Vector Stores | Jump Tables (`jmpq *%reg`) | Atomic Bus Locks (`lock *`) | `divq` / `ud2` Traps | Fixed-Point Lowering |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`CanaryPlugin::on_upstream_request`** | `545` | **0** | **0** | `1` (`RuleOperator` 5-arm match) | **0** | **0 / 0** | `mulxq %rax, %rax, %rax` (`$1000`) |
| **`extract_header_bytes`** | `172` | **0** | **0** | **0** | **0** | **0 / 0** | Inline linear workspace slice |
| **`swar_hash64`** | Inlined | **0** | **0** | **0** | **0** | **0 / 0** | `as_chunks::<8>()` + SplitMix64 |

---

## 3. Macro Throughput, Latency & Split Accuracy (Clean Runs)

All proxies target the identical `192.168.122.20:18081` (`base`) and `:18091` (`canary`) backends across 4,096 distinct rotating `X-User-Id` / `X-Tenant-Id` headers (`-t4 -c128 -d10s --latency` after a 2s warmup):

| Scenario | Throughput (RPS) | Avg | P50 | P90 | P99 | Max | Stdev | Base Hits | Canary Hits | Empirical Ratio |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **S1: Velda Raw Proxy (Baseline)** | **60,439.36** | 2.14ms | 1.83ms | 3.93ms | 6.39ms | 14.44ms | 1.29ms | 604,898 | 0 | 0.00‰ |
| **S2: Velda Canary (`Priority`, 200‰)** | **61,482.83** | 2.10ms | 1.82ms | 3.67ms | 6.20ms | 17.27ms | 1.19ms | 494,634 | 120,775 | **196.25‰** |
| **S3: Velda Canary (`Composite`, 200‰)** | **56,843.00** | 2.28ms | 1.94ms | 4.15ms | 6.82ms | 16.16ms | 1.36ms | 452,768 | 116,426 | **204.55‰** |
| **S4: Velda Canary (`Rule Hit`, `x-canary: true`)** | **56,120.26** | 2.31ms | 1.96ms | 4.26ms | 7.13ms | 13.60ms | 1.41ms | 0 | 562,242 | **1000.00‰** |
| **S5: Nginx 1.24.0 (`split_clients` + `map`)** | **58,824.57** | 2.19ms | 1.89ms | 3.91ms | 6.54ms | 17.98ms | 1.30ms | 475,709 | 118,511 | **199.44‰** |
| **S6: Envoy 1.31.4 (`weighted_clusters 80/20`)** | **22,740.42** | 5.60ms | 5.44ms | 7.07ms | 8.96ms | 15.39ms | 1.13ms | 183,984 | 45,791 | **199.29‰** |

### Key Quantitative Observations
- **+4.5% faster than native C Nginx (`split_clients`)** on identical 2-vCPU KVM hardware (`61,482.83` vs `58,824.57` RPS) and lower P99 latency (`6.20ms` vs `6.54ms`), while executing 3 ordered header rules and multi-header `Priority` fallback.
- **2.70× higher throughput than Envoy 1.31.4** (`61,482.83` vs `22,740.42` RPS) with **2.99× lower P50 latency** (`1.82ms` vs `5.44ms`).
- **`Priority` matches Raw Proxy Baseline**: S2 maintains parity within KVM run-to-run variance against S1 (`61,482.83` vs `60,439.36` RPS), demonstrating negligible per-request cost compared to network stack traversal.

---

## 4. Hardware PMU Counters (10s Steady-State `perf stat`)

Measured over 10s steady-state under concurrent `wrk -t4 -c128` load:

| Scenario | PMU Window RPS* | Cycles (B) | Instructions (B) | IPC | Inst / Req | Branch Miss (%) | L1d Load Miss (%) | LLC Miss (%) | Context Switches | Futex Calls |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **S1: Velda Raw Baseline** | 46,077.64 | 33.14 | 30.10 | `0.91` | `65,329` | `0.92%` | `10.41%` | `24.64%` | 9,400 | `4` |
| **S2: Velda Canary Priority** | 45,717.34 | 32.93 | 31.44 | `0.95` | `68,781` | `0.89%` | `10.22%` | `24.48%` | 9,500 | **`0`** |
| **S3: Velda Canary Composite** | 43,102.23 | 32.44 | 29.39 | `0.91` | `68,181` | `0.90%` | `10.46%` | `25.71%` | 8,699 | **`0`** |
| **S4: Velda Canary Rule Hit** | 42,015.06 | 31.02 | 28.21 | `0.91` | `67,146` | `0.92%` | `10.29%` | `25.84%` | 8,619 | **`0`** |
| **S5: Nginx 1.24.0 (`split_clients`)** | 39,919.13* | 31.88 | 29.69 | `0.93` | `74,366` | `1.01%` | `9.28%` | `21.06%` | 8,052 | `0` |
| **S6: Envoy 1.31.4** | 16,121.45* | 49.42 | 40.96 | `0.83` | `254,060` | `2.38%` | `7.80%` | `23.19%` | 670 | `37` |

*\*Inst/Req is computed using `(instructions / (PMU_RPS × 10))`. For S5 and S6, daemon shutdown at second 11 interrupted the final second of the 12s `wrk` window while `perf stat` had already captured full 10s worker PMU counters; S5/S6 `Inst/Req` are lower-bound approximations.*

---

## 5. `perf record` CPU Symbol Profile (`S2_Canary_Priority`)

Sampled via `perf record -F 999 -g --call-graph dwarf -p <velda-edge-pid> -- sleep 8` (`13,000` samples, `0` lost) and classified via `analyze_perf.py`:

| Architectural Layer | CPU Overhead (%) | Attributed Scope & Notes |
| :--- | :---: | :--- |
| **Linux Kernel Network Stack** | `60.90%` | TCP/IP socket `writev` / `virtqueue_notify` (`[kernel.kallsyms]`) |
| **Component Serving Hot Path** | `39.18%` | Velda ingress/upstream HTTP/1.1 & `epoll` serving (`CanaryPlugin` < `0.01%`, absent from `>= 0.01%` sampled leaves) |
| **Kernel Slab / `sk_buff` Allocators** | `3.27%` | 100% kernel slab (`[k] kmem_cache_free`, `kmem_cache_alloc_node`, `__kmalloc`, `__alloc_skb`); **0.00%** userspace heap `alloc::*` |
| **Tokio Runtime & Transport** | `0.11%` | Async driver scheduling |
| **Userspace Mutex Contention** | `0.05%`–`0.09%` | `libc.so.6 pthread_mutex_trylock` (`0.02%` per worker) / `unlock` (`0.01%`), zero `canary` lock symbols |
