# Canary Plugin Benchmark Report

All live proxy measurements were executed in an isolated dual-node KVM environment (`velda-gw` ↔ `velda-obs`). Clean macro runs had zero profilers attached; hardware PMU (`perf stat`) runs were executed in separate steady-state windows.

---

## 1. Testbed Specification

| Component | Node / VM | Hardware & Software Spec | Role |
| :--- | :--- | :--- | :--- |
| **Gateway Proxy** | `velda-gw` (`192.168.122.10`) | 2 vCPUs (11th Gen Intel Core i5-11400H @ 2.70GHz, SMT off), 1.9 GB RAM, Linux 6.8.0 (`x86_64`, `x86-64-v3`) | Velda Edge (`2` pinned workers), Nginx `1.24.0` (`2` workers), APISIX `3.21.0` (`2` workers), Kong `3.9.5` (`2` workers), Envoy `1.31.4` (`--concurrency 2`) |
| **Load Gen + Backends** | `velda-obs` (`192.168.122.20`) | 4 vCPUs, 4 GB RAM, Linux 6.8.0; `wrk` (`-t4 -c128 -d10s --latency`) | Rotating `4,096`-user Lua generator + symmetric HTTP/1.1 backends (`base` on `:18081`, `canary` on `:18091`) |
| **Canary Config** | N/A | `ratio: 200` (`20.0%`), `3` ordered header rules (`equals`, `prefix`, `exists`), strict all-headers-present SWAR hash | Tested with single header (`["x-user-id"]`) and multi-header (`["x-tenant-id", "x-user-id"]`) |

---

## 2. Micro-Architectural Release ASM Audit (`x86-64-v3`)

Audited via `audit_asm.py` on release assembly (`target/release/deps/velda_plugin-e12321c47c0fe0f9.s`, compiled with `-C target-cpu=x86-64-v3`):

| Function Symbol | Total Insts | Heap Allocs (`alloc::*`) | Unaligned Vector Stores | Jump Tables (`jmpq *%reg`) | Atomic Bus Locks (`lock *`) | `divq` / `ud2` / TLS | Fixed-Point Lowering |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`CanaryPlugin::on_upstream_request`** (inlined `evaluate_permille`) | `582` | **0** | **0** | `1` (`RuleOperator` 5-arm match) | **0** | **0 / 0 / 0** | `mulxq %rax, %rax, %rax` (`$1000`) |

---

## 3. Velda Edge Macro Throughput & Hardware PMU (Final Zero-Fallback Build)

Measured sequentially on `velda-gw` ↔ `velda-obs` (`wrk -t4 -c128 -d10s --latency`, 4,096 rotating `X-Tenant-Id` / `X-User-Id` pairs):

| Scenario | Clean RPS | Avg | P50 | P90 | P99 | Canary Hits / Total | Empirical Ratio | PMU Inst / Req | IPC | Branch Miss | Futex |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **S1: Velda Raw Baseline (`plugins: []`)** | **51,259.42** | 2.50ms | 2.19ms | 4.42ms | 7.20ms | `0 / 517,781` | 0.00‰ | `63,378` | 0.71 | `1.04%` | **`0`** |
| **S2: Canary Single-Header (`["x-user-id"]`, 200‰)** | **44,289.01** | 2.89ms | 2.59ms | 4.86ms | 8.19ms | `93,929 / 447,413` | **209.94‰** | `65,283` (`+3.0%`) | 0.72 | `1.03%` | **`0`** |
| **S3: Canary Multi-Header (`["x-tenant-id", "x-user-id"]`, 200‰)** | **47,159.79** | 2.73ms | 2.36ms | 4.92ms | 8.13ms | `97,396 / 476,169` | **204.54‰** | `67,733` (`+6.9%`) | **0.91** | **`0.92%`** | **`0`** |
| **S4: Canary Rule Hit (`X-Canary: true`)** | **50,873.40** | 2.56ms | 2.17ms | 4.63ms | 8.10ms | `509,861 / 509,861` | **1000.00‰** | `65,733` (`+3.7%`) | **0.93** | **`0.92%`** | **`0`** |

*(Instruction delta per full HTTP/1.1 request between Raw Baseline and active canary evaluation is `+1,905` to `+4,355` instructions/req across the complete socket pipeline, with `0` futex contention.)*

---

## 4. Competitor Isolation Benchmarks (Same `velda-gw` 2-vCPU VM ↔ `velda-obs`)

Measured on identical KVM hardware (`worker_processes 2` / `--concurrency 2`, access logs disabled, 256 keepalive connections):

| Gateway & Mode | Raw RPS | Canary Enabled RPS | Self Overhead vs Raw | Canary P50 | Canary P99 | Canary Inst / Req | Empirical Ratio | Notes |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Nginx `1.24.0`** (`split_clients` + `map`) | — | `58,824.57` | — | 1.89ms | 6.54ms | `74,366`* | 199.44‰ | Native C string murmur2 (`split_clients`) |
| **Apache APISIX `3.21.0`** (`traffic-split` + 3 rules) | `46,128.22` | **`31,302.13`** | **-32.1%** | 3.84ms | 8.56ms | **`162,994`** (`+53.5%`) | 200.00‰ | `traffic-split.lua` rebuilds upstream pickers per request |
| **Kong `3.9.5` OSS** (`pre-function` Lua canary + 3 rules) | `36,649.55` | **`26,970.38`** | **-26.4%** | 4.58ms | 10.94ms | **`172,390`** (`+24.0%`) | 200.04‰ | Kong native `canary` plugin is Enterprise-only; weighted `round-robin` without rules measured `28,188.52` RPS (`-23.1%`) |
| **Envoy `1.31.4`** (`weighted_clusters 80/20` + header match) | — | **`22,740.42`** | — | 5.44ms | 8.96ms | **`254,060`** (`*`) | 199.29‰ | `37` futex calls during 10s steady state |

*\*Nginx/Envoy PMU `Inst/Req` values are lower bounds from a 10s `perf stat` capture inside a 12s `wrk` window stopped at second 11.*

---

## 5. `perf record` CPU Symbol Profile (Pre-Refactor Steady-State `Priority` Capture)

Sampled on `velda-gw` via `perf record -F 999 -g --call-graph dwarf -- sleep 8` (`13,000` samples, `0` lost) and classified via `analyze_perf.py`:

| Architectural Layer | CPU Overhead (%) | Attributed Scope & Notes |
| :--- | :---: | :--- |
| **Linux Kernel Network Stack** | `60.90%` | TCP/IP socket `writev` / `virtqueue_notify` (`[kernel.kallsyms]`) |
| **Component Serving Hot Path** | `39.18%` | Velda ingress/upstream HTTP/1.1 & `epoll` serving (`CanaryPlugin` < `0.01%`, absent from sampled hot leaves) |
| **Kernel Slab / `sk_buff` Allocators** | `3.27%` | 100% kernel slab (`[k] kmem_cache_free`, `kmem_cache_alloc_node`, `__kmalloc`, `__alloc_skb`); **`0.00%`** userspace heap `alloc::*` |
| **Tokio Runtime & Transport** | `0.11%` | Async driver scheduling |
| **Userspace Mutex Contention** | `0.05%`–`0.09%` | `libc.so.6 pthread_mutex_trylock` (`0.02%` per worker) / `unlock` (`0.01%`) |
