# CSRF Plugin Benchmark Report

All live proxy measurements were executed in an isolated dual-node KVM environment (`velda-gw` ↔ `velda-obs`). Clean macro runs had no profilers attached; PMU (`perf stat`) and CPU symbol sampling (`perf record`) were collected in separate steady-state windows.

---

## 1. Testbed Specification

| Component | Node / VM | Hardware & Software Spec | Role |
| :--- | :--- | :--- | :--- |
| **Gateway Proxy** | `velda-gw` (`192.168.122.10`) | 2 vCPUs (11th Gen Intel Core i5-11400H @ 2.70GHz, SMT off), 1.9 GB RAM, Linux 6.8.0 (`x86_64`, `x86-64-v3`) | Velda Edge (`2` pinned workers, `RUST_LOG=warn`, `http1` listener on `:18080`) |
| **Load Gen + Backend** | `velda-obs` (`192.168.122.20`) | 4 vCPUs, 4 GB RAM, Linux 6.8.0; `wrk` (`-t4 -c128 -d10s --latency`) + `velda-bench-backend` (`:8081`) | Sustained HTTP/1.1 `GET /ping` & `POST /ping` workload (`Sec-Fetch-Site: same-origin` / `Cookie` / `X-XSRF-TOKEN`) |

---

## 2. Micro-Architectural Release ASM Audit (`x86-64-v3`)

Audited via `audit_asm.py` on `target/release/deps/velda_plugin-*.s` (`-C target-cpu=x86-64-v3`):

| Build Variant (`CsrfPlugin::on_upstream_request`) | Total Insts | Heap Allocs (`alloc::*`) | Stack Spills (W / R) | Unaligned Vector Ops | Jump Tables (`jmpq *%reg`) | Atomic Locks (`lock *`) | Out-of-Line Cold Calls |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Inline Fallback (Before `#[cold]`)** | `1,099` | **0** | `19 W / 85 R` | `10` | `1` (`MethodMask`) | **0** | `0` |
| **Cold-Split Final (`#[cold]` Legacy + Double-Submit)** | **`507`** (**-53.9%**) | **0** | **`11 W / 22 R`** | **`2`** (**-80%**) | `1` (`MethodMask`) | **0** | `verify_legacy_origin_fallback`, `verify_double_submit` |

---

## 3. Macro Throughput & Latency (Clean Runs)

Executed via `wrk -t4 -c128 -d10s --latency` after 2s connection-pool warmup:

| Scenario | Throughput (RPS) | Delta vs C1 | Avg | P50 | P90 | P99 | Max | Stdev |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **C1: Raw Proxy `POST` (`plugins: []`)** | **69,640.66** | baseline | 1.83ms | 1.72ms | 2.40ms | 4.41ms | 18.27ms | 642.59us |
| **C2: CSRF Safe `GET` Fast-Path (Gate 1)** | **67,363.57** | `-3.3%` | 1.88ms | 1.79ms | 2.36ms | 3.90ms | 10.76ms | 504.23us |
| **C3: CSRF Modern Cross-Origin `POST` (`Sec-Fetch-Site`, Gate 2)** | **63,171.11** | `-9.3%` | 2.01ms | 1.93ms | 2.48ms | 4.04ms | 10.04ms | 516.35us |
| **C4: CSRF Legacy Double-Submit `POST` (`XSRF-TOKEN`, Gate 2+4)** | **61,566.53** | `-11.6%` | 2.06ms | 2.00ms | 2.61ms | 4.27ms | 9.98ms | 540.52us |

---

## 4. Hardware PMU Counters (10s Steady-State `perf stat`)

| Scenario | PMU Window RPS | Cycles (B) | Instructions (B) | IPC | Inst / Req | Branch Miss (%) | L1d Load Miss (%) | Context Switches | Futex |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **C1: Raw `POST` Baseline** | 52,948.70 | 34.59 | 34.96 | `1.01` | `66,033` | `0.86%` | `10.11%` | 8,783 | **`0`** |
| **C2: CSRF Safe `GET`** | 51,910.51 | 33.96 | 32.99 | `0.97` | `63,558` | `0.90%` | `10.02%` | 9,292 | **`0`** |
| **C3: CSRF Modern `POST`** | 44,410.29 | 31.97 | 30.63 | `0.96` | `68,964` (`+2,931`) | **`0.86%`** | `10.28%` | 7,400 | **`0`** |
| **C4: CSRF Double-Submit `POST`** | 45,860.06 | 33.93 | 33.18 | `0.98` | `72,355` (`+6,322`) | **`0.86%`** | `10.13%` | 7,677 | **`0`** |

---

## 5. `perf record` CPU Symbol Profile (`C3_CSRF_Modern_POST`)

Sampled via `perf record -F 999 -g --call-graph dwarf -p <velda-edge-pid> -- sleep 8` and analyzed with `analyze_perf.py`:

| Architectural Layer | CPU Overhead (%) | Attributed Scope & Notes |
| :--- | :---: | :--- |
| **Linux Kernel Network Stack** | `61.66%` | Kernel socket I/O & TCP transmission (`[kernel.kallsyms]`) |
| **Component Serving Hot Path** | `39.87%` | Velda HTTP/1.1 & `epoll` hot path (`CsrfPlugin` < `0.01%`, zero standalone hotspot leaves) |
| **Kernel Slab / `sk_buff` Allocators** | `3.33%` | Kernel network buffer slab only; **`0.00%`** userspace heap `alloc::*` |
| **Tokio Runtime & Transport** | `0.04%` | Async runtime driver overhead |
| **Userspace Mutex Contention** | `0.06%` | Runtime/libc locks only; zero `csrf` lock symbols |
