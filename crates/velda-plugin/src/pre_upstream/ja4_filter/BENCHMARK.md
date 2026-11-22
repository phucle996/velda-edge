# Performance Benchmark Specification: JA4 TLS Filter (`ja4_filter`)

This document specifies the benchmark methodology, verified release assembly profile, and empirical KVM measurements for [`ja4_filter`](crates/velda-plugin/src/pre_upstream/ja4_filter/).

---

## 1. Verified Release Assembly & Symbol Audit (`x86_64`)

- **Target Symbol**: `Ja4FilterPlugin::on_upstream_request`
- **Total Instructions**: **71 lines**
- **Stack Frame**: **0 bytes** (`sub rsp` absent)
- **External Calls (`call`)**: **0**
- **Heap Allocations (`alloc::*`)**: **0**
- **Register Spills**: **0**

---

## 2. Empirical KVM Load Measurement (`velda-gw`, 2 vCPU x86_64)

- **Command**: `oha -z 5s -c 64 --insecure --no-tui https://192.168.122.10:8443/`
- **Pipeline**: TLS termination (`rustls` + `peek_tcp_ja4`) + `pre_upstream` `ja4_filter` + upstream HTTP/1.1 proxy

| Metric | Measured Value |
| :--- | :---: |
| **Duration** | `5.00 s` |
| **Total Requests** | `257,161` |
| **Throughput** | **`51,413 req/s`** |
| **Success Rate** | `100.00%` |
| **P50 Latency** | `1.15 ms` |
| **P90 Latency** | `1.59 ms` |
| **P99 Latency** | `3.74 ms` |
| **Worker CPU Balance** | `velda-worker-0`: `69.1%`, `velda-worker-1`: `69.9%` |
| **Cgroup Memory** | `34.0 MB` (peak `34.8 MB`) |

---

## 3. Adversarial Flood Verification (`velda-gw`)

- **Scenario**: `500` rapid TCP connect + partial `ClientHello` + immediate `RST` connections in `0.03s` on TLS port `8443`.
- **Result**: `0` crashes, FD count remained `46` (zero FD leak), memory stayed at `28.6 MB` (peak `29.2 MB`).
