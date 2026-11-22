# Performance Benchmark Specification: HTTPS Redirect (`https_redirect`)

This document specifies the verified release assembly profile and empirical KVM measurements for [`https_redirect`](crates/velda-plugin/src/pre_route/https_redirect/).

---

## 1. Verified Release Assembly Profile (`x86_64`)

- **Target Symbol**: `HttpsRedirectPlugin::on_request`
- **Optimized Size**: **142 lines** (reduced from 614 lines via `#[cold]` split)
- **HTTPS Fast-Path Stack Frame**: **32 bytes** (`sub rsp, 32`, down 98.6% from `2,312` bytes)
- **Heap Allocations on Fast Path**: **0**
- **Location Construction (`<= 2048B` URI)**: **0 heap allocations** (stack `[u8; 2048]` buffer inside cold path)

---

## 2. Empirical KVM Load Measurement (`velda-gw`, 2 vCPU x86_64)

- **Command**: `oha -z 5s -c 64 --no-tui http://192.168.122.10:8080/test`
- **Response**: `308 Permanent Redirect` (`location: https://192.168.122.10:8443/test`)

| Metric | Measured Value |
| :--- | :---: |
| **Duration** | `5.00 s` |
| **Total Requests** | `597,886` |
| **Throughput** | **`119,506 req/s`** |
| **Success Rate** | `100.00%` |
| **Min / Fastest** | `0.059 ms` |
| **P50 Latency** | `0.486 ms` |
| **P90 Latency** | `0.809 ms` |
| **P99 Latency** | `1.076 ms` |
| **P99.9 Latency** | `3.300 ms` |
