# Performance Benchmark Specification: `elasticsearch_log_shipper`

This document specifies the standardized benchmarking framework for the [`elasticsearch_log_shipper`](./) background log shipper in **Velda Edge** (`crates/velda-plugin/src/background/elasticsearch_log_shipper/`).

> [!NOTE]
> This document serves as the **Standardized Benchmark Specification Form**. All empirical data cells remain in a pending state (`*[TBD]*`) awaiting execution on dedicated cloud VM clusters.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Dedicated Cloud Testbed Specification
*(Pending Setup — awaiting dedicated physical/virtual hardware testbed provisioning).*

- **Network Fabric**: *[Pending Setup]*
- **Gateway Server (VM2)**: *[Pending Setup]*
- **Load Generation Node (VM1)**: *[Pending Setup]*
- **Elasticsearch / OpenSearch Receiver Node**: *[Pending Setup]*

### 1.2 5-Tier Diagnostic Measurement Framework
1. **Macro Tier (Shipper Throughput & Latency)**: Records/sec shipped (`shipped_records`), `_bulk` round-trip latency (P50, P90, P99, P99.9, Max), and failed push rate (`failed_pushes`).
2. **Domain Dynamics & SLA Diagnostics**: NDJSON pointer-stamping throughput (`format_bulk_ndjson`), `gzip` compression ratio/time, and `TelemetryStore` drain lag.
3. **System Resource & Core Isolation Tier**: Background task CPU utilization, steady-state RSS (`payload_buf` / `compress_buf` / `resp_buf` reuse), and zero Frontline worker stall verification.
4. **Micro Tier (Kernel & OS)**: Syscalls per `_bulk` flush, context switches, and keep-alive connection reuse rate (`etp.acquire()`).
5. **Micro Tier (HPC & CPU Hardware Counters)**: Instructions per `TelemetryRecord` formatted, IPC, L1D/L2 cache miss rate (with `_MM_HINT_T1` prefetch on `x86_64`).

---

## 2. Comprehensive 6-Scenario Test Matrix

```text
+---------------------------------------------------------------------------------------------------+
|                        COMPREHENSIVE 6-SCENARIO BENCHMARK MATRIX                                  |
+------------------------------------+------------------------------+-------------------------------+
| Scenario Identifier                | Workload & Traffic Pattern   | Evaluation Objective          |
+------------------------------------+------------------------------+-------------------------------+
| ES 01: Steady-State Baseline       | 50K RPS Frontline -> ES _bulk| Floor shipping throughput     |
| ES 02: Gzip vs Uncompressed        | gzip vs none (256 batches)   | CPU vs wire-bytes trade-off   |
| ES 03: Large Batch Drain Skew      | max_drain_batches = 4096     | Buffer reuse & tail latency   |
| ES 04: Connection EOF Reconnect    | Periodic server keepalive EOF| In-place reconnect() recovery |
| ES 05: Backend Outage / Cool-Down  | ES down (>10 failures -> 30s)| RAM guard batch-discard proof |
| ES 06: Sustained Longevity Test    | 50K RPS x 300s               | Zero-leak RSS stability       |
+------------------------------------+------------------------------+-------------------------------+
```

---

## 3. Scenario Metric Template (ES 01 – ES 06)

| Metric | ES 01 (Baseline) | ES 02 (Gzip vs None) | ES 03 (Large Batch) | ES 04 (EOF Reconnect) | ES 05 (Outage Cool-Down) | ES 06 (Longevity 300s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Shipped Records/sec | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| `_bulk` P50 / P99 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| `format_bulk_ndjson` ns/rec | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| `failed_pushes` | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Background RSS Drift | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Frontline P99 Impact | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
