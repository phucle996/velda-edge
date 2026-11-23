# Performance Benchmark Specification: `s3_log_shipper`

> [!NOTE]
> Standardized benchmark specification form (`*[TBD]*` cells await dedicated cluster runs). For live KVM measurements against SeaweedFS S3, see [TEST.md](./TEST.md).

---

## 1. 6-Scenario Benchmark Matrix

```text
+---------------------------------------------------------------------------------------------------+
|                        COMPREHENSIVE 6-SCENARIO BENCHMARK MATRIX                                  |
+------------------------------------+------------------------------+-------------------------------+
| Scenario Identifier                | Workload & Traffic Pattern   | Evaluation Objective          |
+------------------------------------+------------------------------+-------------------------------+
| S3 01: Steady-State Baseline       | 50K RPS Frontline -> S3 PUT  | Floor archival throughput     |
| S3 02: Codec Trade-Off             | zstd vs gzip vs none         | CPU vs object size ratio      |
| S3 03: Large Batch Spillover       | max_drain_batches = 4096     | Buffer reuse & RSS ceiling    |
| S3 04: Keep-Alive EOF Reconnect    | Server closes idle socket    | In-place reconnect() recovery |
| S3 05: Object Store Outage         | S3 down (>10 failures -> 30s)| Cool-down batch discard proof |
| S3 06: Sustained Longevity Test    | 50K RPS x 300s               | Zero-leak RSS stability       |
+------------------------------------+------------------------------+-------------------------------+
```

| Metric | S3 01 | S3 02 | S3 03 | S3 04 | S3 05 | S3 06 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Shipped Records/sec | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| `PUT` P50 / P99 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Compression Ratio | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Background RSS Drift | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
