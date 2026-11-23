# Performance Benchmark Specification: `kafka_log_shipper`

> [!NOTE]
> Standardized benchmark specification form (`*[TBD]*` cells await dedicated multi-node runs). For live KVM measurements against Redpanda v24.2.11, see [TEST.md](./TEST.md).

```text
+---------------------------------------------------------------------------------------------------+
|                        COMPREHENSIVE 6-SCENARIO BENCHMARK MATRIX                                  |
+------------------------------------+------------------------------+-------------------------------+
| Scenario Identifier                | Workload & Traffic Pattern   | Evaluation Objective          |
+------------------------------------+------------------------------+-------------------------------+
| KF 01: Steady-State Baseline       | 50K RPS -> Produce v7 (zstd) | Floor produce throughput      |
| KF 02: Codec Comparison            | zstd vs gzip vs none         | CPU vs wire-bytes trade-off   |
| KF 03: Multi-Partition Skew        | 16 partitions, round_robin   | Batch packing & CRC32C cost   |
| KF 04: Leader Failover (Err 3/6)   | Broker leadership change     | Metadata v4 refresh recovery  |
| KF 05: Broker Outage / Cool-Down   | Broker down (>10 failures)   | 30s RAM-guard batch discard   |
| KF 06: Sustained Longevity Test    | 50K RPS x 300s               | Zero-leak RSS stability       |
+------------------------------------+------------------------------+-------------------------------+
```

| Metric | KF 01 | KF 02 | KF 03 | KF 04 | KF 05 | KF 06 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Produced Records/sec | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Produce P50 / P99 Latency | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| CRC32C + Codec Time | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| Background RSS Drift | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
