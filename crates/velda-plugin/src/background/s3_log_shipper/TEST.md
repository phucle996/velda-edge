# Verification & Test Report: `s3_log_shipper`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::s3_log_shipper
cargo test -p velda-plugin --test s3_es_shipper_test
```

### Observed Results
- **Unit tests (`background::s3_log_shipper`) — 4/4 passed**:
  - `background::s3_log_shipper::sigv4::tests::test_unix_secs_to_utc_ymdhms` ... **ok**
  - `background::s3_log_shipper::sigv4::tests::test_sigv4_signing_determinism_and_cache` ... **ok**
  - `background::s3_log_shipper::validation::tests::test_valid_s3_log_shipper_config_and_defaults` ... **ok**
  - `background::s3_log_shipper::validation::tests::test_invalid_s3_log_shipper_config` ... **ok**
- **Integration tests (`s3_es_shipper_test`, S3 cases) — passed** (`6 passed; 0 failed` across the suite):
  - `test_validation_accepts_es_and_s3_background_plugins` ... **ok**
  - `test_s3_shipper_end_to_end_sigv4_and_zstd` ... **ok**
  - `test_s3_etp_registry_wiring_sts_token_and_reconnect_recovery` ... **ok**

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` SeaweedFS S3)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**51,815.18 req/sec**, avg latency **623.05 us**).
- **`perf stat` (`velda-edge`)**: `16.01B` cycles, `15.86B` instructions (**0.99 IPC**), `26.63M` cache-misses, `18,926` context-switches, `3` cpu-migrations.
- **SeaweedFS S3 (`:9005`)**: **18** Hive-partitioned `.jsonl.gz` objects uploaded (`kvm-bench/year=2026/month=11/day=23/hour=17/velda-*.jsonl.gz`), **0 WARN/ERROR** in `velda-edge`.
