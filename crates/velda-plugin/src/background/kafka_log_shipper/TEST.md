# Verification & Test Report: `kafka_log_shipper`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::kafka_log_shipper
cargo test -p velda-plugin --test kafka_suite_test
```

### Observed Results
- **Unit tests (`background::kafka_log_shipper`) — 11/11 passed**:
  - `format::binary::tests::test_format_single_record_binary` ... **ok**
  - `format::tests::test_zigzag_varint` ... **ok**
  - `format::tests::test_crc32c_standard_vectors` ... **ok**
  - `format::text::tests::test_format_single_record_text_grpc` ... **ok**
  - `format::text::tests::test_format_single_record_text_http` ... **ok**
  - `format::json::tests::test_format_single_record_json` ... **ok**
  - `protocol::tests::test_produce_request_framing` ... **ok**
  - `protocol::tests::test_sasl_plain_payload` ... **ok**
  - `compress::tests::test_compress_records_all_codecs` ... **ok**
  - `validation::tests::test_valid_kafka_log_shipper_config` ... **ok**
  - `validation::tests::test_invalid_kafka_log_shipper_config` ... **ok**
- **Integration tests (`kafka_suite_test`) — 8/8 passed**:
  - `test_kafka_config_validation_success` ... **ok**
  - `test_kafka_config_validation_rules` ... **ok**
  - `test_kafka_factory_registration` ... **ok**
  - `test_kafka_attached_to_route_rejected` ... **ok**
  - `test_kafka_shipper_e2e_sasl_plain` ... **ok**
  - `test_kafka_shipper_leader_failover_recovery` ... **ok**
  - `test_kafka_shipper_e2e_produce_success` ... **ok**
  - `test_kafka_etp_registry_wiring_and_reconnect_recovery` ... **ok**

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` Redpanda v24.2.11)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**50,787.37 req/sec**, avg latency **650.12 us**).
- **`perf stat` (`velda-edge`)**: `15.08B` cycles, `14.90B` instructions (**0.99 IPC**), `29.46M` cache-misses, `18,789` context-switches, `0` cpu-migrations.
- **Redpanda (`:9092`)**: Topic `velda-logs` high-watermark increased `27,760` -> `68,916` (**+41,156 messages** committed), **0 WARN/ERROR** in `velda-edge`.
