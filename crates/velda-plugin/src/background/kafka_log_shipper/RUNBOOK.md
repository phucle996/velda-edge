# Operations & Configuration Runbook: `kafka_log_shipper`

---

## 1. Configuration Parameter Reference

### 1.1 Plugin Config (`plugins.json` -> `config`)

| Field | Type | Default | Constraints / Validation | Description |
| :--- | :--- | :--- | :--- | :--- |
| `etp_id` | `String` | *(required)* | Must not be empty | Identifier of the `kafka` entry in `EtpRegistry` (`etp.json`). |
| `topic` | `String` | *(required)* | `1..=249` chars, `[a-zA-Z0-9._-]`, not `.` or `..` | Target Kafka topic name. |
| `format` | `String` | `"json"` | `"json"` \| `"text"` \| `"binary"` | Record payload encoding format. |
| `compression` | `String` | `"zstd"` | `"none"` \| `"gzip"` \| `"zstd"` | RecordBatch v2 compression codec. |
| `acks` | `String` | `"leader"` | `"none"` (`0`) \| `"leader"` (`1`) \| `"all"` (`-1`) | Kafka Produce acknowledgment mode. |
| `partitioner` | `String` | `"round_robin"` | `"round_robin"` \| `"route_id"` \| `"peer_ip"` | Partition selection strategy across active topic partitions. |
| `flush_interval_ms` | `u64` | `1000` | `10..=60000` | Flush interval in milliseconds. |
| `max_batch_size` | `usize` | `1000` | `1..=50000` | Maximum records per batch drain. |
| `request_timeout_ms` | `u64` | `5000` | `100..=60000` | Broker request/response timeout in milliseconds. |
| `metadata_refresh_ms` | `u64` | `300000` | Positive integer | Periodic Metadata v4 refresh interval (also triggered on `error_code` 3 or 6). |

### 1.2 Referenced ETP Entry (`etp.json`, `kind: "kafka"`)

| Field | Location | Default | Description |
| :--- | :--- | :--- | :--- |
| `id` | Top-level | *(required)* | Must match `etp_id`. |
| `kind` | Top-level | *(required)* | Must be `"kafka"`. |
| `settings.endpoint` | `settings` | *(required)* | Broker `host:port` (default port `9092`). |
| `settings.client_id` | `settings` | `"velda-edge"` | Kafka wire `client_id` prebaked into Metadata & Produce headers. |
| `settings.sasl_mechanism` / `username` / `password` | `settings` | `null` | Optional SASL/PLAIN authentication executed at ETP connect time. |

---

## 2. Recommended Presets

### 2.1 High-Throughput Production (Zstd + Leader Acks)
```json
{
  "etp_id": "kafka-prod",
  "topic": "velda-access-logs",
  "format": "json",
  "compression": "zstd",
  "acks": "leader",
  "partitioner": "round_robin",
  "flush_interval_ms": 500,
  "max_batch_size": 4000,
  "request_timeout_ms": 5000
}
```

### 2.2 Redpanda / Local Broker (Gzip)
```json
{
  "etp_id": "kafka-obs",
  "topic": "velda-logs",
  "format": "json",
  "compression": "gzip",
  "acks": "leader",
  "flush_interval_ms": 500,
  "max_batch_size": 256
}
```

---

## 3. Incident Triage Playbook

| Symptom | Likely Root Cause | Resolution |
| :--- | :--- | :--- |
| `Kafka partition leader changed; refreshing metadata` (`error_code` 3/6) | Broker leadership failover or topic rebalance. | Automatic: `wire_state.refresh_metadata()` runs immediately. |
| `Unexpected end of Kafka wire buffer` or `early eof` | Produce API version mismatch between client and broker. | Verify broker supports Produce v7 (supported by Kafka 2.1+–3.x and Redpanda v24.x). |
| `Kafka shipper failed 10 consecutive times; entering 30s cool-down` | Broker unreachable, topic missing, or SASL auth failure. | Check broker reachability, topic creation, and `etp.json` SASL credentials. |
