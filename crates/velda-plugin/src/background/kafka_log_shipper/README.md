# Kafka Log Shipper (`kafka_log_shipper`)

`kafka_log_shipper` is a native, pure-Rust, high-throughput background observability plugin for the **Velda Edge Data Plane**. It drains full access log records from core-local lock-free SPSC rings, encodes them into native **Kafka RecordBatch v2** wire frames (`magic = 2`), compresses them using **Zstandard** or **Gzip**, and produces them directly to partition leader brokers over persistent TCP or TLS connections.

---

## 1. Key Architectural Features

- **Strict Frontline Isolation**: Executes exclusively on companion Shadow Logistics Workers (`velda-shadow-{id}`) pinned to the same CPU cores under Linux `SCHED_IDLE`. Frontline Traffic Workers never touch sockets, never format strings, and never block.
- **Zero Extra C-Dependencies**: Pure Rust implementation of the Kafka wire protocol (Produce v7, Metadata v4, SaslHandshake v1, SaslAuthenticate v1, and Castagnoli CRC32-C). Eliminates crashes, memory leaks, and compilation hurdles associated with C libraries like `librdkafka`.
- **Zero Rebalance Risk When Scaling Gateway Nodes**:
  - `kafka_log_shipper` is strictly a **Kafka Producer**, not a Consumer!
  - It does **not** join any consumer group and never sends `JoinGroup`, `SyncGroup`, or `Heartbeat` requests.
  - Gateway nodes can be scaled horizontally from 2 to 100+ pods without causing any consumer rebalance storms or broker churn.
- **Auto-Generated Per-Node `client_id`**: Dynamically derives `velda-edge-{hostname}-{pid}` upon process start. Ensures that each gateway container or VM is identifiable in Kafka broker connection logs and JMX metrics without requiring manual per-node configuration.
- **Automatic Partition Leader Failover**: Detects Kafka cluster partition reassignments (`NOT_LEADER_OR_FOLLOWER` error code 6) and automatically triggers Metadata refreshes to route subsequent produce batches to the new leader broker without downtime.
- **High-Performance RecordBatch v2 Framing**: Implements Kafka's batch layout with hardware-accelerated Castagnoli CRC32-C checksumming, LEB128 Zigzag varints, and optional block-level Zstandard (`zstd`) compression.
- **30-Second Cool-Down Memory Shield**: If consecutive push operations fail repeatedly, the plugin activates a protective 30-second cool-down window, dropping incoming batches lock-free to strictly cap gateway RAM footprint.

---

## 2. Configuration Reference

Declare the plugin in `/etc/velda/config/plugins.json`:

```json
{
  "id": "kafka_log_shipper",
  "config": {
    "brokers": [
      "10.0.1.10:9092",
      "10.0.1.11:9092"
    ],
    "topic": "velda-access-logs",
    "transport_security": "plain",
    "sasl_mechanism": "none",
    "sasl_username": "",
    "sasl_password": "",
    "compression": "zstd",
    "acks": "leader",
    "partitioner": "round_robin",
    "flush_interval_ms": 1000,
    "max_batch_size": 8000,
    "connect_timeout_ms": 2000,
    "request_timeout_ms": 5000,
    "metadata_refresh_ms": 300000
  }
}
```

> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. Execution stage is fixed at compile time (`BackgroundPlugin`).

### Configuration Parameters

| Field | Type | Default | Valid Range | Description |
| :--- | :---: | :---: | :---: | :--- |
| `brokers` | `Array<String>` | **Required** | Non-empty array | Seed Kafka broker addresses (e.g. `["kafka-1:9092", "kafka-2:9092"]`). |
| `topic` | `String` | **Required** | `[a-zA-Z0-9._-]`, $\le 249$ chars | Target Kafka topic name (must pre-exist on Kafka cluster). |
| `transport_security` | `String` | `"plain"` | `"plain"`, `"tls"`, `"mtls"` | Transport security mode. Plaintext TCP or TLS encrypted via `velda-tls`. |
| `tls` | `Object` | `{}` | `OutboundTlsConfig` | Outbound TLS parameters (`ca_pem`, `client_cert_pem`, `client_key_pem`, `insecure_skip_verify`). |
| `sasl_mechanism` | `String` | `"none"` | `"none"`, `"plain"`, `"scram_sha_256"` | SASL authentication mechanism. |
| `sasl_username` | `String` | `""` | String | SASL username. Required if `sasl_mechanism != "none"`. |
| `sasl_password` | `String` | `""` | String | SASL password. Required if `sasl_mechanism != "none"`. |
| `compression` | `String` | `"zstd"` | `"none"`, `"gzip"`, `"zstd"` | Kafka RecordBatch v2 compression codec. |
| `acks` | `String` | `"leader"` | `"none"` (0), `"leader"` (1), `"all"` (-1) | Produce acknowledgment durability level. |
| `partitioner` | `String` | `"round_robin"` | `"round_robin"`, `"route_id"`, `"peer_ip"` | Partition distribution strategy for access log records. |
| `flush_interval_ms` | `Integer` | `1000` | `100` – `60000` ms | Batch flush cadence on the Shadow Worker. |
| `max_batch_size` | `Integer` | `8000` | `1` – `50000` records | Maximum number of records grouped into a single produce batch. |
| `connect_timeout_ms`| `Integer` | `2000` | `100` – `60000` ms | TCP/TLS connection timeout. |
| `request_timeout_ms`| `Integer` | `5000` | `100` – `60000` ms | Kafka request/response timeout for Produce and Metadata calls. |
| `metadata_refresh_ms`| `Integer` | `300000` | `1000` – `3600000` ms | Periodic background metadata refresh interval (default: 5 minutes). |

---

## 3. Transport Security Modes

### Plaintext TCP (`transport_security: "plain"`)
Typically used for internal Kubernetes cluster networks on port 9092:
```json
{
  "brokers": ["kafka.internal:9092"],
  "transport_security": "plain"
}
```

### One-Way TLS (`transport_security: "tls"`)
Authenticates Kafka broker certificate authority (typically port 9093/9094):
```json
{
  "brokers": ["kafka-prod.example.com:9093"],
  "transport_security": "tls",
  "tls": {
    "ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "insecure_skip_verify": false
  }
}
```

### Mutual TLS (`transport_security: "mtls"`)
Authenticates both broker and gateway client identity via cryptographic certificates:
```json
{
  "brokers": ["kafka-secure.example.com:9094"],
  "transport_security": "mtls",
  "tls": {
    "ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "client_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "client_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    "insecure_skip_verify": false
  }
}
```
