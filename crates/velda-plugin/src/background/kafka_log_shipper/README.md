# Kafka Log Shipper (`kafka_log_shipper`)

`kafka_log_shipper` is a native, pure-Rust background observability plugin for the **Velda Edge Data Plane**. It drains access log records from core-local SPSC rings, encodes them into **Kafka RecordBatch v2** wire frames (`magic = 2`), compresses batches with **Zstandard** or **Gzip**, and produces them over pooled connections borrowed from [`velda_etp::kafka::KafkaEtp`](../../../velda-etp/src/kafka/mod.rs).

---

## 1. Key Architectural Features

- **Strict Frontline Isolation**: Executes exclusively on companion Shadow Logistics Workers (`velda-shadow-{id}`) pinned under Linux `SCHED_IDLE`. Frontline Traffic Workers never touch sockets, never format strings, and never block.
- **Shared `velda-etp` Connection Lifecycle**: Broker endpoints, `client_id`, TCP/TLS/mTLS handshakes, optional SASL authentication (`SaslHandshake` v1 + `SaslAuthenticate` v1 on connect), and lock-free `EtpPageArena` slot leasing (`KafkaLease`) live in `velda-etp::kafka`.
- **Zero Extra C-Dependencies**: Pure Rust Kafka binary wire protocol (`Produce` v7, `Metadata` v4, `SaslHandshake` v1, `SaslAuthenticate` v1, Castagnoli CRC32-C).
- **Zero Consumer Rebalance Risk**: Strictly produces batches (`ProduceRequest`, API key 0) without joining consumer groups (`JoinGroup`/`SyncGroup`/`Heartbeat`).
- **Dynamic Leader Failover & In-Place Reconnect**: Automatically triggers `Metadata` refreshes on `NOT_LEADER_OR_FOLLOWER` (6) or `UNKNOWN_TOPIC_OR_PARTITION` (3), retries once via `lease.reconnect().await` on idle broker disconnects, and marks unhealthy slots for background recreation on timeouts or persistent errors.
- **30-Second Cool-Down Memory Shield**: After >10 consecutive push failures, drops incoming batches during cool-down to cap memory footprint.

---

## 2. Configuration Reference (`etp.json` + `plugins.json`)

Define the broker pool in `/etc/velda/config/etp.json`:

```json
{
  "id": "kafka-main",
  "kind": "kafka",
  "pool": {
    "pool_size": 4,
    "connect_timeout_ms": 2000,
    "idle_timeout_ms": 60000
  },
  "settings": {
    "brokers": ["10.0.1.10:9092", "10.0.1.11:9092"],
    "client_id": "velda-edge-prod",
    "sasl_mechanism": "none"
  }
}
```

Declare the plugin in `/etc/velda/config/plugins.json`:

```json
{
  "id": "kafka_log_shipper",
  "config": {
    "etp_id": "kafka-main",
    "topic": "velda-access-logs",
    "format": "json",
    "compression": "zstd",
    "acks": "leader",
    "partitioner": "round_robin",
    "flush_interval_ms": 1000,
    "max_batch_size": 8000,
    "request_timeout_ms": 5000,
    "metadata_refresh_ms": 300000
  }
}
```

### Plugin Parameters

| Field | Type | Default | Valid Range | Description |
| :--- | :---: | :---: | :---: | :--- |
| `etp_id` | `String` | **Required** | Non-empty `kafka` ETP ID | Reference to a `kind: "kafka"` entry in `etp.json`. |
| `topic` | `String` | **Required** | `[a-zA-Z0-9._-]`, $\le 249$ chars | Target Kafka topic name. |
| `format` | `String` | `"json"` | `"json"`, `"text"`, `"binary"` | Record payload encoding format. |
| `compression` | `String` | `"zstd"` | `"none"`, `"gzip"`, `"zstd"` | Kafka RecordBatch v2 compression codec. |
| `acks` | `String` | `"leader"` | `"none"` (0), `"leader"` (1), `"all"` (-1) | Produce acknowledgment durability level. |
| `partitioner` | `String` | `"round_robin"` | `"round_robin"`, `"route_id"`, `"peer_ip"` | Partition distribution strategy for access log records. |
| `flush_interval_ms` | `Integer` | `1000` | `100` – `60000` ms | Batch flush cadence on the Shadow Worker. |
| `max_batch_size` | `Integer` | `8000` | `1` – `50000` records | Maximum records grouped into a produce cycle. |
| `request_timeout_ms`| `Integer` | `5000` | `100` – `60000` ms | Kafka request/response timeout for Produce and Metadata RPCs. |
| `metadata_refresh_ms`| `Integer` | `300000` | `1000` – `3600000` ms | Periodic background metadata refresh interval. |

---

## 3. Transport Security & SASL Modes (`etp.json`)

TLS certificates, mTLS client identities, and SASL/PLAIN credentials belong on the `kafka` ETP entry in `etp.json`:

```json
{
  "id": "kafka-secure",
  "kind": "kafka",
  "tls": {
    "ca_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "client_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "client_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
  },
  "settings": {
    "brokers": ["kafka-secure.example.com:9094"],
    "client_id": "velda-edge-prod",
    "sasl_mechanism": "plain",
    "sasl_username": "velda-producer",
    "sasl_password": "..."
  }
}
```
