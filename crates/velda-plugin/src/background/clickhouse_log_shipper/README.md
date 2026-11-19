# ClickHouse Log Shipper (`clickhouse_log_shipper`)

`clickhouse_log_shipper` is an ultra-high-throughput, zero-allocation background observability plugin for **Velda Edge**. It continuously drains 100% full access log records from core-local lock-free SPSC rings, serializes them directly into ClickHouse's native wire formats (`RowBinary` or `JSONEachRow`), and transmits them over persistent Keep-Alive HTTP/1.1 or TLS/mTLS sessions directly into ClickHouse MergeTree tables.

---

## 1. Key Architectural Features

- **Strict Isolation from Serving Hot Path**: Executes exclusively on companion Shadow Logistics Workers (`velda-shadow-{id}`) pinned to the same CPU cores under Linux `SCHED_IDLE`. Frontline Traffic Workers never touch sockets, never format strings, and never block.
- **100% Observability Integrity**: Ships 100% full access logs without sampling or loss, preserving complete audit trails, request ID tracking, and distributed tracing correlation.
- **MergeTree-Optimized Batch Coalescing**: Designed around ClickHouse's LSM-like columnar storage characteristics. Coalesces thousands of log records into dense columnar blocks per HTTP push, preventing "Too many parts" merge storms on the database server.
- **Zero-Allocation Wire Serializer**: Formats records directly into pre-reserved payload buffers using branchless division-free integer lookup tables and 1-byte LEB128 length back-patching. Eliminates dynamic heap reallocations during steady-state ingestion.
- **Zero-Retry Drop & Fast Fail**: If ClickHouse terminates a connection or experiences network degradation, the plugin fails fast, tears down the socket immediately to prevent half-closed connection poisoning, and avoids sleep-backoff deadlocks.
- **30-Second Cool-Down Memory Shield**: If consecutive push operations fail repeatedly, the plugin activates a protective 30-second cool-down window, dropping incoming batches lock-free to strictly cap gateway RAM footprint.
- **Automated DDL Schema Verification**: Automatically verifies and creates the target `velda_access_logs` table schema upon startup with hardware-efficient columnar compression codecs (`DoubleDelta`, `T64`, `ZSTD`).

---

## 2. Configuration Reference

Declare the plugin under the `"background"` phase in `/etc/velda/config/plugins.json`:

```json
{
  "id": "clickhouse_log_shipper",
  "phase": "background",
  "config": {
    "endpoint": "http://192.168.1.50:8123",
    "transport_security": "plain",
    "database": "velda_analytics",
    "format": "row_binary",
    "username": "default",
    "password": "secret_password",
    "flush_interval_ms": 1000,
    "max_batch_size": 8000,
    "connect_timeout_ms": 2000,
    "request_timeout_ms": 5000
  }
}
```

### Configuration Parameters

| Field | Type | Default | Valid Range | Description |
| :--- | :---: | :---: | :---: | :--- |
| `endpoint` | `String` | **Required** | `http://` or `https://` | ClickHouse HTTP ingestion URL (e.g. `http://clickhouse:8123` or `https://clickhouse.internal:8443`). |
| `transport_security` | `String` | `"plain"` | `"plain"`, `"tls"`, `"mtls"` | Outbound transport security mode. Strictly enforced at compile time against the endpoint scheme. |
| `database` | `String` | **Required** | Non-empty string | Target ClickHouse database name (must pre-exist on ClickHouse server). |
| `format` | `String` | `"row_binary"` | `"row_binary"`, `"json"`, `"json_each_row"` | Wire serialization format. `row_binary` is recommended for high-performance production. |
| `username` | `String` | `"default"` | String | ClickHouse authentication username. Transmitted via `X-ClickHouse-User` header. |
| `password` | `String` | `""` | String | ClickHouse authentication password. Transmitted via `X-ClickHouse-Key` header. |
| `flush_interval_ms` | `Integer` | `1000` | `100` – `60000` ms | Batch flush interval. Clamped to $\ge 100\text{ms}$ aligned with coarse clock ticks. |
| `max_batch_size` | `Integer` | `8000` | `1` – `50000` records | Maximum number of access log records coalesced into a single HTTP push payload. |
| `connect_timeout_ms`| `Integer` | `2000` | `100` – `60000` ms | TCP/TLS connection establishment timeout. |
| `request_timeout_ms`| `Integer` | `5000` | `100` – `60000` ms | HTTP push execution timeout for writing payload and receiving HTTP status header. |
| `tls` | `Object` | `{}` | `OutboundTlsConfig` | Outbound TLS/mTLS parameters (CA certificates, client certificate, client private key). |

---

## 3. Transport Security Modes

### Plaintext TCP (`transport_security: "plain"`)
Used for intra-cluster or private network deployments:
- Requires `http://` scheme.
- Disallows client certificates in `tls` config.

### Standard TLS Server Authentication (`transport_security: "tls"`)
Used when connecting to ClickHouse Cloud or remote instances:
```json
{
  "endpoint": "https://clickhouse.prod.internal:8443",
  "transport_security": "tls",
  "tls": {
    "ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "insecure_skip_verify": false
  }
}
```

### Mutual TLS Authentication (`transport_security: "mtls"`)
Used in Zero-Trust environments where both client and server identities are cryptographically validated:
```json
{
  "endpoint": "https://clickhouse.secure.internal:8443",
  "transport_security": "mtls",
  "tls": {
    "ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "client_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
    "client_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    "insecure_skip_verify": false
  }
}
```

---

## 4. ClickHouse Schema & DDL

Upon startup, the plugin executes an automated table creation handshake against ClickHouse. The table is created inside the configured `database` if it does not already exist:

```sql
CREATE TABLE IF NOT EXISTS {database}.velda_access_logs (
    timestamp          DateTime              CODEC(DoubleDelta, ZSTD(1)),
    request_id         UInt64                CODEC(DoubleDelta, ZSTD(1)),
    duration_ns        UInt64                CODEC(T64, ZSTD(1)),
    peer_ip            String                CODEC(ZSTD(1)),
    peer_port          UInt16                CODEC(T64, ZSTD(1)),
    upstream_ip        String                CODEC(ZSTD(1)),
    upstream_port      UInt16                CODEC(T64, ZSTD(1)),
    route_id           UInt16                CODEC(T64, ZSTD(1)),
    status_code        UInt16                CODEC(T64, ZSTD(1)),
    pipe               LowCardinality(String) CODEC(ZSTD(1)),
    method             LowCardinality(String) CODEC(ZSTD(1)),
    url                String                CODEC(ZSTD(3)),
    bytes_rx           UInt32                CODEC(T64, ZSTD(1)),
    bytes_tx           UInt32                CODEC(T64, ZSTD(1)),
    completion_reason  LowCardinality(String) CODEC(ZSTD(1)),
    grpc_status        LowCardinality(String) CODEC(ZSTD(1)),
    user_agent_family  LowCardinality(String) CODEC(ZSTD(1)),
    user_agent_os      LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (route_id, status_code, timestamp)
SETTINGS index_granularity = 8192;
```

### Schema Design Principles
1. **DoubleDelta Codec**: Applied to monotonically increasing or sequential fields (`timestamp`, `request_id`). Compresses successive deltas into 1 to 4 bits per row.
2. **T64 Codec**: Strips leading identical bits from numerical fields (`duration_ns`, ports, byte counters), leaving small integer residuals that compress efficiently under `ZSTD`.
3. **Partitioning by Month (`toYYYYMM`)**: Allows operations teams to drop or detach old data months instantly via `ALTER TABLE velda_access_logs DROP PARTITION` without full table scans.
4. **Sparse Index Sorting Key (`ORDER BY (route_id, status_code, timestamp)`)**: Perfectly aligns with typical edge analytics queries: filtering by route, grouping by HTTP status code, and slicing across time windows.

---

## 5. Production Tuning & Sizing Guide

| Traffic Tier (RPS) | `flush_interval_ms` | `max_batch_size` | Estimated Blocks / Sec | Network Footprint |
| :---: | :---: | :---: | :---: | :---: |
| **Low** (< 1,000) | `1000` – `2000` | `2000` | 0.5 – 1 push/sec | ~150 KB/sec |
| **Medium** (1,000 – 10,000) | `1000` – `2000` | `8000` | 1 – 2 push/sec | ~1.5 MB/sec |
| **High** (10,000 – 50,000) | `2000` – `3000` | `16000` | 2 – 3 push/sec | ~7.5 MB/sec |
| **Extreme** (> 50,000) | `3000` – `5000` | `32000` | 2 – 4 push/sec | ~15 MB/sec |

### Key Operational Recommendations
- **Avoid Micro-Batching (< 500ms)**: Setting `flush_interval_ms` below 500ms creates an excessive number of tiny data parts on ClickHouse, exhausting server merge worker threads and causing write latency spikes.
- **Prefer `RowBinary` Wire Format**: Native binary encoding uses ~60% less bandwidth than `JSONEachRow` and consumes zero string parsing CPU on the ClickHouse server.
- **Pre-Create the Database**: ClickHouse HTTP interface requires that the target database already exists. Ensure `CREATE DATABASE IF NOT EXISTS velda_analytics;` is run prior to launching Velda Edge.
