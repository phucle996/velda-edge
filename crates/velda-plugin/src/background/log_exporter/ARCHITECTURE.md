# Internal Architecture Specification (`log_exporter`)

## 1. Execution & Memory Isolation Model

`log_exporter` decouples **wait-free Frontline record emission** from **multi-sink asynchronous encoding, compression, signing, and transport** (`pbww = 80` on `SCHED_IDLE`):

```text
  Frontline Core (velda-worker-{0..N-1}, SCHED_OTHER)
    ├─► StatusSampling::keep(status) ─► 3-ALU Weyl Gate ((c + GOLDEN) >> 1) < threshold
    └─► SpscTelemetryRing[core] (ContiguousArena 2MB HugePage, 8,192 × 256B):
          └─ _mm256_stream non-temporal write -> bypasses L1d cache -> WC buffer -> RAM
          │
          ▼ flush_lane(worker_id, 512) -> TelemetryBatchPool (64 × 64KB Slots, leases=AtomicUsize)
  Shadow Worker (velda-shadow-{i}, SCHED_IDLE)
    └─► JoinSet (independent target loop per configured sink):
          subscriber.drain_batches(max_drain_batches) -> Provider serialize + compress -> ETP/File push
            └─ drop(batches) -> leases.fetch_sub(1) -> slot returned to pool at 0 (0 OS free)
```

## 2. Full `256 B` Cacheline Layout & Zero-Copy Multi-Sink Fan-Out

Unlike `trace_exporter` (which restricts reads to Cache Line 0), `log_exporter` serializes the full `256 B` (`#[repr(C, align(64))]`) `TelemetryRecord`:
- **Cache Line 0 (`0..64 B`)**: `request_id`, `duration_ns`, `peer_ip` (`16 B`), `ingress_time_nanos`, `bytes_rx/tx`, `upstream_ip/port`, `route_id`, `upstream_id`, `method`, `pipe`, `internal_status`.
- **Cache Lines 1–3 (`64..256 B`)**: `ua_major_version`, `rpc_msgs_or_flags`, `ua_family`, `ua_os`, `url_len`, and `url_bytes: [u8; 184]` (`scheme + host + path_and_query`).
- **L2 Non-Polluting Prefetch (`_MM_HINT_T1`)**: High-throughput serializers such as ClickHouse `format_row_binary` prefetch `(rec + 4)` across all 4 cache lines (`+0/+64/+128/+192`) into L2 while scanning sequential records without thrashing L1d.
- **Zero-Copy Pool Reference Counting**: Enabling all `7` targets simultaneously creates `7` subscribers against the same `TelemetryBatchPool` (`64 × 64 KiB = 4 MiB` total). Records are never cloned across targets; dropping each `TelemetryBatch` decrements `leases: AtomicUsize` atomically until the last consumer clears `records.len = 0`.

## 3. Pure Provider Dispatch Boundary & Protocol Formats (`providers/`)

| Provider Engine | Protocol & Framing Engine | Compression / Signing / Auth | Zero-Alloc & Batch Mechanics |
| :--- | :--- | :--- | :--- |
| **`local` (`sink.rs`, `format/`)** | Text, NDJSON, or Common Log Format (`clf`) written to `stdout` / `stderr` / persistent `File` (`O_CREAT \| O_APPEND`). | Buffered OS `write_all` + `flush`. | Persistent file descriptor opened once at `StdLogSink::init`, eliminating per-flush `open`/`close` syscalls. |
| **`clickhouse` (`row_binary.rs`, `json.rs`, `wire.rs`)** | `RowBinary` (LE binary ints + LEB128 varint strings across 15 columns) or `JSONEachRow` over HTTP/1.1 Keep-Alive. | `X-ClickHouse-User` / `X-ClickHouse-Key` pre-baked in HTTP prefix; idempotent `CREATE TABLE IF NOT EXISTS` DDL on `prepare()`. | Single upfront `out.reserve(total_recs * 256)` + `MAX_ROW_BYTES = 512` guard; `core::ptr::write_unaligned` packed stores + `fast_write_ip_ptr`; terminal `out.set_len`. |
| **`elasticsearch` (`format.rs`, `wire.rs`)** | HTTP/1.1 `POST {base_path}/_bulk` appending `{action}\n{doc}\n` NDJSON pairs (`Create` for Data Streams \| `Index` upsert). | Optional `Content-Encoding: gzip` (`flate2`); `ApiKey`, `Basic`, or `Bearer` auth pre-baked in `bake_bulk_prefix`. | Streams HTTP response body scanning `"\"errors\":true"` without building a response DOM. |
| **`loki` (`json/`, `proto/`, `wire.rs`)** | HTTP JSON `POST /loki/api/v1/push` or gRPC Protobuf `PushRequest` (`service_name` + user static labels + stream metadata). | Optional `X-Scope-OrgID` (`tenant_id`), `Bearer`, `Basic`; Snappy block compression for gRPC Proto (`wire.rs`). | Thread-local pre-baked stream JSON / Proto envelopes. |
| **`s3` (`format.rs`, `sigv4.rs`, `wire.rs`)** | AWS S3 HTTP/1.1 `PUT /{prefix}/year=YYYY/month=MM/day=DD/hour=HH/velda-{ts}-{seq}.jsonl[.zst\|.gz]`. | Full AWS SigV4 (`ring::digest::SHA256` + `ring::hmac::HMAC_SHA256`); caches daily `kSigning` key (`YYYYMMDD`) so same-day uploads run only 1 final `HMAC_SHA256(StringToSign)`. | Supports `zstd`, `gzip`, or raw `none`; path-style (`force_path_style`) or virtual-hosted-style addressing. |
| **`kafka` (`format/`, `protocol.rs`, `compress.rs`, `wire.rs`)** | Raw Kafka Wire Binary `ApiKey::Produce (0)` + `Metadata (3)` framing **RecordBatch v2** (`CRC32C`, base/max timestamps, zigzag varints). | `Zstd` (`0x04`), `Gzip` (`0x01`), or `None` (`0x00`); `SASL/PLAIN`; `RoundRobin`, `RouteId`, or `PeerIp` partitioning; `acks = 0 \| 1 \| -1`. | Records formatted as `Binary` (`RowBinary`-style compact tuples), `Json`, or `Text`, compressed cleanly via `zstd::bulk::compress(..., 1)`. |
| **`otlp` (`json/`, `proto/`, `wire.rs`)** | OTLP Logs HTTP/1.1 `POST /v1/logs` (`application/x-protobuf` \| `application/json`) or H2 gRPC `LogsService/Export`. | Custom OTLP headers & HTTP/2 `TE: trailers` pre-baked once via `bake_wire_template`. | Direct binary protobuf `ExportLogsServiceRequest` emitter bypassing intermediate `prost` trees. |

## 4. Fault Isolation & Circuit-Breaker Loop (`plugin.rs`)

When `targets.len() > 1`, `LogExporterPlugin::run` spawns each sink inside an isolated `tokio::task::JoinSet` task (`run_target_loop`):
1. **Per-Target Cadence & Queue**: Each target holds an independent `TelemetrySubscriber` (`max_batches.max(256)`) and `tokio::time::interval(flush_interval_ms)`.
2. **Independent Backoff / 30s Cool-Down**: Failure on one target triggers exponential sleep (`100ms << (attempt - 1)` capped at `2,000ms` for up to `10` attempts) followed by a `30s` cool-down loop that executes `subscriber.drain_batches(max_batches)` and drops batches to protect `TelemetryBatchPool`, leaving every healthy sibling task exporting continuously at full fidelity.
