# Deep Architecture: `elasticsearch_log_shipper`

This document details the internal memory layout, formatting mechanics, connection leasing model, and protocol state machine of `elasticsearch_log_shipper`.

---

## 1. Subsystem Isolation & Module Ownership

`elasticsearch_log_shipper` is decomposed into four single-responsibility modules under `crates/velda-plugin/src/background/elasticsearch_log_shipper/`:

```text
  config.rs       ──► ElasticsearchLogShipperConfig, EsBulkOpType, EsCompression
  validation.rs   ──► validate_elasticsearch_log_shipper_config() (JSON + char invariants)
  format.rs       ──► prebake_bulk_action_line(), format_bulk_ndjson() (pointer stamping)
  wire.rs         ──► bake_bulk_prefix(), push_bulk_payload() (HTTP/1.1 + ETP lease)
  plugin.rs       ──► ElasticsearchLogShipperPlugin (BackgroundPlugin, select loop, cool-down)
```

Frontline traffic workers (`velda-worker-*`) only write fixed-size `TelemetryRecord` structs into `TelemetryStore`. All NDJSON serialization, optional `gzip` compression, and `_bulk` HTTP I/O execute asynchronously in the background task (`Direction::Background`, `pbww = 60`), never touching the L7 request hot path.

---

## 2. In-Place Connection Leasing (`ElasticsearchEtp` & `EtpPageArena`)

Instead of heap-allocating per-request `VecDeque` nodes or `Arc<Mutex<_>>` wrappers, `elasticsearch_log_shipper` borrows connections from `velda_etp::elasticsearch::ElasticsearchEtp`:

```text
  etp.acquire().await
        │
        ▼
  ElasticsearchLease (24 bytes: pool: Arc<ElasticsearchEtp>, slot_idx: usize, broken: bool)
        │  Deref / DerefMut -> &mut EtpStream directly at:
        │  &(pool.arena + (slot_idx << 6) + 64)
        │
        ├─► Io error on attempt 0 (kind != InvalidData):
        │     lease.reconnect().await -> connect_fresh() -> replace_leased_slot(slot_idx)
        │
        ├─► HTTP >= 500, Connection: close, unframed body, or Timeout:
        │     lease.recreate_in_background() -> sets broken = true
        │
        └─► Drop(ElasticsearchLease):
              ├─ !broken && idx < baseline  ──► L2 prefetch + ready_bitmap[idx] = 1
              ├─ !broken && idx >= baseline ──► auto-decay spare slot
              └─  broken                    ──► discard_broken_slot + trigger_replenish()
              └─  semaphore.add_permits(1)
```

`size_of::<ElasticsearchLease>() == 24` is enforced by a compile-time `const` assertion in `velda-etp/src/elasticsearch/lease.rs`.

---

## 3. Direct Pointer-Stamping NDJSON Formatter (`format.rs`)

### 3.1 Prebaked Action Line
At task startup (`plugin.rs`), `prebake_bulk_action_line(op_type, index)` constructs the immutable NDJSON action metadata line once:
- `EsBulkOpType::Create`: `{"create":{"_index":"<index>"}}\n`
- `EsBulkOpType::Index`: `{"index":{"_index":"<index>"}}\n`

### 3.2 Hot-Path Formatting & L2 Prefetch
`format_bulk_ndjson(batches, prebaked_action, out)`:
1. Pre-reserves `total_recs * (prebaked_action.len() + 384)` bytes in the reusable `payload_buf` (`Vec<u8>`, initial capacity 128 KiB).
2. On `x86_64`, issues `_MM_HINT_T1` L2 hardware prefetches 4 records ahead (`recs.get_unchecked(idx + 4)`) across 4 cache lines (`+0`, `+64`, `+128`, `+192`). On non-`x86_64` architectures (e.g. `aarch64`), the prefetch block is compiled out cleanly via `#[cfg(target_arch = "x86_64")]`.
3. Reserves `prebaked_action.len() + MAX_ROW_BYTES` (`MAX_ROW_BYTES = 512 + 184 * 6 = 1616` bytes) before each record so pointer arithmetic stays strictly within allocated capacity.
4. Copies static JSON keys and stamps numeric/IP fields using `velda_core::lut::decimal::{fast_write_u64_ptr, fast_write_u16_ptr}` and `velda_core::lut::ip::{fast_write_ip_ptr, fast_write_ipv4_ptr}` without `serde_json` or intermediate `String` allocations.
5. Escapes `rec.url()` bytes via `write_json_escaped_ptr`, handling `"`, `\`, and ASCII control bytes `0x00..=0x1F` (`\u00XX`).
6. Updates `out.set_len(ptr as usize - base_ptr as usize)` once per record.

---

## 4. HTTP/1.1 `_bulk` Wire Protocol & Streaming Error Scanner (`wire.rs`)

### 4.1 Prebaked Request Head
`bake_bulk_prefix(&etp.config, &self.config)` builds the immutable HTTP/1.1 header prefix once at startup:
```text
POST {base_path}/_bulk{?pipeline=...} HTTP/1.1\r\n
Host: {host_header}\r\n
Content-Type: application/x-ndjson\r\n
{Content-Encoding: gzip\r\n}
{Authorization: ApiKey|Basic|Bearer ...\r\n}
Connection: keep-alive\r\n
Content-Length: 
```
Per flush, `head_buf` clears, copies `prebaked_prefix`, stamps `wire_payload.len()` via `velda_core::lut::decimal::fast_write_u64`, and appends `\r\n\r\n`.

### 4.2 Bounded Response Parsing & Seam-Aware `"errors":true` Scan
- **Header Cap**: Reads into `resp_buf` until `httparse::Response` (`[httparse::EMPTY_HEADER; 64]`) completes; rejects headers exceeding `32 * 1024` bytes (`ErrorKind::InvalidData`).
- **Body Framing**:
  - `Transfer-Encoding: chunked`: Parses hex chunk sizes and trailers incrementally (rejecting chunk/trailer lines > `8192` bytes) and compacts `resp_buf` when `pos > 4096`.
  - `Content-Length: N`: Drains remaining bytes in `8192`-byte stack chunks.
  - Neither header present: Scans buffered bytes and marks `close_after = true` so the lease is recycled.
- **Seam-Aware Scanner**: `scan_bulk_errors` searches for the 13-byte needle `b"\"errors\":true"` without buffering the entire Elasticsearch response in RAM. A 12-byte `err_tail` array + 24-byte `seam` stack buffer catches matches split across arbitrary read boundaries.

---

## 5. Complexity, Memory Bounds & Safety Invariants

- **Formatting Complexity**: $O(R + B_{url})$ for $R$ records in drained batches, with zero per-record heap allocations once `payload_buf` reaches steady-state capacity.
- **Buffer Reuse**: `head_buf`, `payload_buf` (128 KiB initial), `compress_buf` (64 KiB initial), and `resp_buf` (1 KiB initial) are allocated once in `run()` and cleared/reused across every tick.
- **Bounded Memory Under Outage**: After `consecutive_failures > 10`, a 30-second cool-down drains and discards `TelemetryStore` batches so stalled Elasticsearch backends cannot cause unbounded queue growth.
- **Unsafe Pointer Safety**: All `unsafe` pointer writes in `format.rs` are preceded by `out.reserve(prebaked_action.len() + MAX_ROW_BYTES)`, where `MAX_ROW_BYTES` (`1616` bytes) bounds the fixed JSON keys, decimal/IP LUT outputs, and worst-case 6x `\u00XX` escaping of `rec.url()`. All slice accesses in `wire.rs` use checked `.get()` / `.get_mut()` / `.checked_add()`.
