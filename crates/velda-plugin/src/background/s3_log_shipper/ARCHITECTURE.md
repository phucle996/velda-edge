# Deep Architecture: `s3_log_shipper`

---

## 1. Module Ownership

```text
  config.rs       ──► S3LogShipperConfig, S3Compression (Zstd | Gzip | None)
  validation.rs   ──► validate_s3_log_shipper_config() (etp_id, no "..", bounds)
  format.rs       ──► format_ndjson() (pointer-stamped), compress_object()
  sigv4.rs        ──► SigV4Signer (cached daily HMAC-SHA256 signing key), UTC calendar math
  wire.rs         ──► S3WireState, upload_batch() (URI, SHA-256, SigV4, HTTP/1.1 PUT via S3Lease)
  plugin.rs       ──► S3LogShipperPlugin (BackgroundPlugin, select loop, 30s RAM-guard cool-down)
```

---

## 2. In-Place Connection Leasing (`S3Etp` & `S3Lease`)

`S3WireState` holds `Arc<S3Etp>` and borrows an exclusive `S3Lease` (`24` bytes, `DerefMut<Target = EtpStream>`) directly from `EtpPageArena` per upload:
- On transient `EtpError::Io` on attempt `0` (`kind != InvalidData`), `lease.reconnect().await` replaces `Slot[idx]` in-place and retries the signed `PUT` immediately.
- On `>= 500`, `Connection: close`, unframed responses, or timeouts, `lease.recreate_in_background()` marks the slot broken so `Drop` triggers asynchronous replenishment without blocking the next tick.

---

## 3. Zero-Allocation URI, SigV4 Signing & Reusable Buffers

`S3WireState` pre-allocates `uri_buf` (128 B), `head_buf` (1 KiB), `compress_buf` (128 KiB), and `resp_buf` (2 KiB) once at task startup:
1. **Hive Key Formatting**: `unix_secs_to_utc_ymdhms()` decomposes `SystemTime::now()` without `chrono`; key components are written into `uri_buf` with `fast_write_u64` and `2`-digit lookup tables.
2. **Payload Digest & Daily Signing Key Cache**: Computes `ring::digest::SHA256` over `wire_bytes`, formats lowercase hex via `write_hex_lower()`, and derives the AWS SigV4 signature using `SigV4Signer`'s cached `YYYYMMDD` signing key (re-deriving the HMAC chain only when the UTC date rolls over).
3. **Optional STS Support**: When `session_token` is configured on `S3EtpConfig`, `x-amz-security-token` is included in both signed headers and canonical headers.
4. **Bounded Response Drain**: Reads HTTP/1.1 response headers with `httparse::Response` (capped at `32 KiB`) and drains `chunked` / `Content-Length` bodies in `8192`-byte stack chunks so keep-alive sockets stay aligned.
