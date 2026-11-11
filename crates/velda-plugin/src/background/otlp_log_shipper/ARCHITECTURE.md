# Architectural Specification: OpenTelemetry Log Shipper (`otlp_log_shipper`)

This document details the architectural invariants, threading model, framing mechanisms, and network socket management of the `otlp_log_shipper` background logging plugin in the **Velda Edge Data Plane**.

---

## 1. Subsystem Architecture & Dual-Transport Routing

The `otlp_log_shipper` operates strictly on companion thread `velda-shadow-0` under Linux `SCHED_IDLE`. It provides native dual-transport support with decoupled subsystems:

```
crates/velda-plugin/src/background/otlp_log_shipper/
├── lut.rs                 # Division-free integer LUT, SIMD hex32, zero-alloc RFC 4291/5952 IP
├── json/
│   ├── client.rs          # HTTP/1.1 persistent TCP/TLS Keep-Alive socket (Port 4318)
│   ├── template.rs        # Pre-baked JSON envelope, static schema URL, LUT record formatter
│   └── mod.rs
├── proto/
│   ├── client.rs          # HTTP/2 multiplexed gRPC client (Port 4317)
│   ├── template.rs        # Direct Protobuf wire encoder, SIMD timestamp & 32-hex stamping
│   ├── model.rs           # Standard Prost models for startup pre-baking & decode testing
│   └── mod.rs
├── plugin.rs              # Shadow worker lifecycle & LogStore draining loop
├── config.rs              # 100% explicit configuration
└── mod.rs                 # Public subsystem exports
```

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
│                                                                                        │
│   FRONTLINE TRAFFIC WORKERS (Core 0..N, SCHED_OTHER)                                  │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Client Ingress -> Route Matching -> Upstream Proxying -> Client Response       │   │
│   │                                                                                │   │
│   │ [Hot-Path Telemetry Hook]                                                      │   │
│   │ 64-byte scalar AccessLogRecord -> Core-Local SPSC Ring (< 70 CPU clock cycles) │   │
│   └───────────────────────────────────────┬────────────────────────────────────────┘   │
│                                           │ Atomic In-Memory Hand-off                  │
│                                           ▼                                            │
│   SHADOW LOGISTICS WORKER (velda-shadow-0, SCHED_IDLE)                                 │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ LogStore::flush_all_lanes() -> Batched Arc<[AccessLogRecord]> Slices           │   │
│   │                                                                                │   │
│   │ OtlpLogShipperPlugin::run()                                                    │   │
│   │  ├─ Transport Subsystem Branching:                                             │   │
│   │  │   ├─ [json::] HTTP/1.1 JSON Path (json::OtlpLogClient)                      │   │
│   │  │   │   ├─ OtlpJsonEnvelope pre-baked prefix / suffix framing                 │   │
│   │  │   │   ├─ OTLP_SCHEMA_URL byte constant embedded without heap allocation      │   │
│   │  │   │   ├─ lut::fast_write_hex32 (exact 32-hex character RequestId)          │   │
│   │  │   │   ├─ lut::fast_write_ip (RFC 4291/5952 zero-alloc IP formatter)         │   │
│   │  │   │   └─ Persistent HTTP/1.1 TCP stream write with Keep-Alive               │   │
│   │  │   │                                                                         │   │
│   │  │   └─ [proto::] gRPC HTTP/2 Path (proto::OtlpLogsGrpcClient)                 │   │
│   │  │       ├─ proto::OtlpProtoWireEnvelope::format_records_wire()                │   │
│   │  │       ├─ Direct Protobuf wire encoding (Zero Prost AST heap allocation)     │   │
│   │  │       ├─ SIMD 128-bit dual-timestamp broadcast (_mm_set1_epi64x)           │   │
│   │  │       ├─ 5-byte canonical gRPC binary framing [0x00, len_u32_be]            │   │
│   │  │       └─ Persistent HTTP/2 multiplexed stream transmission                  │   │
│   │  │                                                                             │   │
│   │  └─ Automatic Reference-Counted Batch Reclamation on Drop                      │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Compile-Time Static Schema URL Byte Slice Invariant

Standard OpenTelemetry logging exporters store `schema_url` as dynamic `String` heap objects on `ResourceLogs` and `ScopeLogs`.

In `otlp_log_shipper`:
1. The OpenTelemetry Schema URL is declared strictly as a compile-time static byte constant:
   ```rust
   pub const OTLP_SCHEMA_URL: &[u8] = b"https://opentelemetry.io/schemas/1.26.0";
   ```
2. In `json/template.rs`: Embedded directly into the pre-baked outer envelope byte buffer (`OtlpJsonEnvelope`).
3. In `proto/template.rs`: Serialized directly into the wire framing with tag 3 (`0x1A`), length varint, and raw byte slice.
4. **Outcome**: Zero runtime UTF-8 validation, zero string allocations, zero pointer indirections.

---

## 3. Pre-Baked Wire Skeletons & Static Body Invariant

Standard logging architectures waste hundreds of CPU clock cycles formatting a redundant dynamic string into the `body` field (e.g., `"request_id=... method=... status=... dur_us=... peer=..."`). In modern structured logging, all 14 execution context metrics are already serialized as authoritative structured attributes under `attributes` (`KeyValue` pairs).

In `otlp_log_shipper`:
1. **Static Body Constant**: In compliance with the OpenTelemetry Logs specification, the log record `body` is emitted as a fixed static string constant:
   ```rust
   // JSON:
   \"body\":{\"stringValue\":\"HTTP access log\"},
   // Protobuf (tag 5, length 17, string value tag 1, length 15):
   b"\x2a\x11\x0a\x0fHTTP access log"
   ```
2. **Fused Wire Skeletons (`proto/template.rs`)**:
   Because `body` is static and severity mappings are finite (`INFO`, `WARN`, `ERROR`), the entire prefix spanning Severity Number (tag 3), Severity Text (tag 4), Body (tag 5), and the Request ID attribute header (tag 6) is pre-computed into static byte skeletons:
   - `SKELETON_INFO`: 45 bytes
   - `SKELETON_WARN`: 45 bytes
   - `SKELETON_ERROR`: 46 bytes

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                        PRE-BAKED OTLP PROTO WIRE SKELETON & RAW POINTER STAMPING                                       │
│                                                                                                                        │
│  LogRecord Wire Layout:                                                                                                │
│  ┌───────────────────────┬──────────────────────┬──────────────────────┬────────────────────────────────────────────┐  │
│  │ Dual Timestamps       │ Pre-Baked Skeleton   │ Request ID Hex32     │ 13 Structured Attributes                   │  │
│  │ [Tag 1 & 2: 20 bytes] │ [Tags 3,4,5,6: 45 B] │ [Attribute: 32 bytes]│ [Tag 6 KeyValues: ~220 bytes]              │  │
│  │                       │                      │                      │                                            │  │
│  │ • time_unix_nano      │ • Severity Number    │ • SSSE3 PSHUFB SIMD  │ • Pre-baked attribute key/val prefixes     │  │
│  │ • observed_time_nano  │ • Severity Text      │ • 32 lowercase hex   │ • Division-free varint stamping            │  │
│  │ ── SIMD 128-bit store │ • Static body:       │   written in < 5 cyc │ • Zero-alloc stack IP stamping             │  │
│  │   _mm_set1_epi64x     │   "HTTP access log"  │                      │ • Sequential raw pointer advance           │  │
│  │   _mm_storeu_si128    │ • request_id prefix  │                      │   (*mut u8 stamping, 0 Vec::push calls)    │  │
│  └───────────────────────┴──────────────────────┴──────────────────────┴────────────────────────────────────────────┘  │
│  ▲                                                                                                                     │
│  └─ Pre-allocated `out` buffer slice -> Write position advanced strictly via pointer arithmetic -> set_len() at end    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

3. **Fused JSON Chunks (`json/template.rs`)**:
   Similarly, JSON formats severity number, severity text, static `"body":{"stringValue":"HTTP access log"}`, and `"attributes":[{"key":"request_id","value":{"stringValue":"` in a single contiguous static slice (`JSON_SEV_INFO_BODY_REQ`, `JSON_SEV_WARN_BODY_REQ`, `JSON_SEV_ERROR_BODY_REQ`), eliminating dynamic concatenation entirely.

---

## 4. Authoritative 128-Bit Request ID & Log Isolation Invariant

- **Log Correlation vs Trace Boundaries**: In strict adherence to observability boundaries, this plugin exports access logs, **not** distributed traces. It does not spoof or overwrite `trace_id` or `span_id`.
- **128-Bit Representation**: The full `u128` identifier from `RequestState` is reconstituted from `AccessLogRecord`:
  ```rust
  let req_hi = rec.timestamp_sec as u64;
  let req_lo = rec.request_id;
  ```
- **Exact 32-Character Lowercase Hex**:
  - `lut::fast_write_hex32`: Vectorized via SSSE3/AVX2 `_mm_shuffle_epi8` (PSHUFB) on `x86_64` to write 32 lowercase hex characters in under 5 clock cycles.
  - In Protobuf: Fixed attribute slot prefix inside the skeleton followed by exactly 32 bytes hex stamped directly via `format_hex32_bytes(ptr, req_hi, req_lo)`.

---

## 5. Hardware-Optimized LUT Engine & Raw Pointer Stamping

- **Dual-Timestamp SIMD Store**:
  On `x86_64`, both `time_unix_nano` and `observed_time_unix_nano` are initialized from the identical 64-bit nanosecond value. Using `_mm_set1_epi64x` and `_mm_storeu_si128`, 16 bytes of timestamp data are stored simultaneously in 1 CPU cycle.
- **Sequential Raw Pointer Stamping (`*mut u8`)**:
  Rather than calling `Vec::push` or `extend_from_slice` dozens of times per record (which triggers compiler bounds checks and capacity re-checks), the wire encoder pre-reserves space and advances an unsafe write pointer `ptr` sequentially using `ptr::copy_nonoverlapping`. The vector's length is updated exactly once at the end via `set_len`.
- **Division-Free Fast Varint Stamping (`stamp_int_attribute`)**:
  Formats unsigned integer attributes directly as compact protocol buffer varints without standard library formatting overhead.
- **Zero-Alloc IP Formatter (`format_ip_to_slice` & `stamp_ip_attribute`)**:
  - Checks RFC 4291 IPv4-mapped pattern via branchless bitwise shift: `(ip_u128 >> 48) == 0 && ((ip_u128 >> 32) & 0xFFFF) == 0xFFFF`.
  - IPv4 dotted-decimal formatting writes directly into a fixed 16-byte stack array without intermediate string objects.
  - Wire key-value framing and formatted IP bytes are stamped directly into the output pointer.

---

## 6. High-Performance Transport Clients

### 6.1 HTTP/1.1 Keep-Alive Client (`json::OtlpLogClient`)
- Reuses established `TcpStream` / `TlsStream` across successive export cycles.
- Detects collector-side idle connection teardown and transparently reconnects without failing the batch.
- Configures `TCP_NODELAY` immediately to eliminate Nagle buffering delays.

### 6.2 gRPC HTTP/2 Multiplexed Client (`proto::OtlpLogsGrpcClient`)
- Establishes persistent HTTP/2 session via the `h2` engine.
- Streams direct wire-encoded payloads across multiplexed streams to `/opentelemetry.proto.collector.logs.v1.LogsService/Export` on port 4317.
- Prepends canonical 5-byte gRPC framing: `[0x00, len_u32_be]`.

