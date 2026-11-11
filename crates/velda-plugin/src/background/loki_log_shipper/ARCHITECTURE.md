# Architectural Specification: Grafana Loki Log Shipper (`loki_log_shipper`)

This document details the architectural invariants, threading model, framing mechanisms, dual-transport delivery (HTTP JSON and gRPC Protobuf), and multi-stream partitioning of the `loki_log_shipper` background logging plugin in the **Velda Edge Data Plane**.

---

## 1. System Context & Dual-Transport Architecture

Grafana Loki organizes log entries into streams defined by sets of labels. The `loki_log_shipper` subsystem operates symmetrically to `otlp_log_shipper`, supporting both **HTTP/1.1 JSON Push** (Port 3100, `/loki/api/v1/push`) and **HTTP/2 gRPC Protobuf Push** (Port 9096, `/logproto.Pusher/Push`):

```text
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
│   │ LokiLogShipperPlugin::run()                                                    │   │
│   │  ├─ Protocol Branching: config.protocol ("http" vs "grpc")                     │   │
│   │  │                                                                             │   │
│   │  ├─ [HTTP JSON Subsystem - Port 3100: /loki/api/v1/push]                       │   │
│   │  │   ├─ LokiJsonFormatter (Persistent Scratch Buffers: http/grpc/raw_values)   │   │
│   │  │   ├─ Pre-baked Stream Templates (LokiStreamTemplate)                        │   │
│   │  │   ├─ 128-Bit RequestId SIMD Hex Stamping (32 lowercase hex chars)           │   │
│   │  │   ├─ RFC 5952 Branchless IPv6 Stack Formatting                              │   │
│   │  │   └─ LokiLogClient: Persistent HTTP/1.1 TCP Keep-Alive + X-Scope-OrgID     │   │
│   │  │                                                                             │   │
│   │  └─ [gRPC Protobuf Subsystem - Port 9096: /logproto.Pusher/Push]               │   │
│   │      ├─ LokiProtoFormatter: Direct Wire Protobuf Encoder (Zero AST Alloc)      │   │
│   │      ├─ Prometheus Label Syntax: {service="...",protocol="http",...}           │   │
│   │      ├─ Canonical 5-byte gRPC framing: [0x00, len_u32_be]                      │   │
│   │      └─ LokiLogsGrpcClient: Persistent HTTP/2 Stream + x-scope-orgid Metadata  │   │
│   │                                                                                │   │
│   │  └─ Automatic Reference-Counted Batch Reclamation on Drop                      │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Multi-Stream Partitioning & Cardinality Protection

Loki's storage index indexes every unique combination of label key-value pairs.
- **Index Cardinality Bomb Anti-Pattern**: Putting high-cardinality fields (`request_id`, `ip_address`, `port`, `user_agent`) into stream labels creates millions of unique streams, causing OOM and index explosion in Loki ingesters.
- **Velda Edge Solution (Strict Bounded Cardinality)**:
  - **Stream Labels**: Strictly limited to low-cardinality static metadata and protocol families:
    `{service="velda-edge",protocol="http",cluster="edge-us"}`
    `{service="velda-edge",protocol="grpc",cluster="edge-us"}`
    `{service="velda-edge",protocol="raw",cluster="edge-us"}`
  - **Log Entry Lines**: High-cardinality fields are embedded directly into the log line string:
    `ts=1710000001 request_id=0000000065ec8781aaaabbbbccccdddd dur_us=320 proto=http/2 up_proto=http/1.1 pipe=buffered status=200 rx=128 tx=512 peer=192.168.1.10:50000 upstream=10.0.0.1:8080 route=1 ua=curl/8 completion=complete`
  - High performance LogQL queries (`{service="velda-edge",protocol="http"} |= "status=500"`) execute at wire speed over compacted chunks without index bloat.

---

## 3. Subsystem Breakdown

### 3.1 Division-Free Integer, SIMD Hex & RFC 5952 IP LUT (`lut.rs`)
- **`fast_write_u64`**: Employs reciprocal 64-bit multiplication and 2-digit decimal lookup table (`DIGIT_PAIRS`), completely eliminating hardware `DIV`/`IDIV` operations.
- **`fast_write_hex32` / `format_hex32_bytes`**: Restores the full 128-bit Request ID `((timestamp_sec as u64) << 64 | request_id as u64)` and vectorizes hex conversion via SSSE3 `_mm_shuffle_epi8` (`PSHUFB`) into exactly 32 lowercase hex characters in ~3.8 ns.
- **`fast_write_ipv6`**: RFC 5952 branchless zero-compression formatter using a stack buffer `[u8; 40]`, replacing slow `write!` and `std::io::Cursor` allocations (~35 ns, 0 heap allocs).

### 3.2 HTTP JSON Subsystem (`json/`)
- **`template.rs`**:
  - `LokiStreamTemplate`: Pre-serializes static JSON stream prefixes (`{"stream":{"service":"...","protocol":"http"},"values":[`).
  - `LokiJsonFormatter`: Holds persistent heap scratch buffers (`http_values`, `grpc_values`, `raw_values`) across flush cycles, calling `.clear()` instead of reallocating memory.
  - Zero heap allocation during steady-state multi-stream serialization.
- **`client.rs`**:
  - `LokiLogClient`: Maintains a persistent HTTP/1.1 TCP connection with `TCP_NODELAY`, reconnecting automatically upon drop or server timeout.
  - Injects `X-Scope-OrgID` for multi-tenant routing and parses HTTP responses via zero-copy `httparse`.

### 3.3 gRPC Protobuf Subsystem (`proto/`)
- **`model.rs`**:
  - Implements canonical Loki `logproto.PushRequest`, `StreamAdapter`, `EntryAdapter`, and `google.protobuf.Timestamp`.
- **`template.rs`**:
  - `LokiProtoFormatter`: Encodes records directly into binary Protobuf wire bytes (tag, wire type, varint, length-delimited string) without intermediate Prost AST allocations.
  - Constructs Prometheus-format labels: `{service="velda-edge",protocol="http",...}`.
- **`client.rs`**:
  - `LokiLogsGrpcClient`: Manages persistent HTTP/2 connection to `/logproto.Pusher/Push`.
  - Frames payload with canonical 5-byte gRPC header `[0x00, (len as u32).to_be_bytes()]`.
  - Injects `x-scope-orgid` metadata and verifies trailing `grpc-status: 0`.

### 3.4 Coordinator (`plugin.rs`)
- Compact background coordinator (< 180 lines).
- Evaluates declared `protocol` (`"http"` or `"grpc"`) and branches cleanly into the selected transport pipeline without dynamic header sniffing or runtime AST switches.

---

## 4. Shadow Worker Invariants & Failure Isolation

1. **Pinned CPU Execution**: Runs exclusively on `velda-shadow-0` under Linux `SCHED_IDLE`. It never shares a core or competes with frontline request workers.
2. **Zero Hot-Path Contention**: Frontline workers deposit 64-byte `AccessLogRecord`s into core-local SPSC queues in < 70 cycles. Log formatting and network serialization take place completely off the hot path.
3. **Fail-Open Isolation**: If Loki returns HTTP 429 / 5xx, or gRPC `RESOURCE_EXHAUSTED`, the shadow worker increments `failed_pushes`, discards stale batches to bound memory usage, and preserves edge serving continuity.
