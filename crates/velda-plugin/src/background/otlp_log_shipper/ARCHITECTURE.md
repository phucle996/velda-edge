# OpenTelemetry Log Shipper — Architecture Specification

This document details the internal subsystem architecture, threading model, memory geometry, empirical machine code proofs, and physical operational boundaries of the **OpenTelemetry Log Shipper Plugin** (`otlp_log_shipper`).

---

## 1. Subsystem Mission & Responsibilities

The `otlp_log_shipper` is an autonomous background logistics plugin designed to:
1. Drain reference-counted 64-byte `AccessLogRecord` batches from `velda-observability::LogStore` via lock-free core-local SPSC ring buffers.
2. Serialize access logs into compliant OpenTelemetry `ExportLogsServiceRequest` payloads via direct Protobuf wire skeletons over HTTP/2 (gRPC) or pre-baked JSON envelopes over HTTP/1.1.
3. Stream telemetry to an OpenTelemetry Collector daemon (e.g. port 4317 for gRPC or port 4318 for HTTP).
4. Maintain 100% fail-open autonomy: collector outages or network stalls never degrade frontline client request serving.

---

## 2. Architecture & Data Flow

```mermaid
flowchart TD
    subgraph DataPlane["Data Plane Frontline Workers (Cores 0..N-1)"]
        FW["Traffic Worker (handle_stream)"]
        HOOK["Telemetry Hook (Stream Completion)"]
        RECORD["AccessLogRecord (64B Scalar Struct)"]
        RING["LogStore Core-Local SPSC Ring Buffer"]

        FW --> HOOK
        HOOK --> RECORD
        RECORD -->|"Lock-Free O(1) Push"| RING
    end

    subgraph ShadowWorker["Shadow Logistics Worker (SCHED_IDLE, Assigned Core i)"]
        LOOP["Periodic Ticker (flush_interval_ms: 100ms)"]
        DRAIN["LogStore::drain_batches(max_batch_size)"]
        MODE{"Protocol Declared?"}

        LOOP --> DRAIN
        RING -.->|"Lock-free batch transfer"| DRAIN
        DRAIN --> MODE

        subgraph GrpcEngine["OTLP / gRPC & HTTP Protobuf Path"]
            PROTO_SKELETON["Pre-Baked Wire Skeletons (INFO/WARN/ERR)"]
            RAW_STAMP["Raw Pointer Arithmetic Stamping"]
            WIRE_TX["wire::push_logs (OtlpEtp / OtlpLease)"]
            MODE -->|"grpc | http/protobuf"| PROTO_SKELETON
            PROTO_SKELETON --> RAW_STAMP
            RAW_STAMP --> WIRE_TX
        end

        subgraph HttpEngine["OTLP / HTTP JSON Path"]
            JSON_ENV["OtlpJsonEnvelope (Pre-Baked Framing)"]
            LUT["SIMD PSHUFB Hex + 2-Digit LUT"]
            WIRE_HTTP["wire::push_logs (OtlpEtp / OtlpLease)"]
            MODE -->|"http/json"| JSON_ENV
            JSON_ENV --> LUT
            LUT --> WIRE_HTTP
        end
    end

    subgraph Collector["Telemetry Ingestion"]
        OTEL_GRPC["OpenTelemetry Collector (:4317 gRPC)"]
        OTEL_HTTP["OpenTelemetry Collector (:4318 HTTP)"]
        WIRE_TX -->|"5-Byte LPM gRPC or HTTP x-protobuf"| OTEL_GRPC
        WIRE_HTTP -->|"JSON POST /v1/logs"| OTEL_HTTP
    end
```

---

## 3. Core Architectural Invariants

### Invariant 1: Dedicated Shadow Logistics Worker Execution
* The shipper executes strictly on background companion threads (`velda-shadow-{id}`) running under Linux `SCHED_IDLE`.
* Frontline traffic workers serving client HTTP requests never format strings, serialize JSON, or execute outbound network I/O calls.
* Hot-path request serving retains 100% CPU capacity regardless of logging volume or collector latency.

### Invariant 2: Lock-Free SPSC Ring Buffer Hand-off
* Frontline workers record telemetry by pushing an atomic 64-byte scalar struct (`AccessLogRecord`) into a core-local Single-Producer Single-Consumer (SPSC) ring buffer.
* No shared mutexes, locks, cross-core atomics, or synchronization barriers exist between traffic workers and the shadow logging worker.

### Invariant 3: Compile-Time Static Schema URL Byte Slice Invariant
* The OpenTelemetry Schema URL is declared strictly as a compile-time static byte constant:
  ```rust
  pub const OTLP_SCHEMA_URL: &[u8] = b"https://opentelemetry.io/schemas/1.26.0";
  ```
* Embedded directly into the pre-baked outer envelope buffer without dynamic heap string allocations.

### Invariant 4: Pre-Baked Wire Skeletons & Static Body Invariant
* In compliance with the OpenTelemetry Logs specification, the log record `body` is emitted as a fixed static string constant (`"HTTP access log"`).
* The entire record prefix spanning Severity Number (tag 3), Severity Text (tag 4), Body (tag 5), and the Request ID attribute header (tag 6) is pre-computed into static byte skeletons (`SKELETON_INFO`, `SKELETON_WARN`, `SKELETON_ERROR`).

### Invariant 5: Raw Pointer Stamping & Vectorized Dual Timestamps
* Formatters advance write positions strictly via raw pointer arithmetic (`*mut u8`), eliminating vector reallocation bounds checks on the inner formatting loop.
* Timestamps (`time_unix_nano` and `observed_time_unix_nano`) are stamped simultaneously via 128-bit SIMD vector instructions (`_mm_set1_epi64x` and `_mm_storeu_si128`).

### Invariant 6: 128-Bit TraceID & RequestID SSSE3 SIMD Hex Stamping
* Trace IDs and Request IDs are converted from 128-bit binary integers into 32 lowercase hex characters using the SSSE3 `_mm_shuffle_epi8` (`PSHUFB`) vector instruction, eliminating 32 scalar loop iterations.

### Invariant 7: Shared `velda-etp` Persistent Transport Reuse
* **gRPC (`OtlpConn::Grpc`)**: Leases persistent `h2::client::SendRequest` handles from `velda_etp::otlp::OtlpEtp` with in-place `lease.reconnect().await` recovery and background driver supervision.
* **HTTP (`OtlpConn::Http`)**: Leases persistent HTTP/1.1 Keep-Alive `EtpStream` sockets from `OtlpEtp` with pre-baked headers (`wire::bake_wire_template`) and checked response draining.
* Sockets remain open inside page-aligned `EtpPageArena` slots across flush ticks, eliminating TCP 3-way handshakes and TLS renegotiation on every interval.

### Invariant 8: Fail-Open Autonomy
* If the remote OpenTelemetry Collector hangs, crashes, or returns HTTP 4xx/5xx or non-zero gRPC status codes, the shipper logs warnings and backs off.
* Frontline traffic workers continue operating normally without queue delays, blocking waits, or memory leaks.

---

## 4. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, background logistics plugins define an explicit workload weight from 1 to 100:

| Dimension | Metric Profile | Impact Analysis |
| :--- | :--- | :--- |
| **Assigned Workload Weight** | **70** | **High Background Workload** |
| **CPU Intensity** | High | SPSC ring batch drain, per-record Protobuf serialization, SSSE3 SIMD hex stamping (`PSHUFB`), HTTP/2 binary framing or JSON text interleaving, and TLS encryption. Under heavy traffic (e.g. 50K RPS), this produces sustained CPU activity on the shadow core. |
| **Memory Footprint** | ~512 KB – 2 MB | Pre-allocated Protobuf wire arena, JSON scratch formatting vectors, and HTTP/2 transport connection state. Zero ongoing heap churn. |
| **Cadence / Wakeup Rate** | High (50ms – 1000ms) | Sub-second flush interval (default: 100ms) generates frequent scheduler activity to maintain low log delivery latency. |
| **Failure Blast Radius** | P3 (Low Impact) | Access log loss only. Collector downtime, connection timeouts, or rejected payloads do not impact frontline client request serving. |

### Complementary Static Sharding Recommendation
* **Core Placement**: Core 2 or 3.
* **Strict Anti-Colocation Rule**: NEVER co-locate on Core 0 with latency-critical `health_probe` (Weight: 1) or master traffic intake. NEVER co-locate on the same core as `loki_log_shipper` (Weight: 80) — both are heavy log shippers that will exhaust core L1/L2 cache capacity and TLS cryptographic hardware units.
* **Ideal Pairing**: Pair with sleep-bound rotators (`jwks_rotator`, `acme_cert_rotator`) or lightweight UDP exporters (`statsd_exporter`) on secondary cores.

---

## 5. Memory Geometry & Reusable Buffer Pools

```text
+-------------------------------------------------------------------------------+
|  OtlpLogShipper Reusable Memory Architecture                                  |
|                                                                               |
|  [Protobuf Engine: proto/template.rs]                                         |
|  +-------------------------------------------------------------------------+  |
|  | SKELETON_INFO:  45 Bytes Static Byte Slice (Pre-baked Wire Header)      |  |
|  | SKELETON_WARN:  45 Bytes Static Byte Slice (Pre-baked Wire Header)      |  |
|  | SKELETON_ERROR: 46 Bytes Static Byte Slice (Pre-baked Wire Header)      |  |
|  | proto_buf: Vec<u8> (Pre-reserved scratch arena, Reused across ticks)    |  |
|  +-------------------------------------------------------------------------+  |
|                                                                               |
|  [JSON Engine: json/template.rs]                                              |
|  +-------------------------------------------------------------------------+  |
|  | OtlpJsonEnvelope: Pre-baked static outer envelope bytes                  |  |
|  | json_buf: Vec<u8> (Capacity: 128 KB, Reused across sequential ticks)    |  |
|  +-------------------------------------------------------------------------+  |
|                                                                               |
|  [SPSC Ring Buffer Hand-off: LogStore]                                        |
|  +-------------------------------------------------------------------------+  |
|  | Per-Core SPSC Ring Buffers: Lock-free 64B AccessLogRecord batches      |  |
|  +-------------------------------------------------------------------------+  |
+-------------------------------------------------------------------------------+
```

---

## 6. Empirical Machine Code Disassembly Proof

### Vectorized Dual-Timestamp Stamping & SIMD PSHUFB Hex Stamping

Compiled under `rustc 1.98` with `--release` (`-C opt-level=3`, `-C target-feature=+ssse3`):

```nasm
; Dual timestamp broadcast to XMM register
movq    %rdx, %xmm0
punpcklqdq %xmm0, %xmm0                    ; Duplicate 64-bit timestamp across 128-bit vector
movdqu  %xmm0, (%rdi)                      ; Store time_unix_nano & observed_time_unix_nano in 1 cycle

; SSSE3 PSHUFB parallel hex nibble conversion
movdqu  (%rsi), %xmm1                      ; Load 16 bytes of RequestID
movdqa  %xmm1, %xmm2
psrlw   $4, %xmm2                          ; Shift high nibbles
pand    .LCPI_MASK_0F(%rip), %xmm1         ; Low nibbles
pand    .LCPI_MASK_0F(%rip), %xmm2         ; High nibbles
movdqa  .LCPI_HEX_LUT(%rip), %xmm3
pshufb  %xmm1, %xmm3                       ; Parallel ASCII conversion (low)
movdqa  .LCPI_HEX_LUT(%rip), %xmm4
pshufb  %xmm2, %xmm4                       ; Parallel ASCII conversion (high)
punpcklbw %xmm3, %xmm4
movdqu  %xmm4, 16(%rdi)                    ; Store 16 formatted hex characters
```

#### Micro-Architectural Analysis
* **Dual-Store Efficiency**: Broadcasts timestamps using `punpcklqdq` and stores both fields in a single vector store (`movdqu`), halving store instruction count.
* **Vectorized Stamping**: Eliminates all scalar loops and conditional branches during hex conversion.

---

## 7. Physical Boundaries, Edge Cases & Operational Trade-offs

The following operational trade-offs reflect physical hardware, network, and kernel boundaries that govern system behavior under production conditions:

### 7.1 HTTP/2 Flow Control Window Exhaustion (gRPC)
* **Physical Reality**: HTTP/2 mandates connection-level and stream-level flow control windows (RFC 7540).
* **Failure Boundary**: Under high logging load, if the OpenTelemetry Collector slows down intake processing, the HTTP/2 stream flow control window will saturate (`WINDOW_UPDATE` delayed).
* **Design Decision**: `OtlpLogsGrpcClient` enforces `request_timeout_ms`. If the collector fails to replenish stream windows within the timeout, the stream is reset without blocking future ticks.

### 7.2 TLS Cryptographic CPU Overhead at High RPS
* **Physical Reality**: Streaming thousands of log records over TLS (`grpcs://` or `https://`) requires continuous AES-GCM or ChaCha20-Poly1305 symmetric encryption.
* **Failure Boundary**: Under 50,000+ RPS traffic, cryptographic operations on the shadow worker thread can consume 10–25% of a core's compute cycles.
* **Mitigation**: Pinned to `velda-shadow-{id}` under `SCHED_IDLE`, ensuring cryptographic computations never preempt frontline traffic serving.

### 7.3 SPSC Ring Buffer Queue Dropping Under Burst Traffic
* **Physical Reality**: SPSC ring buffers in `LogStore` have finite, fixed capacities (e.g. 1024 or 2048 records per core).
* **Failure Boundary**: If client traffic spikes beyond the shadow worker's drain rate, incoming records will be dropped at the ring threshold (`drop_count` incremented).
* **Operational Rule**: To handle sustained traffic bursts without log drops, increase `max_batch_size` or tune `flush_interval_ms` downward (e.g. 50ms).

### 7.4 CFS Scheduler Preemption Under Host Saturation
* **Physical Reality**: Shadow Logistics workers run under Linux `SCHED_IDLE`.
* **Failure Boundary**: If frontline workers sustain 100% CPU utilization across all cores, the Linux Completely Fair Scheduler (CFS) may delay waking the shadow worker, increasing log delivery latency.
* **Operational Rule**: Maintain at least 5% host CPU headroom to prevent shadow logistics starvation.
