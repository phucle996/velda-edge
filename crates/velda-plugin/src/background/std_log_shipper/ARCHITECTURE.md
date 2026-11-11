# Architectural Specification: Standard Stream Log Shipper Plugin (`std_log_shipper`)

This document details the architectural invariants, threading model, memory layout, and micro-architectural mechanics of the refactored `std_log_shipper` background logging plugin in the **Velda Edge Data Plane**.

---

## 1. System Context & The Hourglass Architecture

In accordance with the Velda Edge Hourglass Architecture, request serving and operational telemetry are strictly decoupled:
- **Top Cone (Downstream Ingress)**: Handles connection termination, protocol framing, and TLS handshakes.
- **Waist (Router)**: Executes razor-thin, zero-allocation route selection.
- **Bottom Cone (Upstream Forwarding)**: Dispatches requests over pre-warmed connection pools.
- **Shadow Logistics Layer**: Operates completely outside the request-serving hot path under Linux `SCHED_IDLE` scheduling priority.

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
│                                                                                        │
│   FRONTLINE TRAFFIC WORKERS (Core 0..N, SCHED_OTHER)                                  │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Client Request -> Ingress Decode -> Route Match -> Upstream Pipe -> Response   │   │
│   │                                                                                │   │
│   │ [Hot-Path Telemetry Hook]                                                      │   │
│   │ AccessLogRecord::from_scalars() -> 64-byte scalar struct (1 cache line)        │   │
│   │ Core-Local SPSC Ring Push (< 70 CPU clock cycles, O(1), zero locks, zero alloc)│   │
│   └───────────────────────────────────────┬────────────────────────────────────────┘   │
│                                           │ Atomic In-Memory Hand-off                  │
│                                           ▼                                            │
│   SHADOW LOGISTICS WORKER (velda-shadow-0, SCHED_IDLE)                                 │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ LogStore::flush_all_lanes() -> Batched Arc<[AccessLogRecord]> Slices           │   │
│   │                                                                                │   │
│   │ StdLogShipperPlugin::run() (< 150 lines coordinator)                           │   │
│   │  ├─ StdLogSink::init() -> Persistent File / Stdout / Stderr handle (O(1) open) │   │
│   │  ├─ format_std_log_payload()                                                   │   │
│   │  │   ├─ Splicing pre-baked static JSON/Text frame slices                       │   │
│   │  │   ├─ Division-free 2-digit ASCII Lookup Table (LUT) integer conversion      │   │
│   │  │   ├─ 128-bit RequestId SSSE3 PSHUFB SIMD 32-char hex stamping               │   │
│   │  │   ├─ Zero-allocation RFC 5952 IPv6 branchless zero-compression              │   │
│   │  │   └─ Pre-reserved buffer capacity (zero heap reallocations)                 │   │
│   │  └─ sink.write_all(&out_buf).await -> Direct VFS write (Zero open/close churn) │   │
│   │                                                                                │   │
│   │ Automatic Memory Reclamation when Arc strong count drops to zero               │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Subsystem Layout & Clean Separation of Concerns

The subsystem is modularized into discrete, bounded units:

```text
crates/velda-plugin/src/background/std_log_shipper/
├── lut.rs                  # Hardware-optimized division-free integer, SIMD 32-char hex & RFC 5952 IP LUT
├── config.rs               # Declarative StdLogShipperConfig & TargetStream (Stdout, Stderr, File)
├── sink.rs                 # Persistent StdLogSink holding open tokio::fs::File descriptor across cycles
├── plugin.rs               # Concise shadow worker coordinator (< 150 lines)
├── mod.rs                  # Public API root re-exports
│
└── format/                 # Formatting engine
    ├── mod.rs              # Re-export format_std_log_payload
    ├── text.rs             # Extended Text & NCSA Common Log Format (CLF) formatting
    └── json.rs             # High-speed NDJSON access log formatting
```

---

## 3. Persistent File Sink Mechanics (`sink.rs`)

### 3.1 Elimination of Per-Cycle File Descriptor Churn
Prior implementations incurred high OS overhead by repeatedly invoking `OpenOptions::open()` on every periodic flush (every 50–100ms), creating hundreds of redundant `open()`, `stat()`, and `close()` syscalls per minute:

```text
OLD (Defective):
  Every 100ms: OpenOptions::open(path).await ──► Write ──► Drop File (close syscall)

NEW (StdLogSink):
  Startup:     StdLogSink::init(&target).await (Single O(1) open syscall)
  Serving:     File descriptor kept persistently open across all flush iterations
  Every 100ms: sink.write_all(&out_buf).await ──► Single write syscall ──► sink.flush()
```

### 3.2 Polymorphic Stream Target Support
`StdLogSink` supports three asynchronous output modes without dynamic dispatch (`dyn`):
- `StdLogSink::Stdout(tokio::io::Stdout)`: Direct writes to process standard output.
- `StdLogSink::Stderr(tokio::io::Stderr)`: Direct writes to process standard error.
- `StdLogSink::File(tokio::fs::File)`: Direct asynchronous writes to a persistent filesystem descriptor.

---

## 4. Hardware-Accelerated Serialization Mechanics (`lut.rs`)

### 4.1 Division-Free Decimal Formatting (`fast_write_u64`)
Eliminates costly `DIV` / `IDIV` hardware division instructions (20–80 cycles) by using reciprocal multiplication and a 200-byte L1D-resident table (`DIGIT_PAIRS`):
```text
n >= 100 ──► (n % 100) * 2 ──► DIGIT_PAIRS[rem..rem+2] ──► temp[curr..curr+2]
```

### 4.2 128-Bit RequestId SSSE3 SIMD Hex Stamping (`fast_write_hex32`)
The frontline pipeline emits 128-bit request IDs reconstructed as `(timestamp_sec as u128) << 64 | request_id as u128`.
`std_log_shipper` formats this 128-bit value into 32 lowercase hex characters using the SSSE3 `_mm_shuffle_epi8` (PSHUFB) instruction:
```text
[ req_hi (8B) ] [ req_lo (8B) ] ──► PSHUFB Vector Shuffles ──► 32 ASCII Bytes (3-5 ns)
```

### 4.3 Zero-Heap-Allocation RFC 5952 IPv6 Compression (`fast_write_ipv6`)
Eliminates intermediate heap allocations (`v6.to_string()`) when logging IPv6 addresses:
1. Deconstructs the 128-bit integer into 8 big-endian 16-bit words.
2. Identifies the longest contiguous sequence of zeros ($\ge 2$) per RFC 5952.
3. Formats hex segments into a fixed 40-byte stack buffer (`[u8; 40]`).
4. Slices the result directly into the destination buffer with zero dynamic allocations.

---

## 5. Zero-Reallocation Formatting Pipeline (`format/`)

1. **Pre-Reserved Capacity**: `format_std_log_payload` scans batch record counts upfront ($O(\text{batches})$) and invokes `out.reserve(total_recs * 320)`, ensuring that subsequent chunk splicing and integer writes never trigger heap reallocation or memory relocation.
2. **Pre-Baked Static Frame Slices**: All JSON keys, punctuation, and structural framing bytes are pre-defined as `&'static [u8]` constants.
3. **Multi-Format Support**:
   - `"json"`: High-throughput NDJSON with 32-character SIMD hex request IDs.
   - `"text"`: Detailed multi-protocol operational format (HTTP, gRPC, Raw TCP/UDP).
   - `"clf"`: Canonical NCSA Common Log Format (`host - - [ts] "req" status bytes`).
