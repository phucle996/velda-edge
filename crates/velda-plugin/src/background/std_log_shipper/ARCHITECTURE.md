# Architectural Specification: Standard Stream Log Shipper Plugin (`std_log_shipper`)

This document details the architectural invariants, threading model, memory layout, and machine-level mechanics of the `std_log_shipper` background logging plugin in the **Velda Edge Data Plane**.

---

## 1. System Context & The Hourglass Architecture

In accordance with the Velda Edge Hourglass Architecture, request serving and operational telemetry are strictly decoupled:
- **Top Cone (Downstream Ingress)**: Handles connection termination, protocol framing, and TLS handshakes.
- **Waist (Router)**: Executes razor-thin, zero-allocation route selection.
- **Bottom Cone (Upstream Forwarding)**: Dispatches requests over pre-warmed connection pools.
- **Shadow Logistics Layer**: Operates completely outside the request-serving hot path under Linux `SCHED_IDLE` scheduling priority.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
│                                                                                        │
│   FRONTLINE TRAFFIC WORKERS (Core 0..N, SCHED_OTHER)                                  │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Client Request -> Ingress Decode -> Route Match -> Upstream Pipe -> Response   │   │
│   │                                                                                │   │
│   │ [Hot-Path Telemetry Hook]                                                      │   │
│   │ AccessLogRecord::from_scalars() -> 64 bytes integer struct                     │   │
│   │ Core-Local SPSC Ring Push (< 70 CPU clock cycles, O(1), zero locks, zero alloc)│   │
│   └───────────────────────────────────────┬────────────────────────────────────────┘   │
│                                           │ Atomic In-Memory Hand-off                  │
│                                           ▼                                            │
│   SHADOW LOGISTICS WORKER (velda-shadow-0, SCHED_IDLE)                                 │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ LogStore::flush_all_lanes() -> Batched Arc<[AccessLogRecord]> Slices           │   │
│   │                                                                                │   │
│   │ StdLogShipperPlugin::run()                                                     │   │
│   │  ├─ format_std_log_payload()                                                   │   │
│   │  │   ├─ Splicing pre-baked static JSON/Text frame slices                       │   │
│   │  │   └─ Division-free 2-digit ASCII Lookup Table (LUT) integer conversion      │   │
│   │  └─ Asynchronous write to target stream (stdout / stderr / file)               │   │
│   │                                                                                │   │
│   │ Automatic Memory Reclamation when Arc strong count drops to zero               │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Lock-Free SPSC Ring Ingestion & Subscription

### 2.1 Frontline Ingestion Contract
Traffic workers record operational telemetry into an immutable, 64-byte scalar struct: [`velda_observability::AccessLogRecord`].
- Memory footprint: Exactly 64 bytes (1 cache line).
- Representation: Fully composed of primitive integers (`u64`, `u32`, `u16`, `u8`) with zero pointer indirection and zero string heap allocations.
- Ingestion mechanism: Each worker writes into its dedicated core-local Single-Producer Single-Consumer (SPSC) ring buffer.
- Latency profile: Less than 70 CPU clock cycles per record, completely free of locks, system calls, and atomic contention across CPU cores.

### 2.2 Batched Reference-Counted Draining
The shadow worker periodically executes `subscriber.drain_batches(max_batches)`:
- Chunks of log records are organized into shared slices wrapped in an atomic reference counter: `Arc<[AccessLogRecord]>`.
- Multiple active background exporters (such as `std_log_shipper`, `otlp_log_shipper`, and `loki_log_shipper`) consume the identical reference-counted batch concurrently.
- No exporter copies or duplicates the raw log records in memory. Memory is reclaimed automatically when the last exporter drops its `Arc` handle.

---

## 3. Division-Free 2-Digit ASCII Lookup Table (LUT) Serialization

Traditional log formatting libraries rely on standard library formatting macros (such as `write!`, `format!`), which introduce severe overhead:
1. Parsing format specifiers at runtime.
2. Multiple branching and repeated hardware division (`DIV` / `IDIV` instructions costing 20–80 cycles per operation).
3. Heap allocations and dynamic string resizing.

`std_log_shipper` eliminates this overhead entirely:

```rust
// Pre-computed 2-digit ASCII Lookup Table (200 bytes, fits into a single L1 data cache line)
const DIGIT_PAIRS: &[u8; 200] = b"\
    00010203040506070809\
    10111213141516171819\
    20212223242526272829\
    30313233343536373839\
    40414243444546474849\
    50515253545556575859\
    60616263646566676869\
    70717273747576777879\
    80818283848586878889\
    90919293949596979899";
```

### 3.1 Fast Integer Conversion (`fast_write_u64`)
- Single-digit integers (`n < 10`) are converted in 1 CPU instruction: `buf.push(b'0' + n as u8)`.
- Multi-digit integers are converted 2 digits at a time by extracting pairs via reciprocal multiplication and table indexing.
- Results are stored into a stack-allocated 20-byte buffer and spliced into the destination vector via vectorized `memcpy`.

### 3.2 Pre-Baked Frame Chunk Splicing
Static JSON keys and structure framing bytes are pre-defined as byte slices (`&'static [u8]`):
```rust
const JSON_K0: &[u8] = b"{\"req_id\":\"";
const JSON_K1: &[u8] = b"\",\"ts\":";
const JSON_K2: &[u8] = b",\"dur_us\":";
const JSON_K3: &[u8] = b",\"proto\":\"";
// ...
const JSON_END: &[u8] = b"\"}\n";
```
The serialization loop performs zero parsing. It sequentially appends static chunk slices and converted integer bytes into a reusable contiguous buffer (`Vec<u8>`).

---

## 4. Shadow Worker Threading & `SCHED_IDLE` Lifecycle

1. **CPU Pinning**: `std_log_shipper` runs exclusively on companion thread `velda-shadow-0`, pinned to CPU Core 0 alongside primary traffic worker `velda-worker-0`.
2. **Scheduling Priority**: Managed under Linux `SCHED_IDLE`. When request traffic surges on Core 0, the Linux Completely Fair Scheduler (CFS) immediately yields all CPU time to `velda-worker-0`.
3. **Clock Cycle Scavenging**: During inter-packet intervals or idle moments, the shadow worker activates to process queued records without degrading traffic throughput or tail latency.
4. **Graceful Shutdown**: Listens to the global `watch::Receiver<bool>` shutdown signal. Upon shutdown notification, any residual records in SPSC rings are flushed before thread termination.
