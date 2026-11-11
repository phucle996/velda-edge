# Standard Stream Log Shipper Plugin (`std_log_shipper`)

The `std_log_shipper` plugin is a high-throughput, zero-allocation background logging exporter for the **Velda Edge Data Plane**. It subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC, and Raw L4 TCP/UDP) and writes formatted JSON, text, or Common Log Format (CLF) streams directly to `stdout`, `stderr`, or persistent filesystem targets without imposing locks, memory allocations, or formatting overhead on frontline request-serving worker threads.

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin Identifier** | `std_log_shipper` |
| **Canonical Execution Phase** | `background` (Dedicated Shadow Logistics Worker) |
| **Execution Runtime** | Pinned to `velda-shadow-0` under Linux `SCHED_IDLE` scheduling priority |
| **Capability Declaration** | `PluginCapability::default()` (Non-request-serving background logistics service) |
| **Supported Protocols** | Ingests and formats records across HTTP/1.1, HTTP/2, HTTP/3, gRPC, Raw TCP, and Raw UDP |
| **Hot-Path Request Overhead** | **0 nanoseconds serving cost**; zero lock contention, zero string allocations, zero I/O on traffic cores |
| **Output Target Streams** | Standard Output (`stdout`), Standard Error (`stderr`), or persistent filesystem path (`File`) with open descriptor reuse |
| **Serialization Formats** | Structured JSON (`"json"`), Human-Readable Text (`"text"`), or Common Log Format (`"clf"`) |

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — STD LOG SHIPPER ARCHITECTURE                                   ║
║              (crates/velda-plugin/src/background/std_log_shipper: StdLogShipperPlugin)              ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS]                     [SHADOW LOGISTICS WORKER: velda-shadow-0]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Core 0, 100ms Ticker)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
        │                                                    │ (Every 100ms)
        ▼                                                    ▼
  ┌───────────┐  Zero-Lock SPSC Ring Drain  ┌──────────────────────────────────┐
  │ AccessLog │ ──────────────────────────► │  Drain Batches (max 1000 recs)   │
  │  Record   │    (< 2ns per record)       └────────────────┬─────────────────┘
  └───────────┘                                              │
                                              ┌──────────────┴──────────────┐
                                              │ Upfront Buffer Pre-reserve  │
                                              │ (O(1) scratch vector resize)│
                                              └──────────────┬──────────────┘
                                                             │
                                   ┌─────────────────────────┴─────────────────────────┐
                                   │                                                   │
                            [format: "json"]                                    [format: "text"]
                                   ▼                                                   ▼
                        ┌─────────────────────┐                             ┌─────────────────────┐
                        │   format/json.rs    │                             │   format/text.rs    │
                        │ Structured NDJSON   │                             │ Extended Text / CLF │
                        │ SIMD Itoa + PSHUFB  │                             │ SIMD Itoa + PSHUFB  │
                        │ (235 CPU cycles)    │                             │ (225 CPU cycles)    │
                        └──────────┬──────────┘                             └──────────┬──────────┘
                                   │                                                   │
                                   └─────────────────────────┬─────────────────────────┘
                                                             │
                                                             ▼
                                                    ┌─────────────────┐
                                                    │   sink/mod.rs   │
                                                    │   StdLogSink    │
                                                    │ Persistent FD   │
                                                    │ write_all()     │
                                                    └────────┬────────┘
                                                             │
                                    ┌────────────────────────┼────────────────────────┐
                                    ▼                        ▼                        ▼
                                [stdout]                  [stderr]                 [File]
                               (Console)                 (Console)             (/var/log/...)
```

---

## 2. Technical Documentation Index

| Document | Purpose & Scope |
| :--- | :--- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | In-depth architectural design, Hourglass Model integration, lock-free SPSC ring consumption, division-free 2-digit ASCII Lookup Table (LUT) integer serialization, SSSE3 SIMD byte stamping, persistent `StdLogSink`, and `SCHED_IDLE` lifecycle |
| [MACHINE_CODE_ANALYSIS.md](MACHINE_CODE_ANALYSIS.md) | Assembly inspection, SSSE3 PSHUFB SIMD vector instructions, memory alignment, hardware CPU cycle counts via `RDTSC` (315 cycles/rec JSON, 286 cycles/rec Text), and zero-allocation proofs |
| [BENCHMARK.md](BENCHMARK.md) | Standardized 4-tier benchmark specification form (Macro Serving, System Resources, Kernel Network & Syscalls, Hardware Performance Monitoring Counters) across five operational scenarios |
| [RUNBOOK.md](RUNBOOK.md) | Site Reliability Engineering (SRE) operational guide, severity triage matrix, Mermaid incident response decision tree, diagnostic command toolkit, and disk backpressure mitigation |
| [TEST.md](TEST.md) | Automated unit, integration, and machine code test catalog, test execution reproduction instructions, and boundary assertion invariants |

---

## 3. Strict 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration field must be explicitly declared in `plugins.json`. Velda Edge strictly rejects implicit fallbacks, magic defaults, or omitted properties. If any property is omitted or malformed, configuration validation fails fast at synchronization time.

| Field | Type | Required | Constraints / Allowed Values | Recommended Production Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `target` | string | **Yes** | `"stdout"`, `"stderr"`, or valid absolute file path | `"/var/log/velda/access.log"` or `"stdout"` | Output destination stream for formatted access log records. |
| `format` | string | **Yes** | `"json"`, `"text"`, or `"clf"` (case-insensitive) | `"json"` (Production SIEM)<br>`"text"` (Local Debug) | Serialization format applied to all drained batches. |
| `flush_interval_ms` | integer (u64) | **Yes** | `50 ..= 60000` (milliseconds) | `100` (100 milliseconds) | Periodic timer interval for waking up shadow worker to drain SPSC rings and flush buffered bytes. |
| `max_batch_size` | integer (usize) | **Yes** | `1 ..= 10000` (records) | `1000` | Maximum number of log records drained and processed in a single sequential flush cycle. |

---

## 4. Production Configuration Profiles

### Profile 1: High-Throughput Production Profile (Structured JSON via File Stream)
*Recommended for production edge nodes shipping logs to vector, fluentbit, or datadog agents via filesystem rotation.*

```json
{
  "id": "std_log_shipper",
  "phase": "background",
  "config": {
    "target": "/var/log/velda/access.log",
    "format": "json",
    "flush_interval_ms": 100,
    "max_batch_size": 1000
  }
}
```

### Profile 2: Containerized Cloud-Native Profile (Standard Output JSON)
*Recommended for Kubernetes container runtimes aggregating logs via container stdout.*

```json
{
  "id": "std_log_shipper",
  "phase": "background",
  "config": {
    "target": "stdout",
    "format": "json",
    "flush_interval_ms": 100,
    "max_batch_size": 1000
  }
}
```

### Profile 3: Development & Debug Profile (Human-Readable Text)
*Recommended for developer workstations and local sandbox validation.*

```json
{
  "id": "std_log_shipper",
  "phase": "background",
  "config": {
    "target": "stdout",
    "format": "text",
    "flush_interval_ms": 50,
    "max_batch_size": 100
  }
}
```

---

## 5. Architectural Invariants

1. **Protocol-Agnostic Unified Processing**: A single background instance formats all protocols identically, completely eliminating duplicated plugins per protocol.
2. **Zero Hot-Path Work**: Traffic worker threads strictly record an atomic 64-byte integer struct and push it into a core-local Single-Producer Single-Consumer (SPSC) ring buffer in less than 70 CPU clock cycles.
3. **Division-Free ASCII Lookup Table (LUT)**: All integer-to-ASCII conversions use a pre-computed 200-byte lookup table (`DIGIT_PAIRS`), completely eliminating costly `DIV` and `IDIV` CPU instructions.
4. **128-Bit Request ID SSSE3 SIMD Hex Stamping**: Full 128-bit Request IDs are formatted into exact 32 lowercase hex characters using vector shuffle instructions (`_mm_shuffle_epi8` / PSHUFB) without heap allocations.
5. **Zero-Alloc RFC 5952 IPv6 Formatting**: Native IPv6 addresses are zero-compressed on a fixed 40-byte stack buffer, eliminating runtime `v6.to_string()` heap string churn.
6. **Persistent Sink Handle**: File descriptors are opened once at plugin instantiation time and held open across all 100ms flush cycles, eliminating per-cycle `open()` and `close()` syscall overhead.
7. **Reference-Counted Memory Reclamation**: Log record batches are stored in `Arc<[AccessLogRecord]>`. As soon as all active exporters finish processing a batch, memory is reclaimed without locking.
