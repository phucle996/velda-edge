# Standard Stream Log Shipper Plugin (`std_log_shipper`)

High-throughput, zero-allocation background logging exporter for Velda Edge. Subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC, Raw L4 TCP/UDP) and writes formatted JSON, text, or Common Log Format (CLF) streams directly to `stdout`, `stderr`, or persistent filesystem targets strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production profiles, quickstart verification | All Engineers, Operators |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision flowchart, CLI diagnostic toolkit, emergency playbooks | On-Call SREs, Systems Engineers |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep micro-architectural specification, memory geometry, SSSE3 SIMD PSHUFB hex stamping, disassembly proof, physical boundaries | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | First-principles 5-tier zero-bloat benchmark specification adhering to `/velda-benchmark` with 6-scenario comparative matrix | Performance Engineers, SRE Leads |

---

## 2. Overview & Core Invariants

The `std_log_shipper` plugin operates as an autonomous background companion service designed to format and persist access logs without competing with frontline traffic workers:

* **Shadow Worker Execution**: Operates exclusively on background companion threads (`velda-shadow-{id}`) under Linux `SCHED_IDLE`. Traffic workers serving client HTTP requests never execute string formatting, serialization, or file I/O syscalls.
* **Lock-Free SPSC Ring Drain**: Frontline traffic workers push 64-byte scalar `AccessLogRecord` structs into core-local Single-Producer Single-Consumer (SPSC) ring buffers without mutexes or lock contention.
* **Zero-Allocation Formatting Arena**: `format_std_log_payload` pre-reserves buffer capacity upfront based on batch size and writes formatted bytes directly using static slices, a 200-byte 2-digit ASCII lookup table (`DIGIT_PAIRS`), and 128-bit SSSE3 SIMD vector instructions for request ID hex stamping.
* **Persistent File Descriptor Reuse**: The target file or stream descriptor is opened once at plugin instantiation and held persistently across all periodic flush cycles, completely eliminating per-cycle `open()` and `close()` syscall overhead.
* **Multi-Format Serialization**: Supports structured NDJSON (`"json"`), human-readable text (`"text"`), or NCSA Common Log Format (`"clf"`).
* **Fail-Safe SPSC Bounded Queue**: If disk writes stall or fail, the bounded SPSC ring drops records at the buffer threshold to prevent memory exhaustion and preserve client request serving throughput.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — STD LOG SHIPPER PIPELINE                                   ║
║                    (crates/velda-plugin: std_log_shipper::plugin)                                ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS (Cores 0..N-1)]     [SHADOW LOGISTICS WORKER (Core i)]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Assigned Core i)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
  ...                                            
  velda-worker-n (Core N-1)                      
        │                                                    │ (Periodic Ticker)
        ▼                                                    ▼
  ┌───────────┐  Zero-Lock SPSC Ring Drain   ┌──────────────────────────────────┐
  │ AccessLog │ ───────────────────────────► │  Drain Batches (max_batch_size)  │
  │  Record   │    (O(1) Ring Dequeue)       └────────────────┬─────────────────┘
  └───────────┘                                               │
                                                ┌─────────────┴─────────────┐
                                                │ Upfront Buffer Capacity   │
                                                │ (O(1) Scratch Allocation) │
                                                └─────────────┬─────────────┘
                                                              │
                                   ┌──────────────────────────┴──────────────────────────┐
                                   │                                                     │
                            [format: "json"]                                      [format: "text"]
                                   ▼                                                     ▼
                        ┌─────────────────────┐                               ┌─────────────────────┐
                        │   format/json.rs    │                               │   format/text.rs    │
                        │ Structured NDJSON   │                               │ Extended Text / CLF │
                        │ SIMD Itoa + PSHUFB  │                               │ SIMD Itoa + PSHUFB  │
                        │ (LUT-accelerated)   │                               │ (LUT-accelerated)   │
                        └──────────┬──────────┘                               └──────────┬──────────┘
                                   │                                                     │
                                   └──────────────────────────┬──────────────────────────┘
                                                              │
                                                              ▼
                                                     ┌─────────────────┐
                                                     │   sink/mod.rs   │
                                                     │   StdLogSink    │
                                                     │ Persistent FD   │
                                                     │ write_all()     │
                                                     └────────┬────────┘
                                                              │
                                    ┌─────────────────────────┼─────────────────────────┐
                                    ▼                         ▼                         ▼
                                [stdout]                   [stderr]                  [File]
                               (Console)                  (Console)              (/var/log/...)
```

---

## 3. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, background logistics plugins are assigned a workload weight from 1 to 100 based on CPU intensity, memory footprint, cadence/wakeup rate, and failure blast radius:

| Physical Dimension | Metric Profile | Impact Analysis |
| :--- | :--- | :--- |
| **Assigned Workload Weight** | **15** | **Low Background Workload** |
| **CPU Intensity** | Low | SPSC ring batch drain, SIMD hex stamping (`PSHUFB`), 2-digit LUT byte conversion, and single persistent VFS write syscall. Zero network protocol or TLS encryption overhead. |
| **Memory Footprint** | ~64 KB | Reusable formatting scratch vector (`out_buf`) + SPSC ring subscriber buffers. Zero ongoing heap churn. |
| **Cadence / Wakeup Rate** | Moderate (50ms – 1000ms) | Operates on sub-second flush intervals (default: 100ms) to ensure low log latency. |
| **Failure Blast Radius** | P3 (Low Impact) | Access log loss only. If the target disk is full or read-only, write errors are logged while client request serving continues without delay. |

### Complementary Static Sharding Recommendation
* **Target Affinity**: Core 1, 2, or 3.
* **Anti-Colocation Rule**: Avoid co-locating on Core 0 when Core 0 is strictly dedicated to latency-critical `health_probe` (Weight: 1) and master traffic intake.
* **Ideal Pairing**: Pair with sleep-bound rotators (`jwt_auth` in `external` mode, `acme_cert_rotator`) or lightweight UDP exporters (`statsd_exporter`) on secondary cores.

---

## 4. 100% Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Execution stage is fixed at compile time (`BackgroundPlugin`).

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `target` | String | **Yes** | `"stdout"`, `"stderr"`, or valid absolute file path | `"/var/log/velda/access.log"` or `"stdout"` | Output destination stream for formatted access log records. |
| `format` | String | **Yes** | `"json"`, `"text"`, or `"clf"` (case-insensitive) | `"json"` | Serialization format: structured NDJSON (`"json"`), human-readable text (`"text"`), or Common Log Format (`"clf"`). |
| `flush_interval_ms` | Integer (u64) | **Yes** | `50 ..= 60000` | `100` | Periodic timer interval for waking up shadow worker to drain SPSC rings and flush buffered bytes. |
| `max_batch_size` | Integer (usize) | **Yes** | `1 ..= 10000` | `1000` | Maximum number of log records drained and processed in a single sequential flush cycle. |

---

## 5. Production Configuration Profiles

### Profile 1: High-Throughput Production Profile (Structured JSON via File Stream)
*Recommended for production edge nodes shipping logs to Vector, Fluent Bit, or Datadog agents via filesystem rotation.*

```json
{
  "id": "std_log_shipper",
  "config": {
    "target": "/var/log/velda/access.log",
    "format": "json",
    "flush_interval_ms": 100,
    "max_batch_size": 1000
  }
}
```

### Profile 2: Containerized Cloud Native Profile (JSON via Standard Output)
*Recommended for Kubernetes, Docker, and serverless environments where stdout is captured by container runtimes.*

```json
{
  "id": "std_log_shipper",
  "config": {
    "target": "stdout",
    "format": "json",
    "flush_interval_ms": 100,
    "max_batch_size": 1000
  }
}
```

### Profile 3: Legacy Syslog / Web Server Text Profile (Common Log Format)
*Recommended for traditional web server analytics tools requiring standard NCSA Common Log Format.*

```json
{
  "id": "std_log_shipper",
  "config": {
    "target": "/var/log/velda/access_clf.log",
    "format": "clf",
    "flush_interval_ms": 250,
    "max_batch_size": 2000
  }
}
```

---

## 6. Verification & Quickstart

### Step 1: Prepare Log Directory (If Logging to File)
```bash
sudo mkdir -p /var/log/velda
sudo chown -R $(whoami) /var/log/velda
```

### Step 2: Declare in `plugins.json`
Include the plugin block in `plugins.json`:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "std_log_shipper",
      "config": {
        "target": "stdout",
        "format": "json",
        "flush_interval_ms": 100,
        "max_batch_size": 1000
      }
    }
  ]
}
```

### Step 3: Run Velda Edge Gateway
```bash
rtk cargo run -p velda-edge
```

### Step 4: Verify Formatted Access Logs
Generate test requests and verify formatted NDJSON access logs appear on `stdout`:
```json
{"req_id":"673708a2000000010000000000000001","ts":1731659938,"dur_us":1450,"status":200,"method":"GET","path":"/api/v1/ping","client_ip":"127.0.0.1","downstream":"h1","upstream":"h1","pipe":"buffered","bytes_rx":128,"bytes_tx":256}
```
