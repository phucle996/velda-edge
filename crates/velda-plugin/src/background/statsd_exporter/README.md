# StatsD / DogStatsD Metrics Exporter Plugin

High-performance, zero-allocation, connectionless UDP telemetry exporter for Velda Edge. Drives background metrics streaming to Datadog Agent, Telegraf, StatsD, or Vector daemons strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Subsystem internals, UDP socket lifecycle, MTU packet batching, memory invariants | Systems Architects, Core Contributors |
| **[BENCHMARK.md](BENCHMARK.md)** | 4-Tier empirical performance specifications, throughput, and comparison matrix | Performance Engineers, SRE Leads |
| **[MACHINE_CODE_ANALYSIS.md](MACHINE_CODE_ANALYSIS.md)** | RDTSC cycle measurements, x86_64 disassembly, LUT formatting, Linux `perf` | Low-Level Engineers, Kernel Specialists |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision tree, CLI diagnosis | On-Call SREs, Production Operations |
| **[TEST.md](TEST.md)** | Integration test catalog, mock UDP receiver verification, invariant testing | QA Engineers, Contributors |

---

## 2. Overview & Core Invariants

* **Stateless & Connectionless Transport**: Streams metrics over non-blocking UDP (`tokio::net::UdpSocket`), eliminating TCP state-machine overhead, three-way handshakes, and ACK packet latency.
* **Core-Managed Socket Lifecycle**: Sockets are authoritatively allocated through [`velda_core::global_socket_manager().acquire_shadow_udp()`](../../../../velda-core/src/socket.rs), preventing port collisions with Data Plane traffic listeners.
* **Zero-Allocation Byte Serializer**: Employs a pre-allocated reusable packet buffer and a 2-digit ASCII lookup table (`DIGIT_PAIRS`), completely eliminating `format!` runtime allocations and vtable parsing.
* **MTU-Bounded Packet Batching**: Strictly enforces `max_packet_size` (required, recommended: 1432 bytes) to guarantee datagrams fit within standard Ethernet MTU without triggering kernel IP fragmentation.
* **Off-Path Shadow Worker Isolation**: Operates exclusively on background logistics tasks (`phase: "background"` inferred automatically by `canonical_phase_for_id`), reading lock-free atomic snapshots (`store.snapshot()`) with zero write contention on traffic workers.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — STATSD EXPORTER ARCHITECTURE                                   ║
║              (crates/velda-plugin/src/background/statsd_exporter: StatsdExporterPlugin)              ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS]                     [SHADOW LOGISTICS WORKER: velda-shadow-0]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Core 0, interval_secs)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
        │                                                    │ (Periodic Ticker)
        ▼                                                    ▼
  ┌───────────┐  Lock-Free Atomic Extraction ┌──────────────────────────────────┐
  │ Pipeline  │ ───────────────────────────► │      MetricsStore::snapshot()    │
  │ Counters  │    (Ordering::Relaxed)       │   (36 Pipelines, 563 Data Points)│
  └───────────┘                              └────────────────┬─────────────────┘
                                                              │
                                   ┌──────────────────────────┴──────────────────────────┐
                                   │                                                     │
                        [dialect: "dogstatsd"]                                [dialect: "standard"]
                                   ▼                                                     ▼
                        ┌───────────────────────┐                             ┌───────────────────────┐
                        │ dialect/dogstatsd.rs  │                             │  dialect/standard.rs  │
                        │ Pre-baked Tag Suffix  │                             │ Pre-baked Dot Paths   │
                        │ Direct-to-Packet Pass │                             │ Direct-to-Packet Pass │
                        │ (14,356 CPU cycles)   │                             │ (10,022 CPU cycles)   │
                        └──────────┬────────────┘                             └──────────┬────────────┘
                                   │                                                     │
                                   └──────────────────────────┬──────────────────────────┘
                                                              │
                                                              ▼
                                                   ┌─────────────────────┐
                                                   │    transport.rs     │
                                                   │ acquire_shadow_udp  │
                                                   │ kernel connect()    │
                                                   │ libc::sendmmsg(64)  │
                                                   └──────────┬──────────┘
                                                              │
                                                              ▼ UDP Datagrams (MTU 1432)
                                                   ┌─────────────────────────────────────┐
                                                   │   Datadog / Telegraf / StatsD       │
                                                   └─────────────────────────────────────┘
```

---

## 3. 100% Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Execution phase is automatically inferred as `background` via `canonical_phase_for_id` based on `id: "statsd_exporter"`.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `endpoint` | String | **Yes** | Valid `IP:PORT` or `HOST:PORT` | `"127.0.0.1:8125"` | Target UDP socket address of StatsD, DogStatsD, or Telegraf daemon. |
| `interval_secs` | Integer (u64) | **Yes** | `1 ..= 3600` | `2` | Periodic telemetry flush interval in seconds. |
| `prefix` | String | **Yes** | Non-empty, ASCII alphanumerics, `_`, `-`, `.` | `"velda"` | Global metric name prefix prepended to all exported metrics. |
| `dialect` | String | **Yes** | `"dogstatsd"` or `"standard"` | `"dogstatsd"` | Metric line format: `"dogstatsd"` (with `#k:v` tags) or `"standard"` (dot notation). |
| `max_packet_size` | Integer (usize) | **Yes** | `256 ..= 65507` bytes | *(none, recommended 1432)* | Maximum UDP packet payload size before flushing to avoid IP fragmentation. |

---

## 4. Production Configuration Manifest Examples

### Profile 1: Standard Production DogStatsD Manifest (`plugins.json`)
*Recommended for Datadog Agent, Vector, or Telegraf setups utilizing tag-based dimensional analytics.*

```json
{
  "id": "statsd_exporter",
  "config": {
    "endpoint": "127.0.0.1:8125",
    "interval_secs": 2,
    "prefix": "velda",
    "dialect": "dogstatsd",
    "max_packet_size": 1432
  }
}
```

### Profile 2: Classic StatsD / Graphite Manifest (`plugins.json`)
*Recommended for Etsy StatsD daemons, Graphite carbon receivers, or Prometheus statsd-exporter.*

```json
{
  "id": "statsd_exporter",
  "config": {
    "endpoint": "10.0.1.50:8125",
    "interval_secs": 5,
    "prefix": "edge_cluster",
    "dialect": "standard",
    "max_packet_size": 1432
  }
}
```

---

## 5. Verification & Quickstart

### Step 1: Start Mock UDP Collector Listener
```bash
# Listen on UDP port 8125 using netcat
nc -l -u -p 8125
```

### Step 2: Declare in `plugins.json`
Include the plugin block in `plugins.json` within the storage directory:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "statsd_exporter",
      "config": {
        "endpoint": "127.0.0.1:8125",
        "interval_secs": 2,
        "prefix": "velda",
        "dialect": "dogstatsd",
        "max_packet_size": 1432
      }
    }
  ]
}
```

### Step 3: Run Velda Edge Gateway
```bash
cargo run -p velda-edge
```

### Step 4: Inspect Received Metrics
Verify UDP datagrams stream into the collector terminal:
```text
velda.connections.total:1420|c
velda.connections.active:12|g
velda.requests.total:5400|c|#downstream:h1,upstream:h1,pipe:buffered,status:2xx
velda.bytes.rx:5529600|c|#downstream:h1,upstream:h1,pipe:buffered
velda.pool.connections.idle:32|g
```
