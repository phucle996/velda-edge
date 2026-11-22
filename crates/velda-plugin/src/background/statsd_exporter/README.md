# StatsD / DogStatsD Metrics Exporter Plugin (`statsd_exporter`)

High-performance, zero-allocation, connectionless UDP telemetry exporter for Velda Edge. Drives background metrics streaming to Datadog Agent, Telegraf, StatsD, or Vector daemons strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production presets, quickstart verification | All Engineers, Operators |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision flowchart, CLI diagnostic toolkit, emergency runbooks | On-Call SREs, Systems Engineers |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep micro-architectural specification, memory geometry, execution loop, empirical disassembly proof, physical boundaries | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | First-principles 5-tier zero-bloat benchmark specification adhering to `/velda-benchmark` with 6-scenario comparative matrix | Performance Engineers, SRE Leads |

---

## 2. Overview & Core Invariants

The `statsd_exporter` is an autonomous background logistics plugin designed to push real-time Data Plane telemetry without competing with frontline traffic workers:

* **Shadow Worker Execution**: Operates exclusively on background companion threads (`velda-shadow-{id}`) running under Linux `SCHED_IDLE`. Traffic workers serving client HTTP requests never execute formatting or network syscalls.
* **Lock-Free Atomic Extraction**: Reads point-in-time snapshots from `velda-observability` via `MetricsStore::snapshot()` using relaxed atomic loads, completely avoiding locks, mutexes, or synchronization channels on the hot path.
* **Zero-Allocation Serialization**: Employs a pre-allocated reusable packet arena and a 200-byte 2-digit ASCII lookup table (`DIGIT_PAIRS`), completely eliminating `format!`, `String`, or dynamic vector allocations during metric encoding.
* **Strict MTU Safety Bounding**: Enforces `max_packet_size` (recommended: $\le 1432$ bytes) to guarantee datagrams fit within standard Ethernet MTU without triggering kernel IP fragmentation. Metric lines are never split across datagram boundaries.
* **Kernel Fast-Path `connect()` & Vectorized `sendmmsg`**: Uses a pre-connected non-blocking UDP socket to bypass per-packet kernel routing table lookups, and dispatches up to 64 datagrams per flush cycle in a single kernel transition via `libc::sendmmsg`.
* **Central Core Socket Ownership**: UDP sockets are authoritatively acquired through `velda_core::global_socket_manager().acquire_shadow_udp()`, preventing accidental port collisions with active traffic listeners.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — STATSD EXPORTER PIPELINE                                    ║
║                    (crates/velda-plugin: statsd_exporter::plugin)                                 ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS (Cores 0..N-1)]     [SHADOW LOGISTICS WORKER (Core i)]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Assigned Core i)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
  ...                                            
  velda-worker-n (Core N-1)                      
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
                        │ (LUT-accelerated)     │                             │ (Zero-tag formatting) │
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
                                                              ▼ UDP Datagrams (MTU <= 1432 B)
                                                   ┌─────────────────────────────────────┐
                                                   │   Datadog / Telegraf / StatsD       │
                                                   └─────────────────────────────────────┘
```

---

## 3. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, background logistics plugins are assigned a workload weight from 1 to 100 based on CPU intensity, memory footprint, cadence/wakeup rate, and failure blast radius:

| Physical Dimension | Metric Profile | Impact Analysis |
| :--- | :--- | :--- |
| **Assigned Workload Weight** | **20** | **Moderate-Low Background Workload** |
| **CPU Intensity** | Low-to-Moderate | Periodic timer wakeup (default: 2s), relaxed atomic vector summation across pipeline slots, 2-digit LUT byte formatting, and a single vectorized `sendmmsg` syscall. Active duration is typically sub-millisecond per cycle. |
| **Memory Footprint** | ~150 KB | Fixed reusable packet arena (64 pre-allocated buffers $\times 1688$ bytes) + scratch formatting buffers. Zero ongoing heap churn. |
| **Cadence / Wakeup Rate** | Configurable (1s – 3600s) | Operates on a coarse-grained interval (default: 2s), producing minimal scheduler wakeups. |
| **Failure Blast Radius** | P2 (Low Impact) | Telemetry loss only. Dropped UDP packets or dead collectors have zero impact on live client traffic serving. |

### Complementary Static Sharding Recommendation
* **Target Affinity**: Core 1, 2, or 3.
* **Anti-Colocation Rule**: Avoid co-locating on Core 0 when Core 0 is strictly dedicated to latency-critical `health_probe` (Weight: 1) and master traffic intake. Do not pair with heavy log shippers (`loki_log_shipper` or `otlp_log_shipper`) on cores with constrained memory bandwidth.
* **Ideal Pairing**: Pair with sleep-bound rotators (`jwks_rotator`, `acme_cert_rotator`) or lightweight service discovery background tasks on secondary cores.

---

## 4. 100% Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Execution stage is fixed at compile time (`BackgroundPlugin`).

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `endpoint` | String | **Yes** | Valid `IP:PORT` or `HOST:PORT` | `"127.0.0.1:8125"` | Target UDP socket address of StatsD, DogStatsD, or Telegraf daemon. |
| `interval_secs` | Integer (u64) | **Yes** | `1 ..= 3600` | `2` | Periodic telemetry flush interval in seconds. |
| `prefix` | String | **Yes** | Non-empty, ASCII alphanumerics, `_`, `-`, `.` | `"velda"` | Global metric name prefix prepended to all exported metrics. |
| `dialect` | String | **Yes** | `"dogstatsd"` or `"standard"` | `"dogstatsd"` | Metric line protocol: `"dogstatsd"` (with `#k:v` tags) or `"standard"` (hierarchical dot notation). |
| `max_packet_size` | Integer (usize) | **Yes** | `256 ..= 65507` bytes | `1432` | Maximum UDP packet payload size before flushing to prevent kernel IP fragmentation. |

---

## 5. Production Configuration Manifest Examples

### Profile 1: Kubernetes DogStatsD DaemonSet (`plugins.json`)
*Recommended for Datadog Agent, Vector, or Telegraf setups running as a node daemon or sidecar.*

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

### Profile 2: High-Throughput InfluxDB / Telegraf Standard StatsD (`plugins.json`)
*Recommended for classic StatsD collectors, InfluxDB Telegraf, or Graphite carbon receivers.*

```json
{
  "id": "statsd_exporter",
  "config": {
    "endpoint": "10.0.1.50:8125",
    "interval_secs": 5,
    "prefix": "edge_gateway",
    "dialect": "standard",
    "max_packet_size": 1432
  }
}
```

### Profile 3: WAN Edge / MTU-Constrained Environment (`plugins.json`)
*Recommended for cross-datacenter telemetry or VPN tunnels where effective MTU is lower than 1500.*

```json
{
  "id": "statsd_exporter",
  "config": {
    "endpoint": "198.51.100.25:8125",
    "interval_secs": 10,
    "prefix": "edge_wan",
    "dialect": "dogstatsd",
    "max_packet_size": 1024
  }
}
```

---

## 6. Verification & Quickstart

### Step 1: Start Mock UDP Collector Listener
```bash
# Listen on UDP port 8125 using netcat
nc -l -u -p 8125
```

### Step 2: Declare in `plugins.json`
Include the plugin block in `plugins.json`:

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
rtk cargo run -p velda-edge
```

### Step 4: Inspect Streamed Metrics
Verify formatted UDP datagrams arrive in the collector terminal:
```text
velda.connections.total:1420|c
velda.connections.active:12|g
velda.requests.total:5400|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered,status:2xx
velda.bytes.rx:5529600|c|#downstream:http/1.1,upstream:http/1.1,pipe:buffered
velda.pool.connections.idle:32|g
```
