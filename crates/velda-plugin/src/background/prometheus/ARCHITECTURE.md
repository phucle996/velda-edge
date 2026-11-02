# Prometheus Telemetry Exporter Plugin (`prometheus`) — Architecture

This document describes the architectural design, security boundaries, telemetry invariants, and internal pipeline of the **Prometheus Exporter** plugin in Velda Edge.

---

## 1. Architectural Mission & Invariants

The Prometheus Exporter plugin (`crates/velda-plugin/src/background/prometheus/`) provides an OpenMetrics-compliant, low-overhead HTTP/1.1 and HTTPS endpoint for scraping gateway metrics (`GET /metrics`) and health status (`GET /healthz`).

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Auxiliary Management Core (Core #0)                  │
│                                                                        │
│   Incoming Scrape (Prometheus / Victoriametrics / Datadog Agent)       │
│                                │                                       │
│                                ▼                                       │
│                   ┌───────────────────────────┐                        │
│                   │   ExporterStream (TCP/TLS)│                        │
│                   └─────────────┬─────────────┘                        │
│                                 │ Rate & Timeout Limits                │
│                                 ▼                                       │
│                   ┌───────────────────────────┐                        │
│                   │  Auth / CORS Verification │                        │
│                   └─────────────┬─────────────┘                        │
│                                 │ (Pass)                               │
│                                 ▼                                       │
│                   ┌───────────────────────────┐                        │
│                   │  MetricsStore::snapshot() │ (Atomic / Lock-Free)   │
│                   └─────────────┬─────────────┘                        │
│                                 │                                      │
│                                 ▼                                      │
│                   ┌───────────────────────────┐                        │
│                   │   format_snapshot()       │ (Stack 2-Digit LUT,    │
│                   │   Zero Allocation Engine  │  Zero Recalculation)   │
│                   └─────────────┬─────────────┘                        │
│                                 │                                      │
│                                 ▼                                      │
│                        HTTP/1.1 Response Body                          │
└─────────────────────────────────┼──────────────────────────────────────┘
                                  │ Direct Wire Response
                                  ▼
```

### Core Invariants:
1. **Auxiliary Domain Isolation**: Runs exclusively on supervisor/auxiliary core runtimes. It never touches traffic event loops, worker pinning, or request hot-path execution.
2. **Pure Serialization / Zero Recalculation**: All rate calculations, percentages, and histograms are pre-aggregated and stored as pure 64-bit integers (`u64`/`i64`) by `velda-observability`. The formatter strictly serializes point-in-time snapshots into text without re-calculating business telemetry.
3. **Zero Heap Churn**: Scrape responses reuse an allocated string buffer across sequential requests, avoiding per-scrape heap allocations and memory fragmentation.
4. **Stack 2-Digit LUT ASCII Conversion**: High-performance integer-to-ASCII serialization using a static 200-byte lookup table (`DIGIT_PAIRS`), eliminating division loops and `std::fmt` overhead.
5. **Strict Bound Enforcement**: Output size is bounded by `max_buffer_size_bytes` to prevent memory exhaustion under adversarial conditions.

---

## 2. Component Structure

- **`mod.rs`**: Module declarations and public interface.
- **`server.rs`**: Auxiliary HTTP/1.1 and HTTPS server handling incoming connections, TLS termination, CORS preflight checks, authentication verification (Bearer / Basic), and scrape concurrency bounding via semaphore.
- **`formatter.rs`**: Core serialization engine converting `MetricsSnapshot` into OpenMetrics/Prometheus text format with `fast_write_u64` and `fast_write_i64`.

---

## 3. Data Flow & Lifecycles

1. **Client Connection**: Scrape agent initiates TCP handshake on the configured metrics port (default `9090`).
2. **Security Checks**:
   - Optional TLS handshake (TLS 1.2/1.3 with optional mTLS).
   - Semaphore acquisition (bounded concurrent scrape limit, e.g., max 10 concurrent requests).
   - Authentication check (constant-time header matching for Bearer or Basic credentials).
3. **Snapshot Extraction**: Extracts an immutable `MetricsSnapshot` from `MetricsStore::snapshot()`. This operation is lock-free and non-blocking for traffic workers.
4. **Formatting**:
   - Clears and pre-reserves scratch buffer.
   - Emits Help/Type metadata headers.
   - Emits pipeline counters, active gauges, byte counters, and latency histograms.
   - Emits global connection and TLS handshake counters.
   - Emits upstream connection pool telemetry ("The Soul of Velda").
5. **Delivery**: Writes HTTP/1.1 headers (`Content-Type: text/plain; version=0.0.4; charset=utf-8`) and stream body to wire socket. Buffer is preserved for subsequent scrapes.
