# Architectural Specification: Grafana Loki Log Shipper (`loki_log_shipper`)

This document details the architectural invariants, threading model, framing mechanisms, and multi-stream partitioning of the `loki_log_shipper` background logging plugin in the **Velda Edge Data Plane**.

---

## 1. System Context & Multi-Stream Partitioning Architecture

Loki organizes logs into streams defined by sets of labels. Improper label design leads to cardinality explosion. `loki_log_shipper` enforces bounded cardinality by design:

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
│   │ LokiLogShipperPlugin::run()                                                    │   │
│   │  ├─ Partitioning by Downstream Protocol into 3 Bounded Stream Buckets:         │   │
│   │  │   ├─ Stream 1: protocol="http" (HTTP/1.1, HTTP/2, HTTP/3 records)           │   │
│   │  │   ├─ Stream 2: protocol="grpc" (gRPC over TCP / UDP records)                │   │
│   │  │   └─ Stream 3: protocol="raw"  (L4 TCP / UDP proxy records)                 │   │
│   │  │                                                                             │   │
│   │  ├─ Splicing Pre-Baked Stream Descriptors (LokiStreamTemplate)                 │   │
│   │  ├─ Division-Free 2-Digit ASCII LUT Integer Serialization                      │   │
│   │  ├─ Persistent HTTP/1.1 TCP Stream Write with Keep-Alive to /loki/api/v1/push   │   │
│   │  │                                                                             │   │
│   │  └─ Automatic Reference-Counted Batch Reclamation on Drop                      │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Multi-Stream Partitioning & Cardinality Protection

Loki's storage architecture creates an independent index entry for every unique combination of label key-value pairs. 
- **Anti-Pattern (Index Explosion)**: Putting `request_id`, `ip_address`, or `user_agent` into Loki stream labels creates millions of unique streams, crashing the Loki ingester process.
- **Velda Edge Solution (Bounded Cardinality)**:
  - Stream labels contain strictly low-cardinality static metadata:
    `{"service": "velda-edge", "protocol": "http", "env": "production"}`
  - High-cardinality fields are placed into the log entry payload string:
    `["<timestamp_nanos>", "req_id=0011223344556677 proto=h2 status=200 dur_us=320 peer=192.168.1.1:50000"]`
  - This allows LogQL log aggregation and regex filtering without creating unmanageable index overhead.

---

## 3. Pre-Baked Stream Descriptors (`LokiStreamTemplate`)

At plugin startup, `LokiStreamTemplate::new(service_name, static_labels)` pre-serializes the JSON stream headers:
```rust
pub struct LokiStreamTemplate {
    pub http_prefix: Vec<u8>,
    pub grpc_prefix: Vec<u8>,
    pub raw_prefix: Vec<u8>,
}
```
Each prefix is pre-built as:
`{"stream":{"service":"velda-edge","protocol":"http","env":"production"},"values":[`

During runtime serialization:
1. `format_loki_push_payload` partitions drained records into three local byte vectors (`http_values`, `grpc_values`, `raw_values`).
2. Splicing writes the outer `{"streams":[` container.
3. Appends the pre-baked prefix, appends the formatted values, and closes the stream with `]}`.
4. Total string allocations during push payload generation: **0 allocations**.

---

## 4. Shadow Worker Threading & Network Resilience

- **Execution Context**: Pinned to CPU Core 0, executing under Linux `SCHED_IDLE`.
- **Zero Frontline Interference**: Preempted instantly by CFS whenever request traffic hits `velda-worker-0`.
- **Persistent HTTP/1.1 Connection**: Reuses a single TCP stream with `TCP_NODELAY` enabled.
- **Fail-Open Isolation**: If Loki returns HTTP 429 (ingestion rate limit exceeded) or times out, the shadow worker logs a metric failure, resets the connection, and drops pending batches to safeguard memory limits.
