# velda-observability

High-performance observability, metrics collection, and distributed tracing instrumentation for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Observability Must Not Degrade Hot-Path Latency`.
> Telemetry collection must utilize atomic counters, thread-local aggregators, and zero-allocation log formatters to avoid stalling request execution.

---

## 1. Responsibilities

- **Metrics**: Lock-free counters, gauges, and latency histograms for active connections, request rates, error codes, and byte throughput (always 100%, never sampled).
- **Unified Telemetry (`telemetry`)**: Non-blocking 256B `TelemetryRecord` streaming into a single 2 MB HugePage / contiguous `mmap` per-core `SpscTelemetryRing`, rotated through a shared `leases`-refcounted `TelemetryBatchPool` inside `TelemetryStore`. Trace fields sit in Cache Line 0 (`0..64` B) and derive zero-trust W3C IDs locally so both log shippers and OTLP trace exporters consume the same pooled `TelemetryBatch` without duplicate emits or ring buffers.
- **Sampling (`sampling`)**: Thread-local Weyl-sequence threshold gates (`HttpSampling` by status class + `shed`, `GrpcSampling` by canonical status code + `shed`) keeping Log + Trace emission atomic at <1 ns without hashing. Note: true request counts remain exact via Metrics (`emitted_total` reflects sampled volume).
