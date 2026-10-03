# velda-observability

High-performance observability, metrics collection, and distributed tracing instrumentation for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Observability Must Not Degrade Hot-Path Latency`.
> Telemetry collection must utilize atomic counters, thread-local aggregators, and zero-allocation log formatters to avoid stalling request execution.

---

## 1. Responsibilities

- **Metrics**: Lock-free counters, gauges, and latency histograms for active connections, request rates, error codes, and byte throughput.
- **Tracing**: Structured, asynchronous span emission integrated with [`tracing`](https://docs.rs/tracing) and OpenTelemetry.
- **Access Logging**: Fast, non-blocking access log streaming.
