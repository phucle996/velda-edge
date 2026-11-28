# Internal Architecture Specification (`trace_exporter`)

## 1. Execution & Memory Isolation Model

`trace_exporter` pairs **lock-free core-local span capture** (`velda-observability::telemetry`) with **asynchronous OTLP batch serialization** on `SCHED_IDLE` Shadow Workers (`pbww = 50`):

```text
  Frontline Core (velda-worker-{i}, SCHED_OTHER)
    ├─► StatusSampling::keep(status) ─► 3-ALU Weyl Gate ((state + GOLDEN) >> 1) < threshold
    └─► SpscTelemetryRing[core_id] (#repr(C, align(64)), 256B geometry, ContiguousArena):
          └─ non-temporal write (_mm256_stream) -> bypasses L1d cache -> WC buffer -> RAM
          │
          ▼ flush_lane(worker_id, 512) -> TelemetryBatchPool (64 × 64KB Slots, leases=AtomicUsize)
  Shadow Worker (velda-shadow-{i}, SCHED_IDLE)
    └─► JoinSet per TraceTargetConfig:
          subscriber.drain_batches(64)
            ├─ OtlpTraceProtoEnvelope -> direct tag/varint/LE write (no prost structs)
            ├─ OtlpTraceJsonEnvelope  -> direct hex/itoa JSON append
            └─ wire::push_traces()    -> HTTP/1.1 Keep-Alive (/v1/traces) | H2 gRPC (5B hdr + bytes)
                  └─ drop(batches)    -> leases.fetch_sub(1) -> return slot at 0 (0 __rust_dealloc)
```

## 2. Cache Line 0 Span Layout & Zero-Copy Fan-Out (`record.rs`, `pool.rs`, `ring.rs`)

`TelemetryRecord` is compile-time asserted to `size_of == 256 B` and `align_of == 64 B` (`4` contiguous cache lines). Access-log shippers consume all `4` lines (CL0–CL3 including `url_bytes[184]`), whereas `trace_exporter` restricts reads strictly to **Cache Line 0 (`0..64 B`)**:

```text
  TelemetryRecord Cache Line 0 (Bytes 0..64):
  ┌──────────────────┬──────────────────┬──────────────────┬──────────────────┬──────────┐
  │ request_id (8B)  │ duration_ns (8B) │ peer_ip (16B)    │ ingress_ns (8B)  │ rx/tx(8B)│
  ├──────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────┤
  │ upstream_ip (4B) │ port (2B) | pad  │ route_id (2B)    │ upstream_id (2B) │ meta(4B) │
  └──────────────────┴──────────────────┴──────────────────┴──────────────────┴──────────┘
   meta(4B) = method: HttpMethod (1B) | pipe: PipeKind (1B) | internal_status (1B) | rsv (1B)
```

- **Local W3C Trace/Span ID Derivation**: `trace_id_bytes()` (`16 B`) derives directly from `ingress_time_nanos | request_id`, and `span_id_bytes()` (`8 B`) derives from `request_id`, eliminating UUID allocation and PRNG calls per span.
- **SPSC HugePage Ring**: Each core owns an `8,192`-record (`2 MB` Linux HugePage via `ContiguousArena`) `SpscTelemetryRing` whose producer (`head`, `cached_tail`, `dropped`) and consumer (`tail`, `cached_head`) sit on distinct `64 B` cache lines (`align(64)`).
- **Multi-Buffer Pooled Leases (`TelemetryBatchPool`)**: `64` pre-allocated `64 KB` contiguous slots (`256` records/slot = `4 MB` total = `2 × 2 MB` HugePages) fan out zero-copy to $K$ subscribers (`log_exporter` + multiple `trace_exporter` targets) via `leases: AtomicUsize`. Dropping the final `TelemetryBatch` resets `records.clear()` and recycles the slot with `0` OS deallocations.

## 3. Protocol-Agnostic Weyl Sampling Gate (`sampling/state.rs`, `status.rs`)

Hot-path span throttling runs before pushing to `SpscTelemetryRing` without atomic counters or cryptographic PRNGs:
- Each worker seeds its thread-local Weyl state via `seed_worker(lane) = lane * 0x9E37_79B9_7F4A_7C15` (`GOLDEN`) so cores remain decorrelated.
- Evaluation computes:
  $$c \leftarrow (c + \text{GOLDEN}) \bmod 2^{64}, \qquad \text{keep} \iff (c \gg 1) < \left\lfloor r \cdot 2^{63} \right\rfloor$$
- Executing in `3` GPR instructions (`add`, `shr`, `cmp`) over `[u64; InternalStatus::COUNT]` (`StatusSampling`), error rates (e.g., `internal_error: 1.0`) override noisy success rates (`completed: 0.01`) in `O(1)`.

## 4. Direct Wire Encoding & Catalog Resolution (`providers/otlp/`)

### 4.1 Protobuf Binary Framing (`proto/template.rs`)
Instead of materializing `opentelemetry_proto` `Vec<KeyValue>` / `Vec<Span>` heap vectors per tick:
- `OtlpTraceProtoEnvelope::new(service_name)` pre-bakes immutable `Resource` (`service.name`) and `InstrumentationScope` (`velda.edge.router`) protobuf byte slices once.
- `format_spans_wire` serializes each `TelemetryRecord` sequentially into reusable `single_span` / `spans_wire` scratch buffers, emitting fixed `TraceId` (`tag 1`), `SpanId` (`tag 2`), `<METHOD> <route>` (`tag 5`, resolved lock-free via `velda_router::with_route_bytes`), `SPAN_KIND_SERVER = 2` (`tag 6`), `fixed64` timestamps (`tag 7`/`8`), `net.peer.name` (`velda_upstream::with_upstream_bytes`), `net.peer.ip` (`velda_core::lut::ip::format_ip_bytes`), and `status` (`OK=1`, `ERROR=2`), then frames outer varint lengths into `out_buf`.

### 4.2 HTTP/1.1 & gRPC HTTP/2 Wire Transport (`wire.rs`)
- **Pre-Baked Header/URI Templates (`bake_wire_template`)**: Compiles HTTP/1.1 `POST {path} HTTP/1.1` prefix bytes (`application/x-protobuf` or `application/json`) or HTTP/2 `http::Uri` + `HeaderMap` (`application/grpc`, `TE: trailers`) at startup.
- **Zero-Alloc Length Stamping & Framing**: HTTP stamps `Content-Length` into `head_buf` using `velda_core::lut::decimal::fast_write_u64`; gRPC prepends the 5-byte length-prefixed prefix `[0x00, len_u32_be]` into `BytesMut` and checks trailing `grpc-status == 0`.
- **Resilient Slot Reconnect**: Attempt-0 transport errors invoke `lease.reconnect().await` in place on the leased `OtlpConn`; HTTP 5xx, `Connection: close`, gRPC rejection, or repeated failures trigger `lease.recreate_in_background()`.
