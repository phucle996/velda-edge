# Internal Architecture Specification (`metrics_exporter`)

## 1. Execution & Memory Isolation Model

`metrics_exporter` strictly separates **wait-free Frontline recording** (`velda-observability`) from **Shadow asynchronous serialization and network I/O** (`pbww = 40`):

```text
  Frontline Core (velda-worker-{0..N-1}, SCHED_OTHER)
    └─► MetricsStore (per-core sharded slots, #repr(align(64))):
          ├─ load-before-or on status_mask: AtomicU64 (skips fetch_or once status bit seen)
          ├─ status_counts[InternalStatus as usize].fetch_add(1, Relaxed)
          └─ counters: [AtomicU64; 16] (bytes RX/TX + 7 duration + 6 TTFB + 1 pad = 128B)
          │
          ▼ wait-free snapshot() (Relaxed atomic reads, zero mutexes)
  Shadow Worker (velda-shadow-{i}, SCHED_IDLE)
    ├─► JoinSet task per TargetSlot:
    │     ├─ Pull   ─► BufferPool scratch Vec<u8> -> PrometheusTemplate -> HTTP/HTTPS server
    │     ├─ OTLP   ─► curr.delta_since(&prev) ─► OtlpProtoTemplate | OtlpJsonTemplate ─► OtlpEtp
    │     └─ StatsD ─► curr.delta_since(&prev) ─► DogStatsd / Standard MTU chunks ─► sendmmsg(64)
```

## 2. Pipeline Matrix Geometry & Sparse `status_mask` Scanning

`velda-observability` partitions telemetry across `64` pipeline slots (`PipeKind::ALL`), each holding `InternalStatus::COUNT = 41` status counters (`41 <= 64`) and `PIPELINE_COUNTERS_LEN = 16` contiguous transport/histogram counters (`128 B`, sized to `4 × 256-bit` AVX2 / `8 × 128-bit` NEON registers):

```text
  PipelineSlot (per-core shard, zero locks):
  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │ status_mask:    AtomicU64                          [bit s = 1 iff status s > 0]     │
  │ active_streams: AtomicI64                          [point-in-time UpDownCounter]    │
  │ status_counts:  [AtomicU64; InternalStatus::COUNT] [41 monotonic request totals]    │
  │ counters:       [AtomicU64; 16]                    [RX, TX, 7 Dur, 6 TTFB, 1 Pad]   │
  └─────────────────────────────────────────────────────────────────────────────────────┘
```

- **Zero Branchy Zero-Writes (`load-before-or`)**: Frontline workers test `(slot.status_mask.load(Relaxed) & bit) == 0` before calling `fetch_or(bit, Relaxed)`. On steady-state traffic (e.g., `completed` bit already `1`), `fetch_or` is skipped completely, avoiding shared MESI cache-line RFO invalidations.
- **`O(K)` Sparse Hardware Iteration**: Both `add_status_counts` (`counter.rs`) and all 5 provider template formatters iterate active statuses via CPU bit-scan primitives:
  ```rust
  let mut mask = slot.status_mask;
  while mask != 0 {
      let idx = mask.trailing_zeros() as usize; // x86 TZCNT / ARM RBIT+CLZ
      mask &= mask - 1;                         // x86 BLSR (clears lowest set bit)
      emit_status(slot, idx, slot.status_counts[idx]);
  }
  ```
- **Sparse Complexity Bound**: With $P = 64$ pipeline slots, $S = 41$ status variants, and $K_p \ll S$ actually observed statuses per interval, formatting visits $\sum_{p=0}^{P-1} K_p$ cells instead of $P \cdot S = 2,624$ cells, eliminating empty-series bloat on wire payloads.

## 3. `delta_since` Conservation & Point-in-Time Gauges (`snapshot.rs`)

When `Push` targets run in delta mode (`OtlpTemporality::Delta` or StatsD), `MetricsSnapshot::delta_since(&prev)` builds an incremental snapshot without resetting shared Data Plane atomics:

- **Monotonic Counters & Histograms**: Each counter field $C$ emits $\Delta_t = C(t) \mathbin{\dot{-}} C(t-1)$ (`saturating_sub`), conserving cumulative totals across discrete push ticks $\sum_{k=1}^{T} \Delta_k = C(T)$ whenever no tick baseline advances on a failed send.
- **Selective Delta `status_mask`**: Computes $\Delta_{p,s} = S_{p,s}(t) \mathbin{\dot{-}} S_{p,s}(t-1)$ across `(prev.status_mask | curr.status_mask)` and sets bit `s` in `delta.status_mask` **strictly if $\Delta_{p,s} > 0$**. Statuses observed only in past ticks are omitted automatically.
- **Gauges Pass-Through**: `active_streams` is copied verbatim as an instantaneous point-in-time `UpDownCounter` rather than differenced.

## 4. Pre-Baked Wire Templates & OS-Specialized Transport (`providers/`)

| Provider Module | Pre-Baked Memory State (`Template::new`) | Steady-State Serialization Path | Network / Kernel Execution |
| :--- | :--- | :--- | :--- |
| **`prometheus` (`template.rs`, `server.rs`)** | `PipelineSlotPrefixes` pre-compiles `[64 × 41]` OpenMetrics metric/label prefixes (`HELP`, `TYPE`, `internal_status="..."`). | `PrometheusTemplate::format_snapshot_into` copies static slices + `fast_write_u64_slice` into reusable `PooledBuffer` (`BufferPool`). | `acquire_shadow_tcp` HTTP/HTTPS server, `Semaphore(scrape.max_concurrency)`, dedicated auth-free `GET /healthz`. |
| **`otlp/proto` (`template.rs`)** | Pre-allocates `[64 × 41]` protobuf `PrebakedDpFragment` byte templates with baked timestamps (`ts_offset`) & value (`val_offset`) byte offsets. | `OtlpProtoTemplate::update_and_encode` writes 64-bit little-endian integer values in place and patches outer `ScopeMetrics`/`ResourceMetrics` varint lengths (bypasses `prost::encode` per tick). | Dispatches via `velda-etp::OtlpEtp` (HTTP/1.1 keep-alive `/v1/metrics` or native HTTP/2 gRPC `MetricsService/Export`). |
| **`otlp/json` (`template.rs`)** | Thread-local `OtlpJsonTemplate` pre-bakes metric envelopes, temporality scalar (`1`\|`2`), and `service.name` JSON object. | `format_into` writes 19-digit nanosecond timestamps via `fast_format_ts_19digits_simd` (SSE2/AVX2/`aarch64` SWAR fallback) and decimal values via `fast_write_u64_slice`. | Dispatches via `velda-etp::OtlpEtp` (`application/json`). |
| **`statsd` (`dogstatsd.rs`, `standard.rs`, `wire.rs`)** | `DogStatsdTemplate` (`#tag:val`) and `StandardTemplate` (dot suffixes) pre-bake line prefixes & suffix fragments at TLS init. | `StatsdBatchWriter` packs metric lines tightly into contiguous MTU chunks bounded by `max_packet_size` (`1432` default) with `(start, end)` slice descriptors. | `#[cfg(target_os = "linux")]`: stack `[libc::iovec; 64]` + `[libc::mmsghdr; 64]` via `libc::sendmmsg(64)`; non-Linux falls back to sequential `lease.send().await`. |

## 5. Multi-ETP Attachment Contract (`plugin.rs` & `providers/mod.rs`)

`MetricsExporterPlugin` bridges pull-only and multi-ETP configurations without exposing ETP details to Prometheus pull paths:
- `etp_ids()` returns only `config.etp_id` values for `TargetSlot::Push` entries (`TargetSlot::Pull` contributes nothing).
- `attach_etp(instance: &EtpInstance)` scans slots matching `config.etp_id == instance.id`, constructs an `EtpMetricsAttachment` validating `EtpBackend::Otlp` vs `EtpBackend::Statsd` (plus optional `etp_kind`), and stores it lock-free in a slot-local `OnceLock`. Multiple targets sharing the same `etp_id` bind safely from one `attach_etp` pass.
