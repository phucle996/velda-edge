# Standard Stream Access Log Plugin (`std_http_access_log`)

Hardware-harmonized, zero-allocation request access logging plugin evaluated during the Layer 7 `post_response` phase. Encapsulates HTTP response metadata, canonical ingress headers, and client User-Agent into an ultra-compact 256-byte cacheline-aligned POD record (`PackedLogRecord`), enqueueing entries into per-core lock-free ring buffers via CPU vector registers for asynchronous single-syscall batch draining directly to standard output streams (`stdout` or `stderr`).

---

## 1. What is `std_http_access_log`?

`std_http_access_log` is Velda Edge's canonical access logging engine. Operating in the `post_response` phase immediately following upstream response processing, it captures complete transaction audit logs without inducing hot-path latency, without allocating heap memory, and without polluting CPU caches.

Unlike traditional proxy loggers that rely on synchronous file I/O, coarse-grained mutexes, or heavy format string interpolations, `std_http_access_log` is architected from the ground up for **CPU Register Harmony** and **Physical RAM Confinement**.

---

## 2. Core Purpose & What It Does

1. **Transaction Telemetry Extraction**:
   Extracts response status code, wire payload length, request latency in microseconds (`latency_us`), upstream backend address (`x-upstream-addr`), request UUID (`x-request-id`), route ID prefix, and downstream client User-Agent (up to 192 bytes inline).
2. **Status-Aware Exact Sampling**:
   Applies deterministic, per-status-code sampling rates (`100..=599`). High-frequency success traffic (e.g. `200 OK`) can be sampled down to $1\%$, while critical errors (`500`, `502`, `503`) and security rejections (`429`, `401`, `403`) are captured at $100\%$. Unsampled requests escape in **$< 1.5\ \text{ns}$**.
3. **Structured Batch Output**:
   Formats records via zero-allocation static slot filling into structured **JSON**, **CLF** (Nginx Combined), or key-value **Text**, flushing up to 128 records per batch in a single `libc::write` syscall directly to `stdout` (fd 1) or `stderr` (fd 2).

---

## 3. Core Architectural Concept ("Ý Tưởng Cốt Lõi")

The design of `std_http_access_log` is guided by three hardware-level insights:

```
[ Downstream Client ]
         │
         ▼
[ Frontline Traffic Worker (velda-worker-{id}) ]
  │
  ├─► Sampling Check (L1 Cache Table) ──(Unsampled)──► Hot-Path Escape (< 1.5ns)
  │
  └─► Sampled: Assemble `PackedLogRecord` on Stack (256B)
        │
        ▼
   CPU HANDOFF: ALU/Vector Engine ──► Store Buffer (SB) ──► ROB Retire (Zero ALU Stall)
        │
        ▼ (Non-Temporal Streaming Store: VMOVNTDQ)
   BYPASS L1 / L2 / L3 CACHE ──► GIAM Ở RAM (4KB Page Aligned AlignedSlotBuffer)
                                      │
                                      ▼
             [ Shadow Logistics Worker (velda-shadow-{id}, SCHED_IDLE) ]
                                      │
                                      ├─► prefetch_nta() (Đọc RAM không ô nhiễm L2/L3)
                                      ├─► Slot-filling byte formatting (Zero allocation)
                                      └─► libc::write() ──► stdout / stderr (fd 1 / fd 2)
```

### 3.1 Hardware Separation: Frontline Worker vs. Shadow Worker (Invariant 2.9)
- **Frontline Traffic Worker (`velda-worker-{id}`)**: Exclusively serves downstream traffic on the request hot path. It never writes to files, never locks stdout mutexes, and never blocks.
- **Shadow Logistics Worker (`velda-shadow-{id}`)**: Pinned companion thread running under `SCHED_IDLE` on the same CPU core. Solely responsible for batch draining, byte formatting, and flushing to standard output streams.

### 3.2 CPU Handoff via Store Buffer (Zero ALU Stall)
In superscalar Out-of-Order execution, the CPU's ALU and Vector Execution Units prepare the 256-byte vector data and hand off `(address, data)` pairs to the hardware **Store Buffer (SB)**. The store instruction retires immediately at the Reorder Buffer (ROB). The ALU is liberated within 4 clock cycles to calculate downstream wire encodings for subsequent requests, completely decoupled from memory latency.

### 3.3 RAM Confinement: Protecting L3 Cache (Zero Cache Pollution)
In an edge gateway processing 100,000 req/s, access logging generates over **$25.6\ \text{MB/s}$** of telemetry data. Under standard cache allocation, this telemetry stream would continuously evict the Radix Tree routing tables of `velda-router` and the connection state of `velda-connection-pool` from the Last Level Cache (L3 LLC).

`std_http_access_log` eliminates cache pollution by **confining log records to physical RAM**:
1. **`AlignedSlotBuffer`**: Allocated with 4096-byte (4KB) page alignment directly in RAM, isolated from general heap structures, and hinted with `libc::MADV_SEQUENTIAL`.
2. **Non-Temporal Stores (`_mm256_stream_si256` / `VMOVNTDQ`)**: Records are written directly to RAM via Write-Combining Buffers, **bypassing L1, L2, and L3 caches completely**.
3. **Non-Temporal Prefetch (`_mm_prefetch(..., _MM_HINT_NTA)`)**: When the Shadow Worker reads records from RAM, it prefetches into a transient buffer without polluting L2/L3 cache lines.

---

## 4. Non-Negotiable Invariants

| Invariant | Specification | Enforcement |
| :--- | :--- | :--- |
| **Hot-Path Zero-IO** | Frontline worker never performs disk I/O, socket I/O, or stdout printing. | Verified via static analysis & hot-path benchmark ($< 15\ \text{ns}$). |
| **Status-Aware Exact Sampling** | Only exact 3-digit status codes (`100..=599`). Wildcards (`"2xx"`, `"5xx"`) and implicit defaults are strictly forbidden. | Rejected at configuration validation time. Unconfigured status = 0.0. |
| **Direct Standard Streams Only** | Target must be `"stdout"` or `"stderr"`. Direct file logging (`"target": "file"`) is strictly forbidden. | Config compiler rejects non-stream targets. |
| **Single Ingress Enrichment** | Proxy headers (`x-forwarded-*`, `x-real-ip`) are enriched once at ingress. `std_http_access_log` only reads authoritative workspace headers. | Enforced by architecture boundary. |
| **256-Byte Cacheline Harmony** | `PackedLogRecord` is exactly 256 bytes (4 cache lines, `align(64)`), zero padding holes. | Verified via unit tests (`test_record_memory_layout`). |
| **Non-Blocking Fail-Open Drop** | When the per-core ring buffer saturates under downstream backpressure, excess records are dropped instantly. Client requests are never throttled. | Lock-free queue push returns `Err(())` fail-open. |
| **Static Pre-Allocation & Zero-Realloc** | Buffer slots (4.0 MB/core) pre-allocated in RAM at startup. Zero dynamic heap allocation (`0 B / 0 allocs`) and zero runtime realloc on serving path. | Enforced by `AlignedSlotBuffer` & cyclic index reclaim (`0.000 MB/min` leak). |

---

## 5. Documentation Navigation

- **[OPERATOR & CONFIGURATION RUNBOOK](RUNBOOK.md)**: Full JSON configuration schema, production sampling recipes, Kubernetes log shipping (Vector / FluentBit), memory sizing, and operational alerts.
- **[ARCHITECTURAL SPECIFICATION](ARCHITECTURE.md)**: Deep dive into superscalar CPU execution pipelines, Store Buffer handoff, non-temporal cache bypass mechanics, and SIMD vector transfers.
- **[BENCHMARK & PERFORMANCE SUITE](BENCHMARK.md)**: Micro-benchmarks, latency distributions ($p50, p99, p99.9$), and memory throughput comparisons against Envoy and Nginx.
- **[TEST SUITE SPECIFICATION](TEST.md)**: Unit tests, integration test suites, fuzzing test cases, and chaos failure scenarios.
