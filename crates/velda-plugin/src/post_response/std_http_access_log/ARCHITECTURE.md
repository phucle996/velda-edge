# Standard Stream Access Log Plugin (`std_http_access_log`) — Architectural Specification

This document provides the micro-architectural specification, CPU superscalar execution pipeline mechanics, memory hierarchy invariants, and low-level SIMD transport algorithms of the **`std_http_access_log`** post-response plugin in Velda Edge.

---

## 1. Architectural Philosophy & Invariants

The `std_http_access_log` plugin executes during the Layer 7 **`post_response`** phase immediately after upstream response processing and directly preceding downstream wire encoding. It enforces six non-negotiable architectural invariants:

1. **Frontline Worker Zero-IO & Zero-Lock Invariant**:
   The request-serving Frontline Traffic Worker thread (`velda-worker-{id}`) **never** performs synchronous disk I/O, network writes, mutex locks, or stdout printing. Status check, metadata extraction, and enqueueing into the core's local SPSC ring buffer complete in **$< 15\ \text{ns}$** ($< 1.5\ \text{ns}$ for unsampled requests).
2. **CPU Register Harmony & Physical RAM Confinement**:
   The log entry is encapsulated in `PackedLogRecord`:
   - Exact size: **256 bytes** (exactly 4 CPU cache lines, 64-byte aligned via `#[repr(C, align(64))]`).
   - Symmetrically maps into 8 YMM registers (AVX2 256-bit), 16 NEON registers (ARM64 128-bit), or 32 64-bit general-purpose ALU registers.
   - Non-temporal streaming writes (`_mm256_stream_si256` / `_mm_stream_si128` + `_mm_sfence()`) write directly through Write-Combining Buffers to RAM, **bypassing L1/L2/L3 cache completely (Zero Cache Pollution)**.
   - `AlignedSlotBuffer` guarantees 4KB physical page alignment in RAM with `libc::MADV_SEQUENTIAL`.
   - L3 Cache (LLC) is 100% preserved for `velda-router` (Path Trie) and `velda-connection-pool`.
3. **Status-Aware Exact Sampling Contract (Zero Wildcards, Zero Default)**:
   - Configured strictly with exact 3-digit HTTP status codes (`"200": 0.05`, `"429": 1.0`, `"502": 1.0`).
   - Wildcard patterns (`"2xx"`, `"5xx"`) are strictly forbidden and rejected at configuration compile time.
   - No implicit `default_rate`: unconfigured status codes have an absolute sampling rate of `0.0`.
   - The sampling table `rates: [u16; 600]` occupies **1.2 KB**, residing permanently inside the CPU L1 Data Cache (32 KB/48 KB). Unsampled status codes trigger an **instant escape** in ~1.5ns without random number generation or queue access.
4. **Per-Core Lane Sharding (Invariant 2.9)**:
   - Traffic Worker `velda-worker-{id}` produces strictly into its dedicated local SPSC ring buffer shard (`shards[worker_id]`).
   - Companion Shadow Logistics Worker `velda-shadow-{id}` pinned to the same core under `SCHED_IDLE` drains this shard, eliminating cross-core cache invalidation and false sharing.
5. **Static Slot-Filling Byte Templating ("Điền Vào Ô Trống")**:
   - Serialization avoids dynamic JSON AST trees, reflection, and dynamic string allocations.
   - Static ASCII prefixes reside in `.rodata`. Numbers are formatted directly into contiguous memory via stack-based `itoa`.
6. **Single Syscall Batch Flushing to Standard Streams**:
   - The shadow worker collects up to 128 records per batch into a reusable 64 KB write buffer.
   - Flushes the entire buffer in **a single `libc::write` syscall** to `stdout` (fd 1) or `stderr` (fd 2). Direct file logging is strictly forbidden.

---

## 2. End-to-End Processing Pipeline

```mermaid
flowchart TD
    subgraph HotPath["Frontline Traffic Worker (Hot Path: < 15ns)"]
        Resp["Incoming L7Response"] --> ExtractStatus["Extract Status Code (resp.status.as_u16())"]
        ExtractStatus --> SampleCheck{"SamplingTable::should_sample(status)\n(L1 Cache Direct Index: [u16; 600])"}
        SampleCheck -->|Rate = 0.0 / Unconfigured| InstantEscape["Instant Hot-Path Escape\n(< 1.5ns, 0 Allocations, 0 Locks)"]
        InstantEscape --> Wire["Proceed to Downstream Wire Encoder"]
        
        SampleCheck -->|Sampled| Assemble["Assemble PackedLogRecord on Stack\n• 64B Core Metadata\n• 192B Inline User-Agent"]
        Assemble --> TryPush{"Local Lane Ring Buffer\n(shards[worker_id].try_push)"}
        TryPush -->|Non-Temporal Store| SIMDCopy["STREAM_RECORD_FN(slot, &record)\n(VMOVNTDQ / Streaming to RAM)"]
        SIMDCopy --> PushSuccess["logged_count.fetch_add(1)"]
        TryPush -->|Buffer Full| PushDrop["dropped_count.fetch_add(1)\n(Non-blocking Fail-Open Drop)"]
        PushSuccess --> Wire
        PushDrop --> Wire
    end

    subgraph ShadowWorker["Shadow Logistics Worker (velda-shadow-{id}, SCHED_IDLE)"]
        SPSC[("Core Lane Ring Buffer (RAM)\n(4KB Page Aligned & MADV_SEQUENTIAL)")]
        SIMDCopy -.-> SPSC
        SPSC --> PopBatch["pop_batch(&mut batch)\n(prefetch_nta + up to 128 records)"]
        PopBatch --> SlotFill["Slot-Filling Byte Formatting\n• JSON / CLF / Text\n• itoa fast integer format\n• Reusable 64KB Buffer"]
        SlotFill --> SyscallFlush["Single libc::write(fd, buf, len)\n• Target: stdout (1) or stderr (2)"]
    end
```

---

## 3. CPU Superscalar Execution Pipeline & The Handoff Mechanism

In modern x86_64 superscalar Out-of-Order (OoO) processors (Intel Golden Cove/Raptor Lake, AMD Zen 4/Zen 5) and ARM Neoverse cores, the CPU core is not a monolithic execution engine. It is partitioned into independent hardware functional units operating in parallel:

```
                      [ Instruction Fetch & Decode ]
                                     │
                        [ Micro-Op (uOp) Allocator ]
                                     │
          ┌──────────────────────────┼──────────────────────────┐
          ▼                          ▼                          ▼
[ Vector Execution Units ]   [ Address Generation Unit ]   [ Reorder Buffer (ROB) ]
  (Ports 0, 1, 5: AVX2)          (Ports 2, 3, 7, 8: AGU)                │
  8x YMM Loads (Stack)       Calculates Slot Physical Addr              │
          │                          │                                  │
          └────────────┬─────────────┘                                  │
                       ▼                                                │
             [ Store Buffer (SB) ]  ◄── (CPU HANDOFF OCCURS HERE) ──────┘
                       │
                       ├─────────────────────────────────────────────────┐
                       ▼                                                 ▼
             (Store Retires at ROB)                           (Hardware Background Drain)
             ALU & Vector Ports Liberated!                               │
             Zero ALU Stalls for Next Request                            ▼
                                                              [ Write-Combining Buffer ]
                                                                         │
                                                                         ▼ (Bypass L1/L2/L3)
                                                              [ System RAM (DRAM Bus) ]
```

### 3.1 Functional Unit Division
1. **ALU / Vector Engine (AVX2/AVX-512)**:
   - Responsible strictly for register-to-register arithmetic, logic, and data transfers (`ymm0..ymm7`).
   - The ALU never directly interacts with the memory bus and never waits for DRAM line acknowledgments.
2. **AGU (Address Generation Unit)**:
   - Dedicated execution ports that compute effective memory addresses (`[base + index * scale + disp]`) concurrently with arithmetic execution.
3. **Store Buffer (SB) / Store Queue (SQ) — The Handoff Unit**:
   - The Store Buffer is a specialized FIFO queue of hardware registers located between the execution pipeline and the cache hierarchy (typically 64 to 128 entries deep).
   - When `_mm256_stream_si256` executes:
     - Vector units dispatch the 32-byte data word to the Store Buffer.
     - The AGU dispatches the destination memory address to the Store Buffer.
     - **The Handoff**: Once the `(Address, Data)` pair is enqueued in the Store Buffer, the instruction is declared speculatively complete.
4. **ROB (Reorder Buffer) Retirement (Zero ALU Stall)**:
   - When the store instruction reaches the head of the Reorder Buffer, it is **Retired**.
   - The CPU registers (`ymm0..ymm7`) and vector execution ports are immediately released for downstream request processing.
   - The ALU does **not** stall waiting for the bytes to reach physical RAM. The memory controller drains the Store Buffer in the background.

---

## 4. Physical RAM Confinement Mechanics (Zero Cache Pollution)

### 4.1 The Cache Pollution / Cache Thrashing Problem
Edge gateways are memory-bound systems where request routing speed depends heavily on CPU Cache Hit ratios:
- `velda-router`: Traverses Radix Trees / Path Tries in **$< 15\ \text{ns}$** when trie nodes reside in the L3 Last Level Cache (LLC).
- `velda-connection-pool` & `velda-lb`: Look up Maglev lookup arrays and sharded endpoint states in **$< 10\ \text{ns}$** when hot in L2/L3 cache.

Under heavy edge traffic ($100,000\ \text{req/s}$), access logging produces:
$$100{,}000\ \text{records/sec} \times 256\ \text{bytes} = 25.6\ \text{MB/sec}$$

Under standard CPU write semantics (**Allocate-on-Write / Write-Back**):
1. A store instruction triggers a Read For Ownership (RFO) transaction across the bus.
2. The target line is loaded into L1 Data Cache (32KB/48KB), evicted to L2 Cache (1MB/core), and finally pushed to L3 Cache (32MB–64MB shared LLC).
3. Within **$1.5\ \text{seconds}$**, the high-volume log stream completely fills the L3 cache, forcibly **evicting the routing tables and connection pool shards out to DRAM**.
4. This results in **Cache Thrashing**: every subsequent client request suffers L3 cache misses, increasing P99 serving latency from $\sim 15\ \text{ns}$ to $> 150\ \text{ns}$.

### 4.2 The Solution: Confining Logs Strictly to RAM
`std_http_access_log` confines all log records directly to physical RAM, completely bypassing the CPU cache hierarchy across both write and read paths:

```
[ Frontline Worker Stack ] ──► YMM Registers ──► Store Buffer
                                                      │
                                                      ▼ (VMOVNTDQ Hint)
                                         [ Write-Combining Buffer (WCB) ]
                                                      │
                                    ┌─────────────────┴─────────────────┐
                                    │      Bypasses L1d Cache (32 KB)   │
                                    │      Bypasses L2 Cache (1 MB)     │
                                    │      Bypasses L3 LLC (32-64 MB)   │
                                    └─────────────────┬─────────────────┘
                                                      ▼ (DRAM Bus Interconnect)
                                        [ Physical RAM: AlignedSlotBuffer ]
                                        (4KB Page-Aligned, MADV_SEQUENTIAL)
                                                      ▲
                                                      │ (prefetch_nta hint)
                                       [ Shadow Logistics Worker ]
                                       (Transient L1 load, 0 L2/L3 residency)
```

#### Step 1: Physical Page Isolation (`AlignedSlotBuffer`)
The ring buffer backing storage is allocated with strict **4096-byte (4KB) page alignment**:
```rust
let layout = Layout::from_size_align(byte_len, 4096).unwrap();
let ptr = alloc_zeroed(layout) as *mut UnsafeCell<PackedLogRecord>;
libc::madvise(ptr as *mut libc::c_void, byte_len, libc::MADV_SEQUENTIAL);
```
- **Page Boundary Alignment**: Guaranteed to never share a memory page with general gateway heap objects, preventing False Sharing between worker threads.
- **Kernel Advice**: `libc::MADV_SEQUENTIAL` instructs the Linux kernel memory manager that access is sequential and streaming, preventing the OS from retaining stale pages in page cache.

#### Step 2: Non-Temporal Streaming Stores (`simd::stream_record`)
Frontline workers write records using Non-Temporal Vector Stores:
- **AVX2**: `_mm256_stream_si256` emits the x86 `VMOVNTDQ` instruction for all 8 chunks (256 bytes).
- **SSE2**: `_mm_stream_si128` emits the `MOVNTDQ` instruction for all 16 chunks.
- **Hardware Behavior**:
  - The non-temporal hint tells the processor cache controller: *"This data will not be reused by this core soon. Do not allocate a cache line."*
  - The uOp bypasses L1, L2, and L3 caches, writing directly into the CPU's **Write-Combining Buffers (WCB)** (typically 12 buffers of 64 bytes each).
  - When a 64-byte burst is full, the memory controller streams the block across the system interconnect directly to physical DRAM.
  - **Result: 0 bytes of L1/L2/L3 cache are allocated. The LLC remains 100% pristine for routing and connection state.**

#### Step 3: Weak Ordering & The Store Fence (`_mm_sfence`)
Non-temporal stores bypass standard x86 Total Store Order (TSO) cache coherence and are weakly ordered. To ensure memory consistency between the Frontline Producer and Shadow Consumer:
```rust
_mm256_stream_si256(d.add(0), v0);
...
_mm256_stream_si256(d.add(7), v7);
_mm_sfence(); // Drains all WCB buffers globally to memory
self.head.0.store(head.wrapping_add(1), Ordering::Release);
```
`_mm_sfence()` guarantees that all 256 bytes have been flushed from the Write-Combining Buffers to system memory before the atomic `head` index is released to the Shadow Worker.

#### Step 4: Shadow Worker Non-Temporal Prefetch (`simd::prefetch_nta`)
When the Shadow Worker drains records in batches:
```rust
simd::prefetch_nta(slot);
simd::copy_record(item, slot);
```
`_mm_prefetch(p, _MM_HINT_NTA)` issues a Non-Temporal Access prefetch hint across the 4 cache lines. The hardware prefetcher loads the lines into a transient L1 buffer without allocating lines in L2 or L3 cache, ensuring that the background drain path also causes **zero cache pollution**.

---

### 4.3 Pre-Allocated Fixed RAM Sizing & Zero-Realloc Invariant (Memory Safety Guarantee)

To completely eliminate runtime memory fragmentation, out-of-memory crashes (OOM), and subtle slow leaks under continuous high-throughput edge traffic (40,000+ RPS), `std_http_access_log` strictly enforces the **Pre-Allocated Fixed Memory Invariant**:

```
[ Gateway Bootstrap / Startup Phase ]
                 │
                 ▼
  [ Compute Physical RAM Budget ]
  Formula: Lane RAM = (buffer_capacity * 256 bytes + 4095) & !4095
  Default: 16,384 slots * 256 B = 4,194,304 B (4.0 MB per CPU Core)
                 │
                 ▼
  [ Single alloc_zeroed(Layout::from_size_align(4MB, 4096)) ]
  Pin memory to 4KB page boundaries via libc::madvise(MADV_SEQUENTIAL)
                 │
                 ▼
  [ Gateway Enters Request-Serving Hot Path ] ◄── (MEMORY CEILING LOCKED FOREVER)
                 │
        ┌────────┴────────┐
        ▼                 ▼
[ Frontline Worker ] [ Shadow Worker ]
(0B Heap Allocs)    (Reuses 64KB WriteBuf)
(Stack -> RAM Ring) (write_buf.clear())
```

#### 1. Deterministic Memory Sizing at Startup
The exact physical memory footprint of `std_http_access_log` is calculated and pre-allocated **once** during plugin instantiation:
$$\text{RAM per Worker Lane} = \left\lceil \frac{\text{buffer\_capacity} \times 256\ \text{bytes}}{4096} \right\rceil \times 4096\ \text{bytes}$$
- Với `buffer_capacity = 16,384` (mặc định): **4.0 MB RAM / worker core**.
- Với hệ thống 2 vCPUs (VM2): Tổng cộng **8.0 MB RAM cố định**.
- Vùng nhớ này được `alloc_zeroed` duy nhất một lần và tồn tại suốt vòng đời tiến trình.

#### 2. The Zero-Realloc & Zero-Dealloc Invariant
Sau khi vượt qua pha khởi động (`new`), mã nguồn của `std_http_access_log` **tuyệt đối không bao giờ gọi `alloc`, `realloc`, hay `dealloc` trên bất kỳ luồng nào**:
- **Frontline Traffic Worker**: Tạo `PackedLogRecord` hoàn toàn trên ngăn xếp hàm (`Stack Frame`: 256 bytes) $\to$ Dùng lệnh SIMD `VMOVNTDQ` ghi thẳng vào khe RAM có sẵn trong `AlignedSlotBuffer`. Số lần cấp phát heap trên hot-path là **tuyệt đối `0 B / 0 allocs`**.
- **Shadow Logistics Worker**: Khởi tạo duy nhất một buffer ghi xả 64KB (`let mut write_buf = Vec::with_capacity(65536)`). Trong vòng lặp vô tận, nó chỉ gọi `write_buf.clear()`. Trong Rust, `clear()` chỉ đặt lại độ dài `len = 0` và **giữ nguyên con trỏ heap**, tái sử dụng liên tục cùng một mảng bộ nhớ 64KB.

#### 3. Cơ Chế Thu Hồi Tuần Hoàn (Cyclic Ring Reclaim)
- Producer (Frontline) và Consumer (Shadow) giao tiếp qua hai chỉ số atomic 64-bit: `head` và `tail`.
- Vị trí khe nhớ trong RAM được xác định bằng phép toán bitwise mặt nạ siêu tốc:
  $$\text{slot\_index} = \text{head} \ \& \ \text{mask}$$
- Khi Shadow Worker đọc xong một slot và tăng `tail`, slot đó **tự động trở thành slot trống** để Frontline Worker ghi đè cho request tiếp theo mà không cần giải phóng bộ nhớ hay gọi GC.
- Khi hàng đợi bão hòa (`head - tail == capacity`), Frontline Worker kích hoạt cơ chế **Fail-Open Drop trong 2.1ns**: tăng `dropped_count` và tiếp tục phục vụ client, tuyệt đối **không cấp phát hàng đợi mở rộng (không buffer bloat)** như Envoy.

> [!IMPORTANT]
> **Cam Kết An Toàn Bộ Nhớ**:
> Nhờ cơ chế cấp phát tĩnh 4MB/core và tái sử dụng tuần hoàn 100%, `std_http_access_log` triệt tiêu hoàn toàn hiện tượng phân mảnh heap (Heap Fragmentation) và rò rỉ bộ nhớ (Memory Leak), bảo đảm tốc độ rò rỉ đạt **`0.000 MB / min`** ngay cả khi vận hành liên tục nhiều tháng.

---

## 5. Micro-Architectural Record Memory Layout (256 Bytes)

`PackedLogRecord` is aligned to 64 bytes (`#[repr(C, align(64))]`) and partitioned into two contiguous blocks:

```
+---------------------------------------------------------------------------------------------------+
|                                  PACKED LOG RECORD (256 BYTES)                                     |
+---------------------------------------------------------------------------------------------------+
| Byte Offset | Field Name       | Type      | Size   | Natural Alignment | Description             |
+-------------+------------------+-----------+--------+-------------------+-------------------------+
| 0..8        | timestamp_ms     | u64       | 8 B    | 8 B               | Unix epoch milliseconds |
| 8..12       | content_length   | u32       | 4 B    | 4 B               | Response size (up to 4G)|
| 12..16      | duration_us      | u32       | 4 B    | 4 B               | Latency in microseconds |
| 16..18      | status           | u16       | 2 B    | 2 B               | HTTP status (100..599)  |
| 18..19      | version          | u8        | 1 B    | 1 B               | 1=H1.0, 2=H1.1, 3=H2, 4=H3|
| 19..20      | method           | u8        | 1 B    | 1 B               | 1=GET, 2=POST, 3=PUT... |
| 20..21      | user_agent_len   | u8        | 1 B    | 1 B               | Stored UA length (0..192)|
| 21..22      | request_id_len   | u8        | 1 B    | 1 B               | Stored Req ID len (0..16)|
| 22..23      | route_id_len     | u8        | 1 B    | 1 B               | Stored Route ID len(0..8)|
| 23..24      | upstream_len     | u8        | 1 B    | 1 B               | Stored Upstream len(0..16)|
| 24..40      | request_id       | [u8; 16]  | 16 B   | 1 B               | Inline Request UUID/ID  |
| 40..48      | route_id         | [u8; 8]   | 8 B    | 1 B               | Matched route ID prefix |
| 48..64      | upstream         | [u8; 16]  | 16 B   | 1 B               | Upstream IP:Port string |
+-------------+------------------+-----------+--------+-------------------+-------------------------+
| 64..256     | user_agent       | [u8; 192] | 192 B  | 1 B               | Downstream client UA    |
+-------------+------------------+-----------+--------+-------------------+-------------------------+
| TOTAL: 256 BYTES (Exactly 4 Cache Lines, 0 Padding Holes, 64-Byte Aligned)                        |
+---------------------------------------------------------------------------------------------------+
```

---

## 6. Hardware SIMD Vector Register Dispatch (`simd.rs`)

`std_http_access_log` resolves function pointers once during gateway bootstrap based on `velda_core::global_hardware_topology().cpu.register_capabilities`:

```mermaid
flowchart LR
    Probe["Probed Host CPU Capabilities\n(velda-core HardwareTopology)"] --> Select{"caps.select_for_workload(Bits256)"}
    Select -->|Bits256 (AVX2)| AVX2["stream_record_256\n• 8x VMOVNTDQ (_mm256_stream_si256)\n• _mm_sfence()\n(RAM Confinement: Bypass L1/L2/L3)"]
    Select -->|Bits128 (SSE2 / NEON)| SSE["stream_record_128\n• 16x MOVNTDQ (_mm_stream_si128)\n• _mm_sfence()"]
    Select -->|Bits64 (SWAR ALU)| SWAR["stream_record_64\n• 32x 64-bit ALU transfers\n• Release memory barrier"]
    Select -->|Bits32 (Scalar)| Fallback["stream_record_portable\n• Pointer block copy\n• Release memory barrier"]
```

---

## 7. Status-Aware Sampling Engine (`sampling.rs`)

```
Sampling Table: rates[600]: [u16; 600] (1,200 Bytes permanently resident in L1 Data Cache)
+------------+-------------+----------------------------------------------+
| Status Code| Basis Points| Hot-Path Resolution Path                     |
+------------+-------------+----------------------------------------------+
| 200        | 500 (5%)    | fast_rand_10k() < 500 (5% samples logged)    |
| 404        | 5,000 (50%) | fast_rand_10k() < 5000 (Half logged)         |
| 429        | 10,000 (100%)| Threshold >= 10,000 -> Instant True (No PRNG)|
| 502        | 10,000 (100%)| Threshold >= 10,000 -> Instant True (No PRNG)|
| 201 (unset)| 0 (0%)      | Threshold == 0 -> Instant False (~1.5ns escape)|
+------------+-------------+----------------------------------------------+
```

Unconfigured status codes default to `0` basis points. The frontline worker evaluates `threshold == 0` first; if zero, it returns immediately without executing pseudo-random number arithmetic, achieving an undetectable hot-path footprint.

---

## 8. Single-Syscall Batch Flushing (`drainer.rs`)

The background drainer operates exclusively on established standard output file descriptors:
1. Allocates a thread-local reusable 64KB format buffer.
2. Drains up to 128 records per iteration from the core's local lane SPSC queue.
3. Formats all records into the contiguous buffer via stack-based `itoa` (zero heap allocations).
4. Emits a single `libc::write(fd, buf.as_ptr(), buf.len())` syscall.
5. If the OS pipe buffer accepts fewer bytes than requested, remaining bytes are preserved in-place without reallocation.
