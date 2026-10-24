# velda-core Buffer & Memory Architecture Model

This document specifies the user-space buffer allocation models, pool management algorithms, physical memory pre-warming mechanics, and zero-allocation hot-path invariants in `crates/velda-core/src/buffer/`.

---

## 1. Problem Statement: Memory Bottlenecks at Edge Scale

In a multi-gigabit edge proxy serving $> 100,000$ RPS per node:
1. **Dynamic Memory Allocator Contention (`malloc` / `jemalloc`)**: Allocating and freeing dynamic buffers (`Vec<u8>`, `HeaderMap`) per request causes mutex lock contention, arena fragmentation, and CPU cache eviction across worker threads.
2. **Minor Page Faults (MMU TLB Overhead)**: When a process allocates new memory, the OS kernel lazily allocates virtual addresses. On the first write, the CPU triggers a **Minor Page Fault** to map a physical page frame in the MMU Page Table Entries (PTE), causing pipeline stalls ($1\text{--}5\,\mu\text{s}$ per fault).
3. **Out-of-Memory (OOM) Kill Under Burst Spikes**: Naive object pools without hard caps grow unboundedly during traffic micro-bursts, causing the Linux Kernel OOM killer to terminate the edge process (`SIGKILL 9`).

---

## 2. Pre-Warming & Minor Page Fault Elimination

To guarantee that request processing never halts on memory mapping, `velda-core` eagerly commits physical pages at cold-start bootstrap:

### 2.1 Eager Physical Page Touching
Both [`HeaderPool`](src/buffer/header.rs) and [`PayloadSlabPool`](src/buffer/slab.rs) execute write-touching during cold-start allocation:

```rust
// Cold-start allocation inside PayloadSlab::new():
let page_size = 4096; // Standard Linux x86_64 / aarch64 page
let mut offset = 0;
while offset < size {
    buf[offset] = 0; // Force MMU to create physical PTE mapping upfront
    offset += page_size;
}
```

```rust
// Cold-start allocation inside HeaderWorkspace::new():
map.reserve(capacity); // Forces internal hash table allocation in jemalloc
```

**Result**: 100% of physical page frames for baseline traffic are resident in RAM before the first client connection is accepted. Traffic workers run with **Zero Minor Page Faults**.

---

## 3. Two-Tier Adaptive Memory Management

Instead of statically fixing buffer counts or allowing unbounded heap growth, pools in `velda-core` operate on a **Two-Tier Adaptive Life Cycle**:

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      TRAFFIC WORKER REQUEST LEASE                      │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ acquire()
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ TIER 1: HOT BASELINE FLOOR (Always Resident in RAM)                    │
 │ - Pre-warmed during bootstrap. Never evicted or freed.                 │
 │ - Accommodates 100% of steady-state traffic with zero allocation.      │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ Baseline exhausted (Traffic Burst)
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ TIER 2: BURST STANDBY QUEUE (15s Cooldown Window)                      │
 │ - Spills over dynamically up to Hardware-Sized max_burst_cap.          │
 │ - Returned slabs/workspaces are stamped with Instant::now().           │
 │ - Reused across multi-wave micro-bursts without malloc/free thrashing. │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ Inactive > 15s (Auxiliary Sweeper)
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ LINUX RSS DIRTY RETENTION (jemalloc Decay Window)                      │
 │ - Sweeper returns excess buffers back to the user-space allocator.     │
 │ - Pages remain resident in physical RSS during the decay window,       │
 │   accelerating any subsequent re-expansion without kernel page faults. │
 └────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Tier 1: Hot Baseline Floor
- Sized directly from continuous hardware probing via [`crate::hardware::sizing::HardwareSizing`].
- Workspaces and slabs in Tier 1 reside in memory for the entire life of the edge process.

### 3.2 Tier 2: Burst Standby Queue with 15s TTL
- When steady-state baseline is exhausted during a sudden traffic spike, the pool leases additional workspaces up to `max_burst_cap`.
- When burst leases are dropped, they are returned to `burst: VecDeque<(Instant, T)>` stamped with the return timestamp.
- **Why 15 Seconds?**: Traffic spikes typically arrive in multi-wave ripples (e.g. client retries, microservices cascading requests). Keeping burst buffers alive for 15 seconds prevents destructive allocator thrashing (`malloc`/`free` cycles on every wave).

### 3.3 Non-Blocking Auxiliary Sweeper (`try_lock`)
Eviction of expired burst buffers is strictly decoupled from request traffic:
- Driven by a background timer on an **Auxiliary Supervisor Core**.
- Executes `guard.try_lock()`:
  - If a Traffic Core is currently acquiring or releasing a buffer, the sweeper **immediately skips that tick**.
  - **Zero Traffic Worker Lock Contention**: A traffic worker never waits on a background garbage collection sweep.

---

## 4. Object Pools Owned by `velda-core`

### 4.1 `HeaderPool` (`src/buffer/header.rs`)
Manages pre-allocated [`HeaderWorkspace`] instances:
- **Baseline Allocation**: Defaults to 32 headers capacity per workspace, pre-allocated with native hash bucket arrays.
- **Pre-Loaded Slot 0**: Every leased or recycled workspace is pre-warmed with `(x-request-id, <id>)` in Slot 0 with `Bit 1 - Bypass on Reject` visibility.
- **Reset Invariant**: On drop, [`HeaderWorkspace::reset`] calls `clear()` (retaining bucket table capacity) and shrinks only if an adversarial request bloated the table past $4\times$ baseline.

### 4.2 `PayloadSlabPool` (`src/buffer/slab.rs`)
Manages contiguous byte slices for body reading and streaming pipes across three canonical slab size classes:

| Slab Class | Size (Bytes) | Intended Workload |
| :--- | :--- | :--- |
| `Small4K` | 4,096 | REST JSON payloads, auth tokens, small RPC messages |
| `Medium16K` | 16,384 | Standard API responses, streaming pipe chunk buffers |
| `Large64K` | 65,536 | File uploads, large downloads, media chunks |

- Slabs $> 64\text{ KB}$ bypass the pool and are allocated dynamically, protecting the pool from multi-megabyte payload bloat.

---

## 5. Defensive Memory Invariants (Anti-UAF & Anti-Double-Free)

[`HeaderWorkspaceLease`](src/buffer/header.rs) and [`PayloadSlabLease`](src/buffer/slab.rs) are RAII wrappers enforcing strict compile-time and runtime safety:

1. **Zero Use-After-Free via Compiler Borrow Checker**:
   `Deref` and `DerefMut` implementations return references `&'a HeaderWorkspace` or `&'a mut [u8]` tied directly to the lifetime of the lease struct. The Rust compiler statically rejects any attempt to keep references after the lease is dropped.
2. **Zero Double-Free via `Option::take()`**:
   Inside `Drop`, the lease extracts the inner workspace/slab via `self.workspace.take()`. Even under panics or repeated drop handlers, the resource can only be returned to the pool exactly once.
3. **Async Task Cancellation Safety**:
   When downstream clients abort or Tokio drops a request future mid-await, Rust's stack unwinding invokes `Drop`, safely returning the buffer to the pool without leaks or dangling references.
