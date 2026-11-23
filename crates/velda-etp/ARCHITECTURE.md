# velda-etp — Hardware & Concurrency Architecture

This document specifies the memory layout, CPU cache hierarchy mechanics, cold-path connection blueprints, and formal concurrency invariants of `velda-etp`.

---

## 1. Vectorized 1-Bit Reference Counting vs Standard `Arc<T>`

For exclusive stream connections (`EtpStream`) and datagram sockets (`UdpSocket`) pooled inside `EtpPageArena<C>`, each slot's borrow count is strictly binary ($\in \{0, 1\}$):

| Metric | Standard Heap `Arc<Conn>` (64 Slots) | `EtpPageArena` 1-Bit Ref Count (64 Slots) |
| :--- | :--- | :--- |
| **Ref-Count Metadata Size** | $64 \times 16\text{ B} = 1,024\text{ B}$ (`strong` + `weak`) | **$8\text{ B}$ (`1` `AtomicU64` in Cache Line 0)** |
| **Cache Lines Touched to Find Free Slot** | Up to $64$ separate heap cache lines ($O(N)$ pointer chase) | **$1$ L1D cache line ($O(1)$ via hardware `TZCNT`)** |
| **Claim / Release Instruction** | `lock xadd` + `isize::MAX` overflow branch + `weak` check | **`lock btr` / `lock or` (0 overflow branch)** |
| **Lease Handle Footprint** | $40\text{ B}$ (with `OwnedSemaphorePermit`) | **$24\text{ B}$ (`assert!(size_of::<<Kind>Lease>() == 24)`)** |

Because `trailing_zeros()` (`tzcnt` on `x86-64-v3`, `rbit + clz` on `aarch64`) always selects the lowest set bit (`Slot[0]`, then `Slot[1]`, ...), `EtpPageArena` naturally enforces **LIFO / MRU cache affinity**: under normal load, `Slot[0]` is repeatedly borrowed and returned, keeping its memory cache line and TCP/TLS session hot while higher slots remain untouched.

---

## 2. Two-Tier Cache Hierarchy (L1D Control Line + L2 Connection Slots) & AGU Addressing

```text
  acquire() / try_acquire_ready_slot()
        │
        ▼
  [1] prefetch_l1_control(base)  ──► _MM_HINT_T0 / prfm pldl1keep on Cache Line 0 (offset 0)
        │
        ▼
  [2] tzcnt rcx, rdx             ──► Finds lowest ready bit idx in 1 cycle
      lock btr [ready_bitmap], idx
      lock or  [busy_bitmap],  1 << idx
        │
        ▼
  [3] shl idx, 6                 ──► 1-cycle shift (64 bytes per ArenaSlot<C>)
      prefetcht0 [base + (idx << 6) + 64]
        │                            ▲
        │                            └── CPU AGU folds (+ base) and (+ 64) for free!
        ▼
  [4] Pulls ONLY Slot[idx] from L2 Cache into L1D Cache; returns <Kind>Lease(idx)
```

### Compile-Time Geometry Assertions
- `size_of::<ArenaControlLine>() == 64` and `align_of::<ArenaControlLine>() == 64` (Cache Line 0 = 512 bits).
- `size_of::<ArenaSlot<EtpStream>>() == 64` and `size_of::<ArenaSlot<tokio::net::UdpSocket>>() == 64` (1 Cache Line per connection).
- `MAX_BASELINE_SLOTS = 127` and `MAX_ARENA_SLOTS = 128`, guaranteeing `total_slots > baseline_slots` so the contiguous `+ 4096`-byte spare page always provides at least one burst spillover slot bit.

### Shared-Core Frontline Protection
Shadow Workers (`velda-shadow-{id}`, `SCHED_IDLE`) share physical CPU cores with Frontline Traffic Workers (`velda-worker-{id}`). When a Shadow Worker wakes up to flush telemetry or sync Redis via `velda-etp`:
- Idle slots stay in **L2 Cache** (`_MM_HINT_T1` / `pldl2keep` issued on `release_healthy_slot` and `finish_warm_slot`).
- An active flush touches only **2 cache lines (128 bytes)**: Cache Line 0 (`ArenaControlLine`) and the single leased `Slot[idx]`, preventing L1D cache eviction of the Frontline Worker's `Router` and `HeaderWorkspace` tables.

---

## 3. Cold Connect Blueprint (`connect.rs`)

All connection-establishment state is pre-compiled once at ETP instantiation time into a cache-line-aligned cold struct (`#[repr(align(64))] ColdConnectBlueprint`), isolated from `EtpPageArena`:

```text
  EtpEntryConfig (etp.bin)
        │
        ├──► ColdTargetAddr::parse(host, port)
        │      ├─► IP Literal ("10.244.1.15") ──► velda_core::lut::ip::parse_ip_u128
        │      │                                  ──► ColdTargetAddr::Ip { ip_u128, port_be, addr }
        │      └─► Domain ("redis.svc.cluster.local")
        │            ──► ColdTargetAddr::Dns { cached_ip_seq, cached_ip_lo, cached_ip_hi }
        │                  ├── First connect resolves DNS -> freezes 128-bit IP via SeqLock
        │                  ├── Subsequent connects dial frozen integer ip_u128 directly (0 DNS)
        │                  └── Re-queries DNS only if TCP connect to frozen IP fails (Pod rotation)
        │
        └──► EtpTlsConfig::build_client_config()
               ├─► Multi-Generation Protocol Selection: Auto (TLS 1.3 + 1.2) | Tls12 | Tls13
               ├─► In-Memory Session Cache: Resumption::in_memory_sessions(64)
               │     (Slot[0] handshake caches PSK ticket -> Slot[1..N] & reconnect() resume)
               └─► Certificate Verification Priority:
                     1. insecure_skip_verify == true ──► InsecureServerCertVerifier
                     2. pinned_cert_sha256: [u64; 4] ──► PinnedSha256ServerCertVerifier
                     │    (Zero-alloc stack-only sha256_u64x4 + constant-time 4x u64 XOR/OR)
                     3. ca_cert_pem / webpki_roots   ──► WebPkiServerVerifier (+ optional mTLS)
```

---

## 4. Formal Concurrency & Cancellation Invariants

1. **Async Cancellation Safety (`ClaimedSlotGuard<'a, C>`)**:
   - Whenever `claim_empty_slot()` sets `busy_bitmap[idx] = 1`, the returned `slot_idx` is immediately owned by `ClaimedSlotGuard`.
   - Likewise, `acquire()` holds a borrowed stack `SemaphorePermit<'_>` across `connect_fresh().await`.
   - If the calling task times out or is cancelled at any `.await` point before `commit_leased()` / `commit_warm()` is reached, `ClaimedSlotGuard::drop` clears `busy_bitmap[idx] = 0` and `SemaphorePermit::drop` restores the semaphore permit. Zero bits or permits can leak under arbitrary task cancellation.

2. **100% Initialized Lease Invariant (`replace_leased_slot`)**:
   - A `<Kind>Lease` is only constructed after `Slot[idx]` has been initialized (`init_bitmap[idx] = 1`).
   - During `lease.reconnect().await`, the new connection is established (`connect_fresh().await?`) **before** overwriting `*slot.conn.assume_init_mut() = fresh` in-place via `replace_leased_slot()`.
   - Even if `reconnect().await` fails or is cancelled mid-flight, `Slot[idx]` still holds a valid initialized `C` for `Drop` to clean up (`discard_broken_slot`). Consequently, `Deref`, `DerefMut`, and `Drop` require **zero runtime `init_bitmap` loads or assertions**, compiling `DerefMut` down to a single AGU instruction `lea rax, [base + (idx << 6) + 64]`.

3. **Torn-Read-Free 128-Bit IPv6 LKG Freeze (`SeqLock`)**:
   - `ColdTargetAddr::Dns` protects the 128-bit `(cached_ip_lo, cached_ip_hi)` pair with an odd/even sequence counter (`cached_ip_seq: AtomicU64`).
   - Readers retry if `seq` is odd or changes between `seq1` and `seq2`, preventing torn 128-bit IPv6 reads across concurrent worker threads without locks.
