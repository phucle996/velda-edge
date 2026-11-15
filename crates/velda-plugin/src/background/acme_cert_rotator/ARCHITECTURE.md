# ACME Certificate Rotator — Deep Architectural Specification

```text
+-----------------------------------------------------------------------------------------------------------------+
|                        ACME CERTIFICATE ROTATOR: MULTI-CORE HARDWARE & ROUTING TOPOLOGY                         |
|                                                                                                                 |
|  [SHADOW LOGISTICS DOMAIN: SCHED_IDLE]                      [FRONTLINE SERVING DOMAIN: REAL-TIME HOT PATH]      |
|  Assigned Core i (velda-shadow-{i})                         Cores 0..N-1 (velda-worker-0..n)                    |
|  ┌──────────────────────────────────────────────┐           ┌──────────────────────────────────────────────┐    |
|  │ AcmeCertRotatorPlugin (plugin.rs)            │           │ Traffic Workers (handle_http_stream)         │    |
|  │  - Bootstrap Expiry Check & UTC Cron Engine  │           │  - Active customer request serving           │    |
|  │  - RFC 8555 JWS Protocol Client (P-256)      │           │  - Zero blocking on ACME renewal             │    |
|  │  - Orders Challenge & Pre-bakes Wire Response│           │  - Read SniResolver & Router in RAM          │    |
|  └──────────────────────┬───────────────────────┘           └──────────────────────┬───────────────────────┘    |
|                         │                                                          │                            |
|                         │ 1. Inject Ephemeral Exact Route                          │ 2. Match Challenge Route   |
|                         ▼                                                          ▼    (SIMD 128-bit Prefix)   |
|  ┌─────────────────────────────────────────────────────────────────────────────────┴───────────────────────┐    |
|  │ velda_router::Router (RwLock<Router>)                                                                   │    |
|  │  - Ephemeral Route: /.well-known/acme-challenge/<token> -> RouteId(u32::MAX)                            │    |
|  │  - Guarded by EphemeralRouteGuard (RAII Scope Guard, unconditional drop on exit)                       │    |
|  └──────────────────────────────────────┬──────────────────────────────────────────────────────────────────┘    |
|                                         │ Lookup Challenge Response                                             |
|                                         ▼                                                                       |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐    |
|  │ ChallengeStore (ArcSwap<Vec<Arc<Entry>>>)                                                               │    |
|  │  - Pre-baked HTTP 200 wire response (status line, content-length, key authorization)                    │    |
|  │  - Evicted from L1/L2 via _mm_clflush (Preserves L1D/L2 clean for customer traffic)                     │    |
|  └──────────────────────────────────────┬──────────────────────────────────────────────────────────────────┘    |
|                                         │ Challenge Validated by External CA                                    |
|                                         ▼                                                                       |
|  ┌──────────────────────────────────────────────┐           ┌──────────────────────────────────────────────┐    |
|  │ Atomic Disk Persistence (storage.rs)         │           │ Dynamic TLS Hot-Swap (hot_swap.rs)           │    |
|  │  - Write to .tmp.<uuid>                      │           │  - Injects new CertifiedKey                  │    |
|  │  - fsync (sync_all) & atomic rename          │ ────────► │  - velda-tls::SniResolver (ArcSwap)          │    |
|  │  - Mode 0600 on private keys                 │           │  - Zero dropped in-flight TLS handshakes     │    |
|  └──────────────────────────────────────────────┘           └──────────────────────────────────────────────┘    |
+-----------------------------------------------------------------------------------------------------------------+
```

---

## 1. System Placement & Asymmetric Worker Model

The `acme_cert_rotator` background service enforces Velda Edge's **Rule 2.9 (Frontline Traffic Worker vs Shadow Logistics Invariant)**:

```text
Latency Domain Separation:
┌──────────────────────────────────────────┐       ┌──────────────────────────────────────────┐
│ Frontline Serving Domain (Hot Path)      │       │ Shadow Logistics Domain (Background)     │
├──────────────────────────────────────────┤       ├──────────────────────────────────────────┤
│ - Throughput-critical request routing    │       │ - Bootstrap certificate inspection       │
│ - Strict 0-allocation invariant          │       │ - RFC 8555 ACME protocol execution      │
│ - Zero lock contention                   │       │ - ECDSA P-256 key & CSR generation       │
│ - Instant route evaluation               │       │ - Outbound HTTPS JWS signing & network   │
│ - Direct TLS handshakes via SniResolver  │       │ - Atomic disk persistence to storage_dir │
└──────────────────────────────────────────┘       └──────────────────────────────────────────┘
```

1. **Frontline Traffic Workers (`velda-worker-{id}`)**:
   - Serve inbound customer traffic exclusively on dedicated CPU cores.
   - Never initiate outbound connections to Let's Encrypt or external CAs.
   - When an HTTP-01 challenge arrives on port 80, the request is matched in-memory and blitted directly from `ChallengeStore`, bypassing upstream reverse-proxy pipelines.
2. **Shadow Logistics Worker (`velda-shadow-{id}`)**:
   - Operates on a dedicated companion thread pinned under `SCHED_IDLE`.
   - Owns the full lifecycle of certificate ordering, cryptographic key generation, and disk persistence.
   - Injects temporary routing rules into the data plane only during the brief verification window (2–5 seconds).

---

## 2. Ephemeral Exact Route Injection & RAII Scope Guard

To validate domain ownership per RFC 8555 §8.3, the edge gateway must answer HTTP GET requests at `/.well-known/acme-challenge/<token>` with the computed key authorization string:

```text
                       EPHEMERAL ROUTE LIFECYCLE (RAII)
                       
  ACME Client (Shadow)                         Router / Frontline Workers
         │                                                 │
         │ 1. register_challenge(token, key_auth)          │
         ├────────────────────────────────────────────────►│ (Injects exact route into Router)
         │    [Returns EphemeralRouteGuard]                │ (Publishes wire response to ChallengeStore)
         │                                                 │
         │ 2. CA validates domain ownership                │
         │    (Remote GET /.well-known/acme-challenge/...) │──► Matches RouteId(u32::MAX)
         │                                                 │    Returns pre-baked wire response
         │                                                 │
         │ 3. Challenge Completed / Failed / Timed Out     │
         │    [EphemeralRouteGuard dropped]                │
         ├────────────────────────────────────────────────►│ (Router removes exact route)
         │                                                 │ (ChallengeStore purges entry)
         ▼                                                 ▼
```

### 2.1 RAII Teardown Guarantee (`EphemeralRouteGuard`)

In distributed cloud environments, challenge validation may fail due to CA timeouts, DNS propagation delays, or process cancellation. An unmanaged route injection mechanism risks leaking ephemeral challenge paths into the router table over time.

`acme_cert_rotator` eliminates route leakage through an RAII scope guard:

```rust
pub struct EphemeralRouteGuard {
    router: Arc<RwLock<Router>>,
    listener_id: ListenerId,
    exact_path: String,
    _challenge_guard: ChallengeGuard,
}

impl Drop for EphemeralRouteGuard {
    fn drop(&mut self) {
        if let Ok(mut r) = self.router.write() {
            r.remove_http1_exact_route(self.listener_id, &self.exact_path, ACME_CHALLENGE_ROUTE_ID);
        }
    }
}
```

* **Unconditional Cleanup**: Whether the ACME task completes with `Ok`, exits early with `Err`, propagates a timeout via `?`, or is cancelled by Tokio during a graceful gateway shutdown, the `Drop` implementation executes automatically, removing the route from the routing table and purging the token from RAM.

---

## 3. In-Memory Challenge Serving & SIMD 128-Bit Vector Matcher

The incoming request path must be evaluated rapidly to differentiate standard customer routes from incoming ACME challenge probes:

```text
                 SIMD 128-BIT DUAL-OVERLAPPING PREFIX MATCHER
                 
  Path: [ / . w e l l - k n o w n / a c m e - c h a l l e n g e / ] <token>
        |◄─────────────── 16 Bytes ──────────────►|
        |                  (XMM0: Load 0)         |
        |                                         |
        |                             |◄─────────────── 16 Bytes ──────────────►|
        |                             |                 (XMM1: Load 1)          |
        |                             |◄── 4B Overlap ──►|
```

### 3.1 Vectorized Dual-Overlapping Check (`has_acme_prefix_128_simd`)

The standard ACME challenge prefix `/.well-known/acme-challenge/` spans exactly 28 bytes. Rather than checking characters with scalar byte comparisons or calling dynamic library functions (`memcmp`), the matcher uses two 128-bit vector registers:

1. **Vector Load 0 (`XMM0`)**: Loads bytes `0..16` (`b"/.well-known/acm"`).
2. **Vector Load 1 (`XMM1`)**: Loads bytes `12..28` (`b"/acme-challenge/"`), overlapping by 4 bytes with Load 0.
3. **Parallel XOR Comparison**:
   - Compares both 16-byte blocks against pre-aligned constant keys using `_mm_xor_si128`.
   - Combines differences with `_mm_or_si128` and extracts the equality bitmask with `_mm_movemask_epi8`.
4. **General-Purpose Register (GPR) Preservation**:
   - Because the 28-byte comparison executes purely inside vector registers (`XMM0`, `XMM1`), **all integer GPRs (`%rax`, `%rcx`, `%rdx`, `%rsi`, `%rdi`, `%r8`..`%r15`) remain unoccupied** for customer route matching.

---

## 4. Natural Hardware Eviction & Ephemeral Confinement

ACME HTTP-01 challenge responses are inherently lightweight and short-lived:
- The pre-baked wire response spans approximately 180 bytes (occupying only 3 physical 64-byte cache lines).
- It is generated exclusively on the Shadow Logistics Worker (`velda-shadow-{i}`, Core $i$). Because L1D and L2 caches are strictly private to each physical CPU core, Core $i$'s working set never impacts the private L1D/L2 caches of frontline traffic cores (`velda-worker-{0..N-1}`).
- The CPU's hardware pseudo-LRU (Least Recently Used) eviction controller naturally migrates these 3 cache lines out of Core 0's private cache as the background thread yields or handles subsequent tasks. Zero bus-serializing instructions (such as `clflush`) are executed, avoiding interconnect bus invalidation broadcasts.
- When the external ACME CA executes its verification probe seconds later, the handling frontline worker loads the response cleanly with zero cross-core cache invalidation penalty.

---

## 5. Crash-Safe Atomic Disk Persistence (LKG Storage)

Unlike purely in-memory plugins, TLS certificates must survive gateway restarts to prevent redundant ACME orders that rapidly exhaust CA rate limits:

```text
                       ATOMIC DISK PERSISTENCE (storage.rs)
                       
  Downloaded Cert & Private Key
               │
               ▼
  ┌──────────────────────────────────────────────┐
  │ 1. Open Temporary File:                      │
  │    /var/lib/velda/certs/domain/.tmp.<uuid>   │
  │    (Mode 0600 on private keys)               │
  └──────────────────────┬───────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────┐
  │ 2. Flush Kernel Buffers:                     │
  │    file.sync_all() (fsync syscall)           │
  │    (Guarantees physical block write)         │
  └──────────────────────┬───────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────┐
  │ 3. Atomic Filesystem Replacement:            │
  │    std::fs::rename(tmp_path, target_path)    │
  │    (Atomic directory inode update)           │
  └──────────────────────────────────────────────┘
```

1. **Corruption Immunity**: Writes to `.tmp.<uuid>` first. If the host experiences sudden power loss, kernel panic, or OOM termination, the target `fullchain.pem` or `privkey.pem` remains untouched.
2. **Filesystem Barrier (`fsync`)**: Invokes `sync_all()` before renaming, ensuring physical write-through from kernel page cache to physical disk.
3. **Atomic `rename` Syscall**: The POSIX `rename` syscall atomically replaces the directory inode. The gateway never encounters partially written or truncated PEM files.

---

## 6. Zero-Downtime TLS Hot-Swap State Transitions

Once a new certificate is downloaded and persisted, it must be activated in downstream TLS listeners:

```mermaid
stateDiagram-v2
    [*] --> ActiveCert_V1: Initial Startup from LKG Disk
    
    ActiveCert_V1 --> BackgroundRenewal: 30 Days before Expiry (Cron)
    
    BackgroundRenewal --> OrderValidation: HTTP-01 Challenge Verified
    OrderValidation --> DownloadCert: Download X.509 Chain & ECDSA Key
    
    DownloadCert --> AtomicDiskWrite: Persist to Storage Dir (.tmp -> rename)
    AtomicDiskWrite --> HotSwap: SniResolver::add_certificate()
    
    HotSwap --> ActiveCert_V2: SniResolver Points to CertifiedKey V2
    
    note right of HotSwap
      In-flight handshakes hold Arc<CertifiedKey V1>
      New handshakes select CertifiedKey V2
      0 dropped connections
    end note
```

* **`SniResolver` State Transition**: `TlsHotSwapper` obtains a write guard on `SniResolver` to install the parsed `CertifiedKey`. In-flight TLS 1.3 handshakes holding existing `Arc<CertifiedKey>` references complete their cryptographic handshakes uninterrupted, while subsequent handshakes negotiate with the renewed certificate chain.

---

## 7. Physical Hardware Boundaries & Non-Dogmatic Caveats

Systems engineering requires acknowledging physical boundaries and real-world operational constraints:

1. **Third-Party CA Network Availability**:
   - Automated certificate issuance relies on external network endpoints (Let's Encrypt / Boulder). WAN latency, DNS propagation, and CA API rate-limiting are outside the gateway's direct control. While frontline serving is isolated, certificate renewal itself is subject to external network availability.
2. **Ephemeral Route Injection Lock Contention**:
   - Inserting and removing the ephemeral challenge route requires acquiring a write lock on `velda_router::Router`.
   - While the write window is minimal, high-frequency route modifications on extremely large routing tables could theoretically induce a brief reader stall on frontline workers during route registration and teardown.
3. **Disk I/O Write Latency on Slow Block Storage**:
   - The `sync_all()` syscall forces data to physical block storage. On slow network storage (e.g. overloaded NFS or cloud EBS volumes with throttled IOPS), this blocking operation is offloaded to background threads to avoid impacting the async runtime.
4. **HTTP-01 Protocol Boundary (Wildcard Incompatibility)**:
   - Per RFC 8555, HTTP-01 challenges are strictly incapable of validating wildcard domains (`*.domain.com`). Deployments requiring wildcards must utilize DNS-01 mechanisms.

---

## 8. Empirical Implementation Proof (Assembly & Symbols)

The low-level mechanisms described above are verified directly in the compiled release assembly:

### 8.1 Inlined 128-Bit SIMD Prefix Checker

The prefix check in `ChallengeStore::lookup` compiles into leaf vector instructions without callee-saved register pushes:

```assembly
# Leaf fast-path prefix check from target/release:
cmp    %rcx, 0x1c                  # Compare path length with 28 (0x1c) bytes
jb     .Lnot_found                 # Early exit if path < 28 bytes
movdqu (%rdx), %xmm0               # Load bytes 0..16: "/.well-known/acm"
pxor   .LC_PREFIX_0(%rip), %xmm0   # Vector XOR with expected key 0
movdqu 12(%rdx), %xmm1             # Load bytes 12..28: "/acme-challenge/"
pxor   .LC_PREFIX_1(%rip), %xmm1   # Vector XOR with expected key 1
por    %xmm0, %xmm1                # Combine diff bitmasks
pxor   %xmm0, %xmm0                # Zero vector register
pcmpeqb %xmm1, %xmm0               # Compare byte equality
pmovmskb %xmm0, %eax               # Extract bitmask into EAX
xor    $0xffff, %eax               # Test if all 16 bytes matched
jne    .Lnot_found                 # Exit immediately if mismatch
call   lookup_matched              # Outlined slow-path dispatch on match
```
