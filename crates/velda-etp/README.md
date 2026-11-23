# velda-etp

External Third-Party (`velda-etp`) Data Source Connection & Page-Aligned Arena Engine for the Velda Edge Data Plane.

`velda-etp` manages persistent, application-scoped connection pools to independently deployed third-party infrastructure clusters (`redis`, `clickhouse`, `s3`, `loki`, `elasticsearch`, `otlp`, `kafka`, `statsd`). Connections live across the application lifecycle (`etp.bin`) rather than individual plugin lifecycles (`plugins.bin`), allowing multiple plugins to share a bounded connection arena and surviving plugin reloads with zero reconnection storms.

---

## End-to-End Operational Flow

```text
  velda-sync (etp.json -> etp.bin [magic: "VETP"])
        │
        ▼
  registry::reconcile(&current_registry, &new_configs, prewarm = true)
        │
        ├─► cfg.fingerprint() == existing.fingerprint  [64-bit u64-block FNV-1a]
        │     └──► Arc::clone(existing)  (0 reconnect, active EtpPageArena preserved)
        │
        └─► New or modified ETP entry
              ├──► registry::build_instance(cfg)
              │      ├──► Allocates isolated (required_pages + 1) * 4096B EtpPageArena
              │      └──► Compiles ColdConnectBlueprint (ip_u128, TLS 1.2/1.3, [u64; 4] pin)
              └──► registry::warm_instance(&inst).await  (Make-Before-Break pre-warm)
                     │
                     ▼
  Plugin (on Shadow Worker velda-shadow-{i}, SCHED_IDLE) calls etp.acquire().await
        │
        ├─► Fast Path (Warm Hit in Cache Line 0, L1D):
        │     1. Read ready_bitmap (1-Bit Ref Count per slot)
        │     2. idx = ready_bitmap.trailing_zeros()                [1-cycle hardware TZCNT]
        │     3. Claim bit via LOCK BTR (ready[idx]=0, busy[idx]=1)
        │     4. AGU computes slot_ptr = base + (idx << 6) + 64     [0 ALU add, 0 multiply]
        │     5. prefetcht0(slot_ptr) pulls ONLY Slot[idx] from L2 -> L1D
        │     6. Return 24-byte <Kind>Lease (DerefMut -> &mut EtpStream in-place)
        │
        ├─► Slow Path (Burst Spillover into +4096B Spare Page):
        │     1. Claim empty slot via ClaimedSlotGuard (100% async-cancel-safe)
        │     2. Connect via ColdConnectBlueprint (integer SocketAddr + Session Ticket PSK)
        │     3. Commit in-place at Slot[idx] -> return <Kind>Lease
        │
        ├─► On I/O Error During Plugin Flush:
        │     ├─► lease.reconnect().await      ──► Reconnects & overwrites Slot[idx] in-place
        │     └─► lease.recreate_in_background() ──► Flags broken for background replenish
        │
        └─► On Lease Drop:
              ├─► Healthy & idx < baseline_slots  ──► Updates last_used_sec (coarse_sec()),
              │                                       prefetcht1 (demotes Slot[idx] to L2),
              │                                       sets ready_bitmap[idx] = 1 (LOCK OR)
              └─► Healthy & idx >= baseline_slots ──► Auto-decays spare burst slot to EMPTY
```

---

## Memory Layout (`EtpPageArena<C>`)

Each ETP instance (`id`) allocates its own physically isolated `(required_pages + 1) * 4096`-byte, `4096`-aligned virtual memory arena. Two ETP instances of the same kind (e.g., `otlp-traces` and `otlp-logs`) reside in completely disjoint `k * 4096` memory regions.

```text
  Contiguous (required_pages + 1) * 4096 Bytes (align = 4096):
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │ Cache Line 0   (offset = 0, 64 Bytes = 512 Bits, L1D-Pinned via _MM_HINT_T0)      │
  │   • ready_bitmap:   [AtomicU64; 2]  (128 bits: 1 = warm & idle)                   │
  │   • busy_bitmap:    [AtomicU64; 2]  (128 bits: 1 = leased or connecting)          │
  │   • init_bitmap:    [AtomicU64; 2]  (128 bits: 1 = initialized connection C)      │
  │   • baseline_slots: u16 (1..=127) | total_slots: u16 (<=128) | page_count: u16    │
  │   • replenishing:   AtomicBool    | _pad: [u8; 9]                                 │
  ├───────────────────────────────────────────────────────────────────────────────────┤
  │ Cache Line 1..=baseline_slots (offset = (idx << 6) + 64, 64B/slot, L2-Resident)   │
  │   • Slot[0] (64B, align=64): [ conn: EtpStream (24B) | last_used_sec: u64 (8B) ]  │
  │   • Slot[1] (64B, align=64): [ conn: EtpStream (24B) | last_used_sec: u64 (8B) ]  │
  │   • ...                                                                           │
  ├───────────────────────────────────────────────────────────────────────────────────┤
  │ Spare Page (+4096 Bytes contiguous tail): Slot[baseline_slots .. total_slots]     │
  │   • Absorbs temporary traffic bursts in-place without heap fragmentation          │
  │   • Automatically closes and decays back to EMPTY on Lease Drop                   │
  └───────────────────────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure & Ownership Boundaries

| Module | Responsibility | Key Invariants |
| :--- | :--- | :--- |
| [`arena.rs`](src/arena.rs) | `(required_pages + 1) * 4096B` page-aligned bitmap arena (`EtpPageArena<C>`) & cancel-safe [`ClaimedSlotGuard`](src/arena.rs) | Compile-time asserted `64B` control line & `64B` slot size; L1D/L2 explicit prefetch; `coarse_sec()` idle expiry |
| [`connect.rs`](src/connect.rs) | Cold-path connection blueprint (`ColdConnectBlueprint`), integer IP target (`ColdTargetAddr`), TLS 1.2/1.3 compiler, and `[u64; 4]` SHA-256 cert pinning | `velda_core::lut::ip` integer `SocketAddr`; 128-bit `SeqLock` LKG DNS freeze; zero-alloc stack-only `sha256_u64x4`; `Resumption::in_memory_sessions(64)` |
| [`config.rs`](src/config.rs) | Declarative ETP schema (`EtpEntryConfig`, `EtpKind`, `EtpPoolSettings`) & 64-bit block FNV-1a fingerprinting | 8-byte `u64::from_le_bytes` chunk hashing; canonical JSON settings normalization |
| [`registry.rs`](src/registry.rs) | Flat `EtpRegistry`, `EtpInstance`, `EtpBackend` enum, and Make-Before-Break `reconcile()` | Zero OOP getter wrappers; `O(1)` fingerprint comparison preserves unchanged pools across reloads |
| [`error.rs`](src/error.rs) | Typed [`EtpError`](src/error.rs) hierarchy | Explicit `Config`, `Io`, `Tls`, `PoolExhausted`, `Timeout`, `NotFound`, `KindMismatch` variants |
| [`redis/`](src/redis/mod.rs) | `RedisEtp` & 24-byte `RedisLease` | Pre-bakes pipelined RESP2 `[AUTH + SELECT db]` handshake executed in a single RTT on connect |
| [`clickhouse/`](src/clickhouse/mod.rs) | `ClickHouseEtp` & 24-byte `ClickHouseLease` | In-place HTTP/HTTPS `EtpStream` pool; plugins read `etp.config` to pre-bake their own query prefixes |
| [`s3/`](src/s3/mod.rs) | `S3Etp` & 24-byte `S3Lease` | Persistent S3 / MinIO transport pool with region, bucket, and credential metadata |
| [`loki/`](src/loki/mod.rs) | `LokiEtp` & 24-byte `LokiLease` | Persistent Grafana Loki transport pool with optional `tenant_id` and auth metadata |
| [`elasticsearch/`](src/elasticsearch/mod.rs) | `ElasticsearchEtp` & 24-byte `ElasticsearchLease` | Persistent Elasticsearch / OpenSearch transport pool |
| [`otlp/`](src/otlp/mod.rs) | `OtlpEtp` & 24-byte `OtlpLease` | Multi-instance OTLP Collector pool (`http_protobuf`, `http_json`, `grpc`) shared across logs, metrics, and traces |
| [`kafka/`](src/kafka/mod.rs) | `KafkaEtp` & 24-byte `KafkaLease` | Round-robin multi-broker persistent TCP/TLS pool |
| [`statsd/`](src/statsd/mod.rs) | `StatsdEtp` & 24-byte `StatsdLease` | Pre-connected `tokio::net::UdpSocket` arena (`64B` slot per socket) for `Standard` and `DogStatsd` emission |

---

## Division of Responsibilities: `velda-etp` vs `velda-plugin`

- **`velda-etp` owns Connection Mechanics & Pool Lifecycle**:
  - Allocates the `k * 4096B` arena, resolves/freezes integer IPs, performs TCP + TLS 1.2/1.3 / mTLS / SHA-256 cert-pin handshakes, executes session-level authentication (such as Redis `AUTH` + `SELECT`), and lends out 24-byte in-place `<Kind>Lease` guards (`DerefMut` to `EtpStream` or `UdpSocket`).
- **`velda-plugin` owns Domain Logic & Wire Frame Baking**:
  - Reads static endpoint/credential metadata from `etp.config` once at startup to bake its own complete request frame prefix (e.g., ClickHouse `INSERT INTO ... FORMAT RowBinary`, Loki `/loki/api/v1/push`, S3 `SigV4` headers, or OTLP `/v1/logs` vs `/v1/metrics` vs `/v1/traces`).
  - Borrows a `<Kind>Lease` during background flush cycles on Shadow Workers, writes its pre-baked buffer directly onto the leased stream, and invokes `lease.reconnect().await` in-place if an idle socket was closed by the remote server.

---

## Verification

```bash
cargo test -p velda-etp
cargo clippy -p velda-etp --all-targets -- -D warnings
```
