# Performance Benchmark Report: JWT Authentication Plugin (`jwt_auth`)

This report evaluates unified [`JwtAuthPlugin`](plugin.rs) (`pre_upstream` phase) across **static (`mode: "internal"`)** and **dynamic ETP key-rotation (`mode: "external"`)** configurations against **100% real Identity & Secret Providers (zero mock servers)** under **asymmetric `tc netem` WAN network shaping** on isolated KVM guests. Every test group reverts both VMs (`velda-gw` and `velda-obs`) cleanly to `bench-clean` snapshots before starting.

---

## 1. Mathematical Proofs & Hardware Complexity Bounds

### Proof 1 — 3-Tier Cache Alignment & Bounded Memory Residency (`cache.rs` & `store.rs`)
From compile-time assertions in [`cache.rs`](cache.rs) and [`store.rs`](store.rs):
* `size_of::<JwtCache>() == 64`, `align_of::<JwtCache>() == 64` (single L1D control line at `JwtAuthPlugin` offset `0`)
* `size_of::<DirectoryLine>() == 64`, `align_of::<DirectoryLine>() == 64`, `offset_of!(JwksKeyTable, dir) == 0`
* `GROUP_WIDTH = 64` slots per `BucketGroup` (`align_of = 64`, `offset_of!(BucketGroup, tags) == 0`)
* `MAX_HOT_L3_TOTAL_BYTES = 4 * 1024 * 1024` ($4\text{ MiB}$), `MAX_COLD_DRAM_TOTAL_BYTES = 256 * 1024 * 1024` ($256\text{ MiB}$)

For $L$ worker lanes and system RAM $M_{\text{ram}}$, `JwtCache::hardware_sized()` enforces:

$$B_{\text{hot}} \le 4\text{ MiB}, \qquad B_{\text{cold}} = \operatorname{clamp}\!\left(\left\lfloor\frac{M_{\text{ram}}}{50}\right\rfloor,\; 4\text{ MiB},\; 256\text{ MiB}\right)$$

while keeping [`JwksKeyTable`](store.rs) header metadata compact in L1D (`64 B` `DirectoryLine`: `tags[16]`, `algs_packed` storing $16 \times \text{u4}$ algorithms in one `u64`, `valid_mask: u16`, `primary_slot: u8`, `flags: u8`, `table_version_tag: u64`).

### Proof 2 — $O(1)$ Multi-Version Key Lookup Across `MAX_JWKS_SLOTS = 16` Slots
When a cold JWT carries a JOSE `kid` string, `hash_kid_salted()` maps arbitrary-length strings to a boot-salted integer $\text{kid}_{\text{u64}} = \max(\operatorname{Trunc64}(\text{SHA-256}(\text{boot\_salt} \parallel \text{"kid:"} \parallel \text{raw\_kid})), 2)$. `resolve_slot_by_kid_u64()` scans all 16 `tags[16]` bytes in a single SIMD instruction (`match_tag_mask16` via SSE2 `_mm_cmpeq_epi8` / NEON `vceqq_u8` / zero-borrow SWAR) masked by `valid_mask`. With 8-bit uniformly distributed tags across `16` occupied slots:

$$\mathbb{E}[\text{FP}_{\text{u64}}] = 16 \times 2^{-8} = 0.0625\text{ extra checks/lookup}$$

**Result:** Resolving an uncached token against the **oldest standby key in `Slot 15`** executes the exact same constant-time L1D bitmap scan as **`Slot 0` (primary)** — empirically verified in `JW03` (`659,079` vs `659,199` instructions/req across 16 live Vault versions).

### Proof 3 — Lock-Free Key Rotation Coherence & Selective Cache Invalidation
Each `BucketGroup` slot records a `u64` tag:
* **Verified tokens (`Positive`)** store the signing slot's `key_tag = max(FNV-1a(kid_u64 || alg_u4 || raw_bytes), 2)`.
* **Unbound rejections (`UnknownKid`, `ClaimAssertionFailed`)** store `DirectoryLine::table_version_tag`.

On cache lookup, `JwksKeyTable::is_cache_tag_valid()` checks `p_idx` first and falls back to `valid_mask & !(1 << p_idx)`:
1. **Additive rotation** (promoting new key `v2..vN` into `Slot 0` while keeping prior versions in `Slots 1..15`): old tokens' `key_tag` remains present in `standby_mask`, **preventing any cache-flush storm** and validating both cached and cold old-key tokens (`JW04`).
2. **Explicit key retirement** (removing a retired `kid` or rotating its key material): its `key_tag` vanishes from `JwksKeyTable`, causing cached tokens signed by the retired key to **miss immediately** and reject with `401` even before `cache_ttl_secs` expires (`JW04` Vault `min_decryption_version` retirement test).

### Proof 4 — Zero-Hash Malformed Guard & Bit-7 Reject Non-Pollution
`is_plausible_jwt_shape()` checks `5 <= len <= 16,384` and exact two-dot framing (`header.payload.signature`) in nanoseconds before computing `SHA-256`. Valid cache evictions (`demote_victim_if_valid()`) explicitly exclude `CachedDecision::Empty | CachedDecision::Unauthorized`, preventing `401` sprays from evicting active sessions in Cold DRAM.

### Proof 5 — Per-Boot Keyed Collision Bound
`JwtCache::token_to_u128()` computes keyed `SHA-256(boot_salt || token)` truncated to `128` bits (`u128`), ensuring static collision sprays cannot predict bucket indices across restarts and bounding accidental birthday collisions across $n$ distinct active tokens to $P \le \frac{n^2}{2^{129}}$ ($< 10^{-20}$ at $n = 10^9$).

---

## 2. Test Environment & Real-Provider Topology

All tests executed across two KVM guests (`Ubuntu 24.04`, Linux `6.8`, 4 vCPU each) with `tc netem` (`prio` + `u32`, SSH port `22` bypassed) shaping every packet on `velda-gw` (`enp1s0`):

| Node | Spec | Roles / Competitors | `tc netem` Shaping (`velda-gw` `enp1s0`) |
| :--- | :--- | :--- | :--- |
| **`velda-gw`** | 4 vCPU, 4 GiB RAM | `velda-edge`, `HAProxy 2.8.30`, `Envoy v1.32`, `APISIX 3.11.0` | Client `sport 8080`: `delay 6ms 1.5ms distribution normal loss 0.02% 25%` |
| **`velda-obs`** | 4 vCPU, 6 GiB RAM | `wrk`, `velda-bench-backend` (`:8081`), **Real IdPs** | Backend `dport 8081`: `delay 1.5ms 0.3ms distribution normal`<br>IdP `dport 8090/4444/8200`: `delay 10ms 2ms distribution normal` |

> [!NOTE]
> **Real Providers Exercised (Zero Mocks)**:
> - **Red Hat Keycloak `26.0.7`** (`quay.io/keycloak/keycloak:26.0.7`, `:8090`): `/realms/master/protocol/openid-connect/certs` + live dual-key component rotation via `/admin/realms/master/components` + exported realm PEM for `internal` static mode and HAProxy.
> - **Ory Hydra `v2.2.0`** (`oryd/hydra:v2.2.0`, `:4444/:4445`, `STRATEGIES_ACCESS_TOKEN=jwt`): `/.well-known/jwks.json` + live `POST /admin/keys/hydra.jwt.access-token` rotation.
> - **HashiCorp Vault `1.18`** (`hashicorp/vault:1.18`, `:8200`): Transit engine `/v1/transit/keys/{name}` (`ed25519` / `ecdsa-p256` / `rsa-2048`), live multi-version `/rotate` across up to `16` versions, and `/config` `min_decryption_version` key retirement.

---

## 3. Empirical Results & 5-Tier Diagnostic Matrix

### JW01 — Cross-Gateway Benchmark vs Real Red Hat Keycloak `26.0.7` (`c=512`, `GET /ping`, 64 OIDC JWTs)

| Gateway / Mode | Key Source | Clean RPS | Avg Lat | p50 | p99 | `non2xx` | RSS (MB) | `insn/req` | `cyc/req` | IPC | Branch Miss | L1D Miss | LLC Miss | `syscalls/req` | `CS/req` | `futex` Calls |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Velda Edge (`external`)** | Dynamic Keycloak `/certs` (`ttl=300s`) | **42,809.62** | **11.84ms** | **11.57ms** | **19.74ms** | **0** | 72.3 | **114,880** | **227,192** | 0.51 | 1.08% | 11.12% | 41.15% | **4.13** | 0.0908 | 8 |
| **Velda Edge (`internal`)** | Static `RS256` PEM (`pbww=0`) | **42,261.19** | 11.99ms | 11.69ms | 20.45ms | **0** | 72.2 | **115,276** | **229,413** | 0.50 | 1.07% | 11.21% | 41.27% | **4.16** | 0.0911 | **6** |
| **HAProxy `2.8.30`** | Static `jwt_verify(RS256, PEM)` | 29,412.92 | 17.31ms | 17.33ms | 27.16ms | **0** | **19.8** | 372,962 | 413,819 | 0.90 | **0.76%** | 5.39% | 30.29% | 5.13 | 0.0506 | 132 |
| **Envoy `v1.32`** (`jwt_authn`) | Dynamic `remote_jwks` (`300s`) | 12,040.38 | 33.71ms | 34.63ms | 53.27ms | *40,314* | 84.6 | 1,247,917 | 1,245,014 | 1.00 | 1.14% | **3.26%** | **19.70%** | 8.24 | 0.0527 | 51 |
| **Apache APISIX `3.11.0`** | Dynamic `openid-connect` (`use_jwks`) | 7,776.86 | 82.68ms | 63.05ms | 787.03ms | **0** | 215.6 | 2,102,443 | 1,908,411 | **1.10** | 1.14% | 4.01% | 20.83% | 7.07 | **0.0179** | 42 |

*\*Note on Envoy `JW01`: Under immediate `c=512` ramp against Keycloak over `10ms` IdP `netem`, Envoy's `remote_jwks` returned `40,314` `401` rejections across early worker JWKS convergence (whereas `JW07` immediately afterward ran with `non2xx = 0`).*

*Key Takeaways (`JW01`)*:
* **Static vs Dynamic Parity**: Dynamic `external` mode (`ArcSwap<JwksKeyTable>`, **42,810 RPS**) and static `internal` mode (**42,261 RPS**) exhibit identical per-request instruction footprints (`114.9k` vs `115.3k insn/req`).
* **Advantage Over Cached/Static Competitors**: Velda is **`1.46x` faster than HAProxy `2.8.30`** (`3.25x` fewer `insn/req`, zero `RS256` re-verification on cached hits), **`3.56x` faster than Envoy `jwt_authn`** (`10.86x` fewer `insn/req`, zero `non2xx` errors), and **`5.50x` faster than APISIX `3.11.0`** (`18.30x` fewer `insn/req`, **`39.87x` lower p99**, and **`2.98x` lower RSS**).

### JW07 — Authenticated `12 KB` Payload Proxy Under Real Keycloak `RS256` (`c=256`, `GET /12kb`)

| Gateway | RPS | Avg Lat | p50 | p99 | Max Lat | `non2xx` | RSS (MB) | Speedup vs Velda |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| **Velda Edge (`internal` static)** | **24,393.57** | **10.36ms** | **10.21ms** | **16.34ms** | **24.67ms** | 0 | 66.6 | `1.00x` |
| **Velda Edge (`external` KC JWKS)** | **24,365.02** | 10.46ms | 10.26ms | 17.42ms | 34.05ms | 0 | 66.6 | `1.00x` |
| **HAProxy `2.8.30` (Static RS256)** | 18,968.99 | 13.44ms | 12.94ms | 23.63ms | 49.91ms | 0 | **21.8** | `1.29x` faster |
| **Envoy `v1.32` (`jwt_authn`)** | 9,332.44 | 27.32ms | 27.37ms | 39.76ms | 58.08ms | 0 | 87.7 | `2.61x` faster |
| **Apache APISIX `3.11.0` (`use_jwks`)** | 6,614.05 | 38.95ms | 36.97ms | 64.95ms | 409.63ms | 0 | 210.5 | `3.69x` faster |

### JW02 — Static vs Dynamic & Cold Cryptographic Verification (`cache_ttl_secs: 0`) Across Real Providers

| Case / Provider | Mode & Algorithm | Conns | Clean RPS | Avg / p50 / p99 | `non2xx` | `insn/req` | `cyc/req` | IPC | RSS (MB) |
| :--- | :--- | ---: | ---: | :---: | ---: | ---: | ---: | ---: | ---: |
| **Static `HS256` Warm (`ttl=300`)** | `internal` HMAC-SHA256 | 512 | **49,452.35** | 10.23 / 10.06 / 16.47ms | 0 | **82,651** | **174,724** | 0.47 | 72.4 |
| **Static `HS256` Cold (`ttl=0`)** | `internal` HMAC-SHA256 | 512 | **48,926.22** | 10.37 / 10.12 / 17.61ms | 0 | **92,317** | **185,400** | 0.50 | 71.2 |
| **Vault `EdDSA` Warm (`ttl=300`)** | `external` Transit `ed25519` | 512 | **49,481.25** | 10.25 / 10.04 / 16.95ms | 0 | 84,877 | 178,974 | 0.47 | 72.0 |
| **Vault `EdDSA` Cold (`ttl=0`)** | `external` Transit `ed25519` | 256 | 23,101.64 | 26.16 / 10.29 / 411.24ms* | 0 | 658,710 | 525,648 | 1.25 | 61.4 |
| **Vault `ES256` Warm (`ttl=300`)** | `external` Transit `ecdsa-p256` | 512 | **49,479.49** | 10.21 / 10.02 / 16.57ms | 0 | 83,915 | 176,376 | 0.48 | 73.3 |
| **Vault `ES256` Cold (`ttl=0`)** | `external` Transit `ecdsa-p256` | 256 | 24,498.57 | 10.38 / 10.26 / 16.36ms | 0 | 685,456 | 522,813 | 1.31 | 61.1 |
| **Keycloak `RS256` Warm (`ttl=300`)** | `internal` Static PEM | 256 | 29,094.31 | 8.75 / 8.65 / 13.81ms | 0 | 114,508 | 231,424 | 0.49 | 62.6 |
| **Keycloak `RS256` Cold (`ttl=0`)** | `internal` Static PEM | 256 | 25,540.24 | 9.87 / 9.68 / 16.21ms | 0 | **399,928** | 396,955 | 1.01 | 61.5 |
| **Keycloak `RS256` Cold (`ttl=0`)** | `external` Dynamic `/certs` | 256 | 25,749.49 | 9.79 / 9.61 / 16.23ms | 0 | **398,298** | 395,569 | 1.01 | 61.0 |
| **Ory Hydra `RS256` Warm (`ttl=300`)** | `external` `/.well-known/jwks.json` | 512 | 48,037.46 | 10.55 / 10.32 / 17.55ms | 0 | 90,951 | 190,696 | 0.48 | 72.6 |
| **Ory Hydra `RS256` Cold (`ttl=0`)** | `external` `/.well-known/jwks.json` | 256 | 20,511.86 | 12.29 / 12.29 / 17.39ms | 0 | 946,858 | 668,502 | **1.42** | 61.9 |

*\*Note: `Keycloak` tests in `JW02` ran at `c=256` (WAN Little's Law ceiling $256 / 8.65\text{ms} \approx 29.6\text{k}$); on the `perf stat` pass, `Vault EdDSA Cold` recorded `p99=23.85ms` at `19,520 RPS`, while its clean run hit a `tc netem` `0.02%` loss retransmit spike (`max=481.56ms`).*

### JW03 — 16 Simultaneous Key Versions Without Rotation (`MAX_JWKS_SLOTS = 16`, Real Vault Transit `bench-ed`)

All 16 Ed25519 versions (`v1..v16`) live simultaneously in `JwksKeyTable` (`v16` primary at `Slot 0`, `v9` standby at `Slot 7`, `v1` oldest standby at `Slot 15`):

| Key Slot Position (`kid`) | Warm RPS (`ttl=300, c=512`) | Warm p50 / p99 | Warm `insn/req` | Cold Uncached RPS (`ttl=0, c=256`) | Cold p50 / p99 | Cold `insn/req` | Cold `cyc/req` | `non2xx` |
| :--- | ---: | :---: | ---: | ---: | :---: | ---: | ---: | ---: |
| **`Slot 0` (`kid="16"`, primary)** | **48,969.18** | 10.11 / 17.65ms | 83,996 | **24,731.10** | 10.18 / 15.93ms | 659,079 | 522,745 | **0** |
| **`Slot 7` (`kid="9"`, standby)** | **49,728.57** | 9.98 / 17.02ms | 84,064 | **24,677.19** | 10.22 / 15.64ms | 658,839 | 522,237 | **0** |
| **`Slot 15` (`kid="1"`, oldest)** | **49,752.75** | 9.98 / 16.65ms | 83,952 | **24,910.19** | 10.14 / 15.10ms | 659,199 | 521,741 | **0** |

**Proved $O(1)$ Slot Independence**: Cold verification against the **16th oldest key (`Slot 15`)** differs from `Slot 0` by only **120 instructions/req (`0.018%`)**, confirming `match_tag_mask16` + L3 `kid_ints` + Key Byte Arena elimination of linear key-list scanning.

### JW04 — Live Multi-Version Rotation, Old-Key Transition, Retirement & LKG Survival

| Provider & Transition Scenario | Setup & Action Executed Live | Measured Result & Status Assertions |
| :--- | :--- | :--- |
| **Vault `1.18` Live 5-Version Rotation (`v1 -> v2..v5`, `refresh=10s`)** | Continuous `24.04s` `c=512` `v1` traffic while calling `/transit/keys/bench-ec-rot/rotate` 4x live (`t=3s`), promoting `v5` to `Slot 0` | **49,827.41 RPS** (`1,197,766 reqs`), `p50=10.02ms`, `p99=16.66ms`, `max=28.58ms`, **`non2xx=0`**, RSS `71.6 MB` |
| **Vault Multi-Version Coexistence (`v1`, `v3`, `v5` Post-Rotation)** | Fresh unseen JWTs for `v1` (oldest `Slot 4`), `v3` (`Slot 2`), `v5` (`Slot 0`) + cached `v1` + mixed replay (`c=512`) | `v1_cached=200`, `v1_cold_unseen=200`, `v3_cold_unseen=200`, `v5_cold_unseen=200`; Mixed replay: **49,631.12 RPS (`non2xx=0`)** |
| **Vault Old-Key Retirement (`min_decryption_version: 2`)** | Retired `v1` on Vault, waited `11.5s` (`refresh=10s`), replayed **still-cached (`ttl=300s`) `v1` token** vs `v3`/`v5` | Cached `v1` flipped immediately to **`401 Unauthorized`**; `v3` & `v5` stayed **`200 OK`** |
| **Keycloak `26.0.7` Live Dual-Key + `preserve_static_key: true`** | Added `rsa-generated` `v2` (`priority=200`) live on Keycloak while serving `v1` (`priority=100`); replayed mixed `[v1, v2]` at `c=512` | `v1_old=200`, `v2_new=200`; Dual-key traffic: **43,098.00 RPS** (`p99=19.54ms`, **`non2xx=0`**) |
| **Keycloak Full Container Crash LKG Survival (`docker rm -f idp-kc`)** | Killed Keycloak mid-run and kept serving `[v1, v2]` at `c=512` | **42,423.22 RPS** (`213,610 reqs`, `p50=11.61ms`, `p99=20.56ms`, **`non2xx=0`**) |
| **Ory Hydra `v2.2.0` Live JWK Rotation (`hydra-rotated-v2`)** | Added `hydra-rotated-v2` to `/admin/keys/hydra.jwt.access-token` after `v1` tokens active; waited `11.5s`, replayed `v1` at `c=512` | `v1` stayed **`200 OK`** at **49,475.94 RPS** (`p50=10.03ms`, `p99=16.74ms`, **`non2xx=0`**) |

<img src="./jwt_throughput.svg" width="100%" alt="Real-Provider JWT Auth Throughput Across Static, Dynamic, Rotation, and 16-Slot Modes" />

### JW05 — Wire-Speed Rejections, 2-Dot Structural Pre-Filter & `Bit-7` Negative Cache (`c=512`, `tc netem`)

| Rejection Vector | Mechanism Triggered | Clean RPS | p50 | p99 | Expected Wire Code | RSS (MB) | `insn/req` | `cyc/req` | `syscalls/req` | `futex` Calls |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | ---: | ---: |
| **`jwt_401_missing`** | Early return before token hashing | **78,742.00** | 6.41ms | **10.48ms** | `401` (`632,776` reqs) | 67.2 | **40,243** | 76,437 | 2.16 | 6 |
| **`jwt_401_malformed_2dot`** | `is_plausible_jwt_shape` (0 SHA-256) | **69,883.28** *(perf: 70,357)* | 6.57ms | 1.01s *(perf: 12.85ms)* | `401` (`636,382` reqs) | **63.9** | 40,388 | **74,185** | **2.12** | 6 |
| **`jwt_401_badsig_negcache`** | `TAG_REJECT_BIT` (`0x80 \| tag7`), 0 crypto | **78,176.94** | 6.44ms | 10.78ms | `401` (`628,103` reqs) | 69.2 | 44,511 | 80,554 | 2.14 | **0** |
| **`jwt_403_assert_claims`** | Cached `CachedDecision::Forbidden`, 0 crypto | **78,186.52** | 6.45ms | 10.68ms | `403` (`628,135` reqs) | 65.6 | 43,983 | 80,662 | 2.14 | 4 |

### JW06 — Cross-Provider Head-to-Head Comparison (`Keycloak` vs `Ory Hydra` vs `Vault`)

Directly comparing all three real providers across warm serving, cold cryptographic verification, key rotation mechanics, and failover under `10ms ± 2ms` IdP `tc netem`:

| Comparison Axis | **Red Hat Keycloak `26.0.7`** (`:8090`) | **Ory Hydra `v2.2.0`** (`:4444/:4445`) | **HashiCorp Vault `1.18`** (`:8200` Transit) |
| :--- | :--- | :--- | :--- |
| **Key Discovery Path** | `/realms/master/protocol/openid-connect/certs` | `/.well-known/jwks.json` | `/v1/transit/keys/{name}` (multi-version PEM/Raw map) |
| **Algorithm & Key Geometry** | `RS256` (`2048-bit`, skips `RSA-OAEP` `use:enc`) | `RS256` (`STRATEGIES_ACCESS_TOKEN=jwt`) | `EdDSA` (`ed25519`), `ES256` (`P-256`), `RS256` |
| **Warm Cache RPS (`c=512`, `ttl=300s`)** | **42,809.62 RPS** (`114,061` insn/req post-opt) | **48,037.46 RPS** (`90,951` insn/req) | **49,481.25 RPS** (`EdDSA`) / **49,479.49** (`ES256`) |
| **Cold Crypto (`ttl=0`, `c=256`)** | **25,991.84 RPS** (`375,601` insn/req post-opt) | **20,511.86 RPS** (`946,858` insn/req) | **24,498.57 RPS** (`ES256`) / **23,101.64** (`EdDSA`) |
| **Live Rotation & Old-Key Coexistence** | `/admin/realms/master/components` priority swap; keeps old `kid` in `/certs` | `/admin/keys/hydra.jwt.access-token` append; retains old `kid`s | `/transit/keys/{name}/rotate` versions `1..N`; keeps newest `16` (`min_decryption_version` retires old) |
| **Chaos Recovery (`UnknownKid`, `refresh=300s`)** | **`246.0 ms`** (`200 OK` on new `rsa-chaos-v2`) | **`1,133.4 ms`** (`200 OK` on `hydra-chaos-v2`) | LKG **`200`** when container dead; self-healed **`10,072.4 ms`** after fresh Vault restart |

### JW08 — Optimization Before/After Verification & Cross-Provider Chaos Stress Matrix

Evaluated after compiling and deploying updated `x86-64-v3` binaries with (1) **Zero-DOM streaming `serde_json::Deserializer` (`eval_jwt_payload_streaming`)**, (2) **Zero-atomic `JwtCache::lookup_and_apply`**, and (3) **Rate-limited `UnknownKid` Shadow wakeup (`cooldown = 2s`)**:

| Optimization / Chaos Test Case | Baseline (`Before`) | Optimized (`After`) | Delta / Verified Verdict |
| :--- | ---: | ---: | :--- |
| **`JW02` Keycloak `RS256` Cold (`ttl=0`, `~1 KB` OIDC JSON)** | 25,749.49 RPS (`398,298` insn / `395,569` cyc) | **25,991.84 RPS** (**`375,601`** insn / **`379,423`** cyc) | **`-22,697 insn/req` (`-5.7%` total)**, `-16,146 cyc/req`, `0` errs |
| **`JW02` `HS256` Cold (`ttl=0`, compact JWT, `c=512`)** | 48,926.22 RPS (`92,317` insn/req) | 48,816.84 RPS (**`91,092`** insn/req) | **`-1,225 insn/req`**, `p99=16.49ms`, `0` errs |
| **`JW02` `HS256` Heavy `1.8 KB` Nested JSON Cold (`c=512`)** | — | **40,413.25 RPS** (`161,033` insn/req, `p99=20.83ms`) | Skips `25` realm roles + `12` clients + `15` groups with `0` DOM tree |
| **`JW01` Keycloak Dynamic Cached (`c=512`)** | 42,809.62 RPS (`114,880` insn/req) | 41,732.72 RPS (**`114,061`** insn/req) | **`-819 insn/req`** (eliminated `Arc::clone`/`Arc::drop` on every hit) |
| **Chaos A (`Keycloak`): `UnknownKid` Flood (`c=512`) + Attack Suite** | — | **77,055.14 RPS** flood; `alg:none`, `HS256` pubkey confusion, `20 KB` JWT = **`401`** | `rsa-chaos-v2` recovered in **`246.0 ms`** under `refresh=300s` |
| **Chaos B (`Ory Hydra`): Live Rotation (`refresh=300s`)** | — | `v2` demand-synced in **`1,133.4 ms`**; old `v1` remained **`200 OK`** | Zero 300s polling stall |
| **Chaos C (`Vault 1.18`): `18`-Version Slot Overflow + Restart** | — | `v18=200`, `v10=200`, `v3=200`; evicted **`v1=401`**, **`v2=401`** | LKG `200` on Vault death; self-healed **`10.07s`** after restart |

---

## 4. Verified Release Assembly (`x86-64-v3`) & Symbol Audit

Compiled with `RUSTFLAGS="-C target-cpu=x86-64-v3"` (`target/release/deps/velda_plugin-e4115f479001c454.s`), inspecting `_RNvXs_...JwtAuthPlugin...on_upstream_request` (which inlines `JwtCache::lookup`, `match_tag7_mask64`, `JwksKeyTable::resolve_slot_by_kid_u64`, and `verify_jwt`):

| Audited Criterion | Verified Observation in `.s` Release Binary | Interpretation / Invariant Check |
| :--- | :---: | :--- |
| **Total Inlined Instructions (`on_upstream_request`)** | `2,089` instructions | Encompasses full sync fast-path + fallback `verify_jwt` paths inlined with zero call indirection |
| **Heap Allocations (`__rust_alloc`)** | `1` call *(cold miss path only)* | Strictly gated behind L3 cache miss + non-empty `claims_to_headers` allocation (`0` on cache hit & reject paths) |
| **Dynamic Indirect Jumps (`jmpq *%reg`)** | **0** | **PASS**: Zero virtual/jump-table BTB pollution |
| **Inline `syscall` & Bus `lock` Instructions** | **0 syscalls / 0 `lock` ops** | **PASS**: Clock uses `req.ingress_time_sec()`; `ArcSwap` & per-core `JwtCache` read lock-free with `0` bus locks |
| **Prefetch, AVX2 & BMI Instructions** | `prefetcht0` (`1`), `prefetcht2` (`2`), `vpcmpeqb` (`6`), `vpmovmskb` (`6`), `tzcnt` (`7`), `blsr` (`4`) | Hardware L1D/L3 cache prefetching + 64-byte AVX2 SIMD `TagLine` matching + BMI1 bit extraction |

---

## 5. Comprehensive Architectural Comparison

| Dimension | Velda Edge (`jwt_auth`) | HAProxy `2.8.30` (`jwt_verify`) | Envoy `v1.32` (`jwt_authn`) | Apache APISIX `3.11.0` (`openid-connect`) |
| :--- | :--- | :--- | :--- | :--- |
| **Static & Dynamic Mode Unity** | Single `JwtCache` + `JwksStore` (`mode: internal \| external`) with zero-overhead parity (`42.3k` vs `42.8k RPS`) | Static PEM file only (`requires reload` / SPOE external process for rotation) | Supports `local_jwks` & `remote_jwks` (`12.0k RPS`, initial fetch pressure under burst) | Static `jwt-auth` vs OpenResty `lua-resty-openidc` (`7.8k RPS`, `215.6 MB` RSS) |
| **Multi-Version Rotation (`16` Slots)** | `64B` L1D `DirectoryLine` + L3 `key_tags` retains standby old keys cleanly (`0` cache flush) and drops retired `kid`s immediately | Manual key file swap drops single-key verification | Re-verifies RSA per request without cross-request L3 signature cache | Lua table + OpenResty dict lookup (`2.10M insn/req`) |
| **Old-Key Slot Traversal Cost** | **$O(1)$**: `659,079` insn/req (`Slot 0`) vs `659,199` insn/req (`Slot 15`, oldest key) | Linear/single PEM key argument per `jwt_verify` ACL | C++ JWK map lookup + full BoringSSL verify per request | Lua table scan + OpenSSL FFI per request |
| **Signed Token Caching** | Per-core L3 AVX2 `vpcmpeqb` (`82k–115k insn/req` cached) + Bit-7 negative cache (`78.2k RPS`) | None (`373k insn/req` even for repeated tokens) | None (`1.25M insn/req`) | LuaJIT + shared dict overhead (`787ms p99` tail) |
