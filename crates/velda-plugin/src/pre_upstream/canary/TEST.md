# Canary Plugin Verification & Test Matrix

## 1. Unit & Static Analysis Tests

| Scope | Location | Verified Invariants | Command |
| :--- | :--- | :--- | :--- |
| **Plugin Behavior & Strict Boundaries (3 tests)** | `crates/velda-plugin/src/pre_upstream/canary/plugin.rs` | 1. All 5 header rules force canary even when `hash_headers` are absent; missing/partial/blank `hash_headers` stay 100% deterministically on baseline (`0` PRNG fallback); `AccessLevel::Read` preserves headers verbatim.<br>2. Multi-header (`["x-tenant-id", "x-user-id"]`) 20,000-pair distribution matches `125‰`, and increasing `ratio` `125 -> 350` keeps 100% of prior canary users pinned.<br>3. Rejects `ratio: 0`, `ratio: 1000`, `ratio: 1001`, empty `hash_headers: []`, unknown legacy field (`hash_strategy`), and whitespace rule values; verifies `wire_plugin_upstreams()` resolution. | `cargo test -p velda-plugin --lib canary` |
| **Schema Validation (138 tests)** | `crates/velda-plugin/src/pre_upstream/canary/validation.rs` & `validation` module | Full pre-sync config validation suite (`CanarySchemaConfig` with `#[serde(deny_unknown_fields)]`). | `cargo test -p velda-plugin --lib validation` |
| **Edge Runtime Wiring (3 tests)** | `crates/velda-edge/src/runtime/plugin.rs` | Plugin table assembly and upstream wiring invariants. | `cargo test -p velda-edge --lib runtime::plugin` |
| **Hot-Path ASM Invariant** | `target/release/deps/velda_plugin-*.s` | Confirms `0` `alloc::*` heap calls, `0` `divq`, `0` `ud2`, `0` `lock *`, `0` TLS lookups (`__tls_get_addr`), and BMI2 `mulxq $1000` in `CanaryPlugin::on_upstream_request`. | `audit_asm.py <file.s> --symbol ...` |

---

## 2. Control Plane Sync & Upstream Wiring Verification

| Test Case | Verified Component | Result |
| :--- | :--- | :--- |
| **Canary Canonical ID Rejection** | `velda-sync` (`plugins.json`) | Rejected non-canonical `"id": "canary_main"` safely while preserving LKG state (`Unknown plugin ID 'canary_main'`). |
| **Dense `u16` Index Resolution** | `velda-plugin::wire_plugin_upstreams` + `velda-edge/src/runtime/plugin.rs` | Resolved `backend-canary` -> `upstream_id=1` at boot (`revision=1`) when placed second in `upstreams.json`. |
| **Adversarial Hot-Reload Re-Index** | `velda-edge/src/reload.rs` | Reordered `upstreams.json` placing `backend-canary` at index `0` and `backend-base` at index `1` (`revision=3`). Edge reloaded atomically, logged `upstream_id=0`, and routed rule-miss -> `:18081` (`base`) and rule-hit -> `:18091` (`canary`). |

---

## 3. Isolated Dual-Node KVM End-to-End Test Matrix

Executed live between `velda-obs` (`wrk` / Python HTTP client / `curl` -> `velda-gw:18080`) and dual symmetric HTTP/1.1 backends (`base` on `192.168.122.20:18081`, `canary` on `192.168.122.20:18091`):

| Test Case | Setup | Assertion | Measured Result |
| :--- | :--- | :--- | :--- |
| **Strict Missing-Header Safety (No Fallback)** | `ratio: 500`, `hash_headers: ["x-tenant-id", "x-user-id"]`, 20 requests each with `{}` or `{"X-Tenant-Id": "acme"}` or `{"X-User-Id": "user-1"}` | Missing any configured header routes 100% safely to `backend-base`. | **20/20 `base`** for every missing/partial combination (`PASS`) |
| **5 Rule Operators** | Matching `equals`, `prefix`, `suffix`, `contains`, `exists` headers with `hash_headers` absent | All 5 matchers force `backend-canary`. | **5/5 `canary`** (`PASS`) |
| **Zero Header Mutation** | `GET /inspect` with `X-Canary-Eq: true`, `X-User-Id: alice-99` | Upstream receives exact client headers without proxy-injected canary markers. | Reflected exact client headers on `upstream: "canary"` |
| **Single-Header Split (`wrk`)** | `ratio: 200`, `hash_headers: ["x-user-id"]`, `4,096` users, `447k` reqs | Routes near `200‰` deterministically. | `base=353,484`, `canary=93,929` (**`209.94‰`**) |
| **Multi-Header Fold Split (`wrk`)** | `ratio: 200`, `hash_headers: ["x-tenant-id", "x-user-id"]`, `4,096` pairs, `476k` reqs | Folds both headers cleanly near `200‰`. | `base=378,773`, `canary=97,396` (**`204.54‰`**) |
| **Session Stickiness** | `20` repeated requests each for `prod-user-00000` (base) and `prod-user-00001` (canary) | Zero cross-upstream drift for identical key. | `prod-user-00000 -> {'base'}`, `prod-user-00001 -> {'canary'}` |

---

## 4. Reproducing Checks Locally

```bash
# Formatting & Strict Lints
cargo fmt --all --check
cargo clippy -p velda-plugin -p velda-sync -p velda-edge --all-targets --all-features -- -D warnings

# Unit & Integration Tests
cargo test -p velda-plugin --lib canary
cargo test -p velda-plugin --lib validation
cargo test -p velda-edge --lib runtime::plugin
```
