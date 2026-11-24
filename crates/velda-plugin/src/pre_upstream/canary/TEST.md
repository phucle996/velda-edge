# Canary Plugin Verification & Test Matrix

## 1. Unit & Static Analysis Tests

| Scope | Location | Verified Invariants | Command |
| :--- | :--- | :--- | :--- |
| **Plugin Behavior (3 tests)** | `crates/velda-plugin/src/pre_upstream/canary/plugin.rs` | 1. Rule override forces `selected_upstream = Some(id)` even at `ratio = 0`.<br>2. Sticky `Priority` determinism across 50 identical requests + `hash_to_permille` distribution.<br>3. `AccessLevel::Read` leaves `HeaderWorkspace` 100% unmodified + `u16::MAX` sentinel safety. | `cargo test -p velda-plugin --lib canary` |
| **Schema Validation (138 tests)** | `crates/velda-plugin/src/pre_upstream/canary/validation.rs` & `validation` module | Rejects `ratio > 1000`, invalid HTTP header names (`HeaderName::from_bytes`), empty rule values for value-matching operators, empty `canary_upstream`, missing `hash_headers` on `priority`/`composite`, and unknown extra JSON fields. | `cargo test -p velda-plugin --lib validation` |
| **Hot-Path ASM Invariant** | `target/release/deps/velda_plugin-*.s` | Confirms `0` `alloc::*` heap calls, `0` `divq`, `0` `ud2`, `0` `lock *`, and BMI2 `mulxq $1000` in `CanaryPlugin::on_upstream_request`. | `audit_asm.py <file.s> --symbol ...` |

---

## 2. Control Plane Sync & Upstream Wiring Verification

| Test Case | Verified Component | Result |
| :--- | :--- | :--- |
| **`canary` Canonical ID Rejection** | `velda-sync` (`plugins.json`) | Rejected non-canonical `"id": "canary_main"` safely while preserving LKG state (`Validation error in plugins: Unknown plugin ID 'canary_main'`). |
| **Dense `u16` Index Resolution** | `velda-plugin::wire_plugin_upstreams` + `velda-edge/src/runtime/plugin.rs` | Resolved `backend-canary` -> `upstream_id=1` at boot (`revision=1`) when placed second in `upstreams.json`. |
| **Adversarial Hot-Reload Re-Index** | `velda-edge/src/reload.rs` | Reordered `upstreams.json` placing `backend-canary` at index `0` and `backend-base` at index `1` (`revision=3`). Edge reloaded atomically, logged `upstream_id=0`, and routed rule-miss -> `:18081` (`base`) and rule-hit -> `:18091` (`canary`). |

---

## 3. Isolated Dual-Node KVM End-to-End Test Matrix

Executed live between `velda-obs` (`wrk` / `curl` -> `velda-gw:18080`) and dual symmetric HTTP/1.1 backends (`base` on `192.168.122.20:18081`, `canary` on `192.168.122.20:18091`):

| Test Case | Setup | Assertion | Measured Result |
| :--- | :--- | :--- | :--- |
| **Baseline Miss** | `ratio: 0`, no rule headers | Requests route to `backend-base`. | `200 base` |
| **5 Rule Operators** | `ratio: 0`, matching `equals`, `prefix`, `suffix`, `contains`, `exists` headers | All 5 matchers route `100%` to `backend-canary`. | `equals: canary`, `prefix: canary`, `suffix: canary`, `contains: canary`, `exists: canary` |
| **Zero Header Mutation** | `GET /inspect` with `X-Canary-Eq: true`, `X-User-Id: alice-99` | Upstream receives exact headers without proxy-injected canary headers. | Backend reflected exact client headers on `upstream: "canary"` |
| **2,000-User Split Accuracy** | `ratio: 200`, `hash_strategy: "priority"`, `2,000` distinct `X-User-Id`s (`prod-user-00000..01999`) | Distribution converges near `200‰` (`20.0%`). | `base=1566`, `canary=434` (`217.00‰`); under `wrk` 615k reqs: **`196.25‰`** |
| **Session Stickiness** | `20` repeated requests each for `prod-user-00000` (base) and `prod-user-00001` (canary) | Zero cross-upstream drift for identical key. | `prod-user-00000 -> {'base'}`, `prod-user-00001 -> {'canary'}` |
| **Composite Split Accuracy** | `ratio: 200`, `hash_strategy: "composite"`, `["x-tenant-id", "x-user-id"]`, `569k` reqs | Composite fold across multiple headers matches target ratio. | `base=452768`, `canary=116426` (**`204.55‰`**) |

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
