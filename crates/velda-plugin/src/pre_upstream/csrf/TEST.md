# CSRF Protection Verification & Test Matrix

## 1. Unit & Static Analysis Tests

| Scope | Location | Verified Invariants | Command |
| :--- | :--- | :--- | :--- |
| **Modern Fetch Metadata & Same-Site (1 test)** | `crates/velda-plugin/src/pre_upstream/csrf/plugin.rs` | Safe `GET`/`HEAD`/`OPTIONS` bypass; `same-origin` & `none` pass without cookies/tokens; `cross-site` & strict `same-site` return `403 Forbidden`; explicit `trusted_origins` passes; `allow_same_site: true` permits `same-site`. | `cargo test -p velda-plugin --lib csrf` |
| **Legacy Origin/Referer & H2 Authority Fallback (1 test)** | `crates/velda-plugin/src/pre_upstream/csrf/plugin.rs` | Verifies H1 `Host` + `Origin`, HTTP/2 URI `:authority` without `Host`, valid `Referer` with `?query#frag`, fail-closed rejection on malformed userinfo (`https://evil.com@app.example.com/`), rejection on cookie-only requests lacking provenance, and clean bypass for cookie-less Bearer API clients. | `cargo test -p velda-plugin --lib csrf` |
| **Legacy Double-Submit & Multi-Cookie H2 (1 test)** | `crates/velda-plugin/src/pre_upstream/csrf/plugin.rs` | Verifies scanning across multiple `Cookie` header lines (`req.headers.get_all(COOKIE)`), matching `XSRF-TOKEN == X-XSRF-TOKEN` passes `200`, mismatch/missing cookie returns `403`. | `cargo test -p velda-plugin --lib csrf` |
| **Schema Validation (2 tests + 135 validation suite)** | `crates/velda-plugin/src/pre_upstream/csrf/validation.rs` | Accepts modern & legacy `double_submit` configs; rejects unsafe `POST` in `safe_methods`, wildcards (`*`, `https://*`, `https://*.com`), invalid headers/cookies, and removed legacy fields (`secret_key`, etc.). | `cargo test -p velda-plugin --lib validation` |
| **Hot-Path ASM Invariant** | `target/release/deps/velda_plugin-*.s` | Confirms `0` `alloc::*`, `0` `lock *`, `0` `clock_gettime`, `0` `hmac`/`ring` calls, `507` instructions after `#[cold]` splitting of `verify_legacy_origin_fallback` and `verify_double_submit`. | `audit_asm.py <file.s> --symbol ...` |

---

## 2. Isolated Dual-Node KVM End-to-End Verification

Executed live between `velda-obs` (Python HTTP client + raw TCP socket client → `velda-gw:18080`) and `velda-obs:8081` (`velda-bench-backend`):

| Test Group | Assertions Executed | Result |
| :--- | :--- | :---: |
| **Modern + Legacy Provenance Suite (`11/11`)** | `GET /ping` (`200`), `POST` `same-origin` (`200`), `none` (`200`), untrusted `cross-site` (`403`), strict `same-site` (`403`), trusted partner (`200`), legacy `Origin == Host` (`200`), `Referer` (`200`), `@` userinfo spoofed `Referer` (`403`), cookie-only missing provenance (`403`), Bearer API client with no cookie (`200`). | **PASS (11/11)** |
| **Double-Submit Multi-Cookie Split Lines (`3/3`)** | Hot-reloaded `double_submit` config (`manifest revision=2`); split `Cookie: session=user_1` + `Cookie: XSRF-TOKEN=tok_ok_99` with matching header (`200`), mismatched header (`403`), missing header (`403`). | **PASS (3/3)** |

---

## 3. Reproducing Checks Locally

```bash
cargo fmt --all --check
cargo clippy -p velda-plugin -p velda-sync -p velda-edge --all-targets --all-features -- -D warnings
cargo test -p velda-plugin --lib csrf
cargo test -p velda-plugin --lib validation
cargo test -p velda-sync --lib post_sync::plugin
```
