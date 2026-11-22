# HTTPS Redirect (`https_redirect`) — Test Matrix & Verification Report

All unit tests for `https_redirect` are executed via:
```bash
cargo test -p velda-plugin https_redirect
```

## Test Coverage Summary (3 / 3 Unit Tests + 5 Live KVM Probes Passing)

### 1. Unit Tests (`plugin.rs` — 3 Tests)
- `test_https_redirect_passes_when_already_https`: Verifies `x-forwarded-proto: https` and `https://` URI scheme return `Action::Continue`.
- `test_https_redirect_upgrades_http_to_https_308_and_301`: Verifies `308` and `301` responses, port replacement (`:80` stripped, `:8443` appended), and IPv6 bracket preservation (`[2001:db8::1]`).
- `test_https_redirect_rejects_missing_host_and_invalid_config`: Verifies missing `Host` returns `RejectReason::BadRequest` and invalid `status_code: 302` fails config validation.

### 2. Live KVM Adversarial Probes (`velda-gw` — `192.168.122.10:8080`)
- Spoofed `X-Forwarded-Proto: https` on plaintext port `8080` $\to$ `308 Permanent Redirect` (not bypassed).
- Missing `Host` header $\to$ `400 Bad Request`.
- Malformed IPv6 `[::1]abc` $\to$ `400 Bad Request`.
- Valid IPv6 `[2001:db8::1]:8080` $\to$ `308 Permanent Redirect` to `https://[2001:db8::1]:8443/v1?x=1`.
- Oversized `4,000`-byte URI (`> 2048B` stack buffer) $\to$ `308 Permanent Redirect` (`Location` length `4038`).
