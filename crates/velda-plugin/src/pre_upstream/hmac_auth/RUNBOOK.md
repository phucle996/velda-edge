# HMAC Authentication Plugin (`hmac_auth`) — Operational Runbook

Operational guide for configuring, rotating credentials, monitoring rejections, and triaging production issues with `hmac_auth`.

---

## 1. Execution & Dependency Behavior

`hmac_auth` executes inline on Frontline Traffic Workers (`velda-worker-{i}`) during `pre_upstream` on `SupportedProtocols::ALL_HTTP` routes. When `validate_request_body` is `true`, `capability()` requests `AccessLevel::Read` on the body; when `false`, it runs header-only.

Every rejection calls `reject_unauthorized`, attaching `www-authenticate: HMAC realm="velda-edge"` as a bypass header and returning `Action::Reject(RejectReason::Unauthorized)` (`401 Unauthorized` at protocol layer).

---

## 2. Parameter Reference Table

| Field | JSON Type | Required | Constraints / Allowed Values | Notes |
| :--- | :--- | :---: | :--- | :--- |
| `access_key_header` | `string` | Yes | Valid RFC 9110 `tchar` header name (`1..=256`B, no `:`) | Header carrying caller `access_key_id`. |
| `signature_header` | `string` | Yes | Valid RFC 9110 `tchar` header name | Header carrying `hex` (case-insensitive) or `base64` signature. |
| `signature_algorithm` | `string` | Yes | `"hmac-sha256"` or `"hmac-sha512"` (normalized lowercase) | Determines `hmac::Key` algorithm and expected digest size (`32`B / `64`B). |
| `signature_encoding` | `string` | Yes | `"hex"` or `"base64"` (normalized lowercase) | Wire encoding checked before SHA-256 / HMAC (`44`/`64`/`88`/`128` chars). |
| `timestamp_header` | `string` | Yes | Valid RFC 9110 `tchar` header name; **must appear in `signed_headers`** | Unix epoch seconds ASCII unsigned integer. |
| `clock_skew_seconds` | `u64` | Yes | `1..=86400` | Maximum allowed `|req.ingress_time_sec() - req_timestamp|`. |
| `signed_headers` | `string[]` | Yes | Non-empty, valid RFC 9110 header names, no duplicates | Must include `timestamp_header`, and `nonce_header` when `enforce_nonce == true`. |
| `validate_request_body` | `bool` | Yes | `true` or `false` | Enables body SHA-256 computation & `content_sha256_header` check; `false` signs `UNSIGNED-PAYLOAD`. |
| `content_sha256_header` | `string` | Yes | Valid RFC 9110 `tchar` header name | Required field; verified against actual body SHA-256 when `Body::Bytes` is present, or used for unbuffered payloads. |
| `nonce_header` | `string` | Yes | Valid RFC 9110 `tchar` header name; must be in `signed_headers` if `enforce_nonce == true` | Carries single-use caller nonce string. |
| `enforce_nonce` | `bool` | Yes | `true` or `false` | Only allocates `NonceCache` when `true`. |
| `credentials` | `object[]` | Yes | Non-empty; unique trimmed `access_key_id`; `secret_key` length `>= 16` | Swapped atomically via `HmacAuthPlugin` reload. |

### `credentials[]` Entry Fields

| Field | JSON Type | Required | Constraints | Notes |
| :--- | :--- | :---: | :--- | :--- |
| `access_key_id` | `string` | Yes | Non-empty after trim; unique across array | Public lookup identifier. |
| `secret_key` | `string` | Yes | `len >= 16` and `trim().len() >= 16` | Shared secret pre-compiled into `ring::hmac::Key`. |
| `consumer_id` | `string \| null` | No | Valid HTTP header value if `Some` | Injected as `x-consumer-id` on success; when `null`, any client-supplied `x-consumer-id` is removed. |

---

## 3. Deployment Recipes

### 3.1 Strict Financial / Payment API (`SHA-256` + Body Digest + Nonce)

```json
{
  "id": "hmac_auth",
  "config": {
    "access_key_header": "x-access-key-id",
    "signature_header": "x-signature",
    "signature_algorithm": "hmac-sha256",
    "signature_encoding": "hex",
    "timestamp_header": "x-timestamp",
    "clock_skew_seconds": 30,
    "signed_headers": ["host", "x-timestamp", "x-nonce", "x-content-sha256"],
    "validate_request_body": true,
    "content_sha256_header": "x-content-sha256",
    "nonce_header": "x-nonce",
    "enforce_nonce": true,
    "credentials": [
      {
        "access_key_id": "vnpay_live_key_01",
        "secret_key": "0123456789abcdef0123456789abcdef",
        "consumer_id": "vnpay-prod"
      },
      {
        "access_key_id": "vnpay_live_key_02",
        "secret_key": "fedcba9876543210fedcba9876543210",
        "consumer_id": "vnpay-prod"
      }
    ]
  }
}
```

### 3.2 Key Rotation Pattern

To rotate keys without breaking upstream identity or rate limits:
1. Add the new `access_key_id` + `secret_key` alongside the existing key, sharing the same `consumer_id`.
2. Reload gateway snapshot; upstream services continue receiving the same `x-consumer-id`.
3. Remove the retired `access_key_id` once partner traffic migrates.

*(To hide `x-access-key-id`, `x-signature`, or `x-nonce` from upstream after authentication, place `header_mutation` after `hmac_auth` with those names in `remove_headers`.)*

---

## 4. Observability Signals

- **Wire Response on Rejection**: `401 Unauthorized` with `www-authenticate: HMAC realm="velda-edge"`. Detailed failure reasons (bad key vs bad timestamp vs bad signature vs replayed nonce) are intentionally omitted from the wire response to avoid oracle leakage.
- **Internal Rejection Metric**: Tagged via `RejectReason::Unauthorized` in downstream/observability counters.

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Valid partner requests fail with `401 Unauthorized` | Clock drift between client and gateway exceeds `clock_skew_seconds`, or query string parameters were not sorted lexicographically. | Verify client NTP sync, confirm timestamp is Unix epoch seconds, and verify query sorting (`a=1&b=2`). |
| Streaming POST fails even with valid HMAC | Client provided `x-content-sha256` mismatching actual buffered `Body::Bytes` SHA-256, or empty body header mismatched `EMPTY_SHA256_HEX`. | Recompute SHA-256 on exact wire body bytes; lowercase/uppercase hex are both accepted on `x-content-sha256` and `x-signature`. |
| Valid HTTP/2 requests fail signature check | Client signed `:authority` without sending `Host`, or signed `Host` matching `:authority`. | Ensure `"host"` is in `signed_headers` and client signs the exact `:authority` value when `Host` header is absent. |
| Bursty valid traffic triggers unexpected `401` with `enforce_nonce: true` | Single shard hit `MAX_ENTRIES_PER_SHARD` (`8192`) of unexpired nonces, or client retried using the exact same `x-nonce`. | Ensure clients generate a fresh unique nonce per retry and keep `clock_skew_seconds` small (e.g. `30..60`s). |
| Config reload fails in `velda-sync` | Unknown JSON key (`deny_unknown_fields`), `secret_key` < 16 bytes, or missing `timestamp_header` / `nonce_header` inside `signed_headers`. | Correct JSON schema and ensure `signed_headers` covers all anti-replay headers. |
