# HMAC Authentication Plugin (`hmac_auth`)

High-performance, zero-IO Hash-based Message Authentication Code verification plugin conforming to RFC 2104. Evaluated during the `pre_upstream` phase after route matching. Guarantees message integrity, caller identity, and absolute anti-replay protection via timestamp drift windows and sharded in-memory nonce tracking.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `hmac_auth` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Pre-upstream FIFO pipeline (evaluated before upstream dispatch) |
| **Access Level** | `PluginCapability::new(AccessLevel::ReadWrite, SupportedProtocols::ALL_L7, false)` |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Nanosecond-level; zero-IO, in-memory credential lookup, constant-time verification |
| **Memory Invariant** | Lock-striped sharded nonce set with automatic time-to-live eviction |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `access_key_header` | String | **Yes** | Valid RFC 9110 header name | `"x-access-key-id"` | Header containing caller's public access key ID. |
| `signature_header` | String | **Yes** | Valid RFC 9110 header name | `"x-signature"` | Header carrying the client's computed HMAC signature. |
| `signature_algorithm` | String | **Yes** | `"hmac-sha256"` or `"hmac-sha512"` | `"hmac-sha256"` | Cryptographic HMAC hashing algorithm. |
| `signature_encoding` | String | **Yes** | `"hex"` or `"base64"` | `"hex"` | Wire format encoding of the incoming signature. |
| `timestamp_header` | String | **Yes** | Valid RFC 9110 header name | `"x-timestamp"` | Header carrying request timestamp (Unix epoch seconds). |
| `clock_skew_seconds` | u64 | **Yes** | Integer between `1` and `86400` | `60` | Maximum allowable time drift ($\pm \Delta t$) before request is rejected. |
| `signed_headers` | Array of strings | **Yes** | Array of lowercase header names | `["host", "x-timestamp", "x-nonce"]` | List of headers participating in the canonical signature. Must include `timestamp_header` (and `nonce_header` if `enforce_nonce: true`). |
| `validate_request_body` | Boolean | **Yes** | `true` or `false` | `true` | When `true`, computes SHA-256 payload digest to guarantee request body integrity. |
| `content_sha256_header` | String | **Yes** | Valid RFC 9110 header name | `"x-content-sha256"` | Header containing pre-computed SHA-256 payload digest for streaming requests. |
| `nonce_header` | String | **Yes** | Valid RFC 9110 header name | `"x-nonce"` | Header carrying unique single-use request nonce string. |
| `enforce_nonce` | Boolean | **Yes** | `true` or `false` | `true` | Enables in-memory single-use nonce tracking to eliminate replays within clock skew. |
| `credentials` | Array of objects | **Yes** | Non-empty array of credential objects | See schema below | Authorized caller keys pre-compiled directly into RAM. |

### Credential Object Schema

| Field | Type | Required | Description |
| :--- | :---: | :---: | :--- |
| `access_key_id` | String | **Yes** | Unique public client/caller identifier (e.g. `"partner_fintech_01"`). |
| `secret_key` | String | **Yes** | Shared secret key (minimum 16 characters). |
| `consumer_id` | String | No (null) | Logical consumer identity injected into upstream request via `x-consumer-id`. |

---

## 3. Canonical Signature Specification

Clients must construct the exact canonical string before computing HMAC:

```text
HTTP_METHOD\n
CANONICAL_URI\n
CANONICAL_QUERY_STRING\n
CANONICAL_HEADERS\n
SIGNED_HEADERS_LIST\n
PAYLOAD_HASH
```

1. **`HTTP_METHOD`**: Uppercase string (e.g. `POST`, `GET`).
2. **`CANONICAL_URI`**: Absolute URI path (e.g. `/v1/transfers`).
3. **`CANONICAL_QUERY_STRING`**: Alphabetically sorted key-value query pairs (`key=val` joined with `&`).
4. **`CANONICAL_HEADERS`**: For each header declared in `signed_headers` (in declared order), formatted as `name:value\n` with trimmed values.
5. **`SIGNED_HEADERS_LIST`**: Semicolon-separated list of signed header names (e.g. `host;x-nonce;x-timestamp`).
6. **`PAYLOAD_HASH`**:
   - If `validate_request_body: true`: Hex-encoded SHA-256 digest of request body (`e3b0c442...` for empty body).
   - If `validate_request_body: false`: Literal string `"UNSIGNED-PAYLOAD"`.

---

## 4. Production Manifest Example (`plugins.json`)

```json
{
  "id": "hmac_auth_prod",
  "phase": "pre_upstream",
  "config": {
    "access_key_header": "x-access-key-id",
    "signature_header": "x-signature",
    "signature_algorithm": "hmac-sha256",
    "signature_encoding": "hex",
    "timestamp_header": "x-timestamp",
    "clock_skew_seconds": 60,
    "signed_headers": [
      "host",
      "x-timestamp",
      "x-nonce"
    ],
    "validate_request_body": true,
    "content_sha256_header": "x-content-sha256",
    "nonce_header": "x-nonce",
    "enforce_nonce": true,
    "credentials": [
      {
        "access_key_id": "client_fintech_partner_01",
        "secret_key": "sec_k89f2a019482bca881726c0981273412",
        "consumer_id": "fintech-partner-prod"
      }
    ]
  }
}
```

---

## 5. Associated Engineering Documentation

> [!TIP]
> `hmac_auth` follows the standard 4-document specification layout for production edge plugins:
> - **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Deep hot-path design, Mermaid sequence diagrams, zero-branching dual layouts (`sign_canonical_h1` vs `sign_canonical_h2_h3`), single-pass `CanonicalBuffer`, 64-byte aligned `NonceCache`, and 128-bit SIMD constant-time verification.
> - **[TEST.md](./TEST.md)**: Testing methodology, adversarial attack scenarios (A3 body tampering, spoofing, replay attacks, unpoisoned nonces), test matrix, and the official Python Client Signing Reference Implementation.
> - **[BENCHMARK.md](./BENCHMARK.md)**: High-concurrency performance metrics (29,009.60 RPS @ 2.90ms), LuaJIT C-FFI OpenSSL load generator scripts, and CPU headroom normalization.
