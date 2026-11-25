# HMAC Authentication Plugin (`hmac_auth`)

Constant-time HMAC-SHA256 / HMAC-SHA512 request authentication plugin executed during the `pre_upstream` phase on HTTP traffic (`SupportedProtocols::ALL_HTTP`). Validates caller identity, request integrity (`METHOD`, `PATH`, sorted query, signed headers, and SHA-256 payload digest), timestamp drift, and optional single-use nonce anti-replay protection.

```text
+---------------------------------------------------------------------------------+
|                         HMAC AUTHENTICATION HOT PATH                            |
+---------------------------------------------------------------------------------+
|  L7Request (req.headers: HeaderWorkspace)                                       |
|    |                                                                            |
|    +---> [1] access_key_header (prehashed) ─► CredentialTable::lookup           |
|    |                                          (zero-hash AGU bit-shift / pext)  |
|    +---> [2] timestamp_header (prehashed)  ─► |ingress_sec - ts| <= clock_skew  |
|    +---> [3] nonce_header (if enforce_nonce) ─► compute 128-bit fingerprint     |
|    +---> [4] signature_header ─► decode hex/base64 onto stack (32B / 64B)       |
|    +---> [5] payload SHA-256  ─► body digest vs content_sha256_header / empty   |
|    +---> [6] streaming HMAC   ─► hmac::Context::with_key ─► constant_time_eq    |
|    +---> [7] NonceCache       ─► single-lock check_and_record_fp (post-verify)  |
|    +---> [8] x-consumer-id    ─► set precompiled value OR remove spoofed header |
|                                                                                 |
|  Any failure ─► insert_bypass(WWW-Authenticate) ─► Action::Reject(Unauthorized) |
+---------------------------------------------------------------------------------+
```

---

## 1. What This Plugin Does

- **Zero-Hash Static Credential Lookup**: Compiles configured `credentials` into `CredentialTable`, resolving `access_key_id` via distinguishing bit-shifts / BMI2 `pext` directly into AGU-indexed `Option<Box<CredentialEntry>>` pointer slots instead of runtime HashMap hashing.
- **Canonical Request Integrity Verification**: Streams `METHOD`, `PATH`, sorted query string, pre-hashed signed headers (with `:authority` fallback for HTTP/2 and HTTP/3), signed header list, and payload SHA-256 digest directly into `ring::hmac::Context`.
- **Anti-Replay Window & Nonce Tracking**: Rejects expired/future timestamps outside `clock_skew_seconds` and enforces single-use nonces inside a sharded `NonceCache` when `enforce_nonce` is enabled.
- **Authoritative Consumer Identity Injection**: Stamps pre-compiled `x-consumer-id` for downstream services when `consumer_id` is configured, or explicitly deletes any client-supplied `x-consumer-id` when `consumer_id` is `None`.

---

## 2. Where It Sits in Velda Edge

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `hmac_auth` |
| **Phase** | `pre_upstream` |
| **Worker Affinity** | Frontline Traffic Workers (`velda-worker-{i}`) |
| **PBWW (`pbww()`)** | `0` (pure inline hot path, no background tasks) |
| **Capability** | `(AccessLevel::Write, AccessLevel::Read)` if `validate_request_body == true`; otherwise `header_only(AccessLevel::Write)` |
| **Supported Protocols** | `SupportedProtocols::ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Failure Wire Action** | `Action::Reject(RejectReason::Unauthorized)` with bypass header `www-authenticate: HMAC realm="velda-edge"` |

---

## 3. Canonical Signature Specification

Clients sign the following canonical UTF-8/ASCII sequence using the shared `secret_key` (`HMAC-SHA256` or `HMAC-SHA512`):

```text
HTTP_METHOD\n
CANONICAL_PATH\n
CANONICAL_QUERY_STRING\n
CANONICAL_SIGNED_HEADERS\n
SIGNED_HEADERS_LIST\n
PAYLOAD_HASH
```

- `HTTP_METHOD`: Uppercase HTTP method (`GET`, `POST`, ...).
- `CANONICAL_PATH`: Request URI path (`/` when empty).
- `CANONICAL_QUERY_STRING`: Query `key=val` pairs sorted lexicographically by key then value and joined by `&` (empty line if no query).
- `CANONICAL_SIGNED_HEADERS`: Each header in `signed_headers` in declared order as `lowercase_name:trimmed_value\n` (comma-joined if multi-value; uses URI `:authority` for `host` on HTTP/2 & HTTP/3 when `Host` header is absent).
- `SIGNED_HEADERS_LIST`: Semicolon-separated lowercase `signed_headers` (e.g. `host;x-timestamp;x-nonce`).
- `PAYLOAD_HASH`: Lowercase SHA-256 hex digest of the body (`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` for empty body), or literal `UNSIGNED-PAYLOAD` when `validate_request_body` is `false`.

---

## 4. Quick Start Configuration

```json
{
  "id": "hmac_auth",
  "config": {
    "access_key_header": "x-access-key-id",
    "signature_header": "x-signature",
    "signature_algorithm": "hmac-sha256",
    "signature_encoding": "hex",
    "timestamp_header": "x-timestamp",
    "clock_skew_seconds": 60,
    "signed_headers": ["host", "x-timestamp", "x-nonce"],
    "validate_request_body": true,
    "content_sha256_header": "x-content-sha256",
    "nonce_header": "x-nonce",
    "enforce_nonce": true,
    "credentials": [
      {
        "access_key_id": "partner_fintech_01",
        "secret_key": "replace_with_32_byte_random_secret_key",
        "consumer_id": "fintech-partner-prod"
      }
    ]
  }
}
```

---

## 5. Documentation Directory

- **[Operational Runbook](RUNBOOK.md)**: Complete parameter reference, deployment recipes, observability signals, and incident triage playbook.
- **[Architectural Specification](ARCHITECTURE.md)**: `CredentialTable` bit-extraction / AGU dispatch, streaming `hmac::Context`, `NonceCache`, and security invariants.
- **[Performance Benchmark Report](BENCHMARK.md)**: 5-tier diagnostic matrix and verified `x86-64-v3` release assembly audit.
- **[Test Verification Report](TEST.md)**: `cargo test` commands and verified unit test catalog across all `hmac_auth` submodules.
