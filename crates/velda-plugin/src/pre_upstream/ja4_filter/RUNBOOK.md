# JA4 TLS Fingerprint Filter (`ja4_filter`) — Operations Runbook

## 1. Parameter Reference

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `deny` | `Vec<String>` | **Yes** (non-empty) | List of JA4 fingerprints to reject with `403 Forbidden` (HTTP) / `PERMISSION_DENIED` (gRPC). |

> [!IMPORTANT]
> **Strict Deny-Only & Route-Scoped Design**:
> `ja4_filter` has **no** `mode` (`allow`/`deny`) and **no** internal `include_routes` / `exclude_routes` fields. Attach `ja4_filter` only to the specific routes in `routes.json` that require bot/scanner mitigation.

---

## 2. Accepted Fingerprint Formats

Each entry in `deny` accepts either format:

1. **Canonical 36-character FoxIO JA4 string**:
   - Format: `<ja4_a (10 chars)>_<ja4_b (12 hex)>_<ja4_c (12 hex)>`
   - Example: `"t13d1516h2_8daaf6152771_e5627efa2ab1"`
   - Compiled at config load time via `velda_core::ja4_fingerprint_id()`.
2. **Pre-hashed 64-bit Hex ID (`0x...`)**:
   - Format: `"0x"` followed by 1 to 16 hex digits (non-zero).
   - Example: `"0xdeadbeefcafebabe"`

---

## 3. Scenario-Based Configuration Recipes

### Recipe A: Protect Web Login While Keeping APIs Open
- `plugins.json`:
  ```json
  {
    "id": "ja4_filter",
    "config": {
      "deny": [
        "t13d190900_9dc949149365_97f8aa674fd9",
        "t13i3012h2_1d37bd780c83_8537cf56674e"
      ]
    }
  }
  ```
- `routes.json`: Attach `"plugins": ["ja4_filter"]` to `/login` and `/checkout`, and omit it from `/api/webhook` (so partner Python/Go clients are unaffected).

---

## 4. Observability Signals

| Signal | External Wire Output | Internal Semantic |
| :--- | :--- | :--- |
| **HTTP/1.1, H2, H3 Reject** | `403 Forbidden` (`cache-control: no-store`, `x-request-id`) | `RejectReason::Forbidden` |
| **gRPC Reject** | `:status: 200`, `grpc-status: 7 (PERMISSION_DENIED)` | `RejectReason::Forbidden` |

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| **Legitimate client blocked with 403** | Client's TLS library (e.g., corporate proxy or custom SDK) shares a JA4 fingerprint in `deny`. | Remove that JA4 entry from `deny`, or detach `ja4_filter` from the API route used by that SDK. |
| **Plaintext HTTP traffic not blocked** | Plaintext listeners (`http-in`) have `RequestState.ja4 == 0`, which intentionally bypasses `ja4_filter`. | Pair with `https_redirect` in `pre_route` to force clients onto HTTPS listeners. |
| **Config validation fails on startup** | Empty `deny: []`, `0x0`, or malformed JA4 string (not 36 chars with `_` at offsets 10 and 23). | Fix the malformed entry in `plugins.json` and re-run `velda-sync`. |
