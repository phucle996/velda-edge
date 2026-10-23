# CSRF Protection Plugin (`csrf`) — Test & Verification Report

This document records the official verification methodology, attack simulations, and empirical results for the **`csrf`** pre-upstream plugin.

---

## 1. Objectives & Threat Model

The `csrf` test harness validates defense against the following vectors:
- **W3C Simple Request Form Trap**: Malicious sites submitting `<form action="..." method="POST" enctype="text/plain">` without triggering CORS preflights.
- **Subdomain Cookie Tossing (RFC 6265 §8.5)**: Adversaries possessing an unprivileged subdomain attempting to inject forged CSRF cookies.
- **Timing Side-Channel Exploits**: Byte-by-byte signature deduction attacks.
- **Token Tampering & Expiration**: Manipulating timestamp bits or using expired tokens.
- **Safe Method Bypasses**: Ensuring zero degradation or interference with idempotent requests (`GET`, `HEAD`, `OPTIONS`).

---

## 2. Test Verification Matrix

| Test Case | Scenario / Attack Vector | Cookie State | Header State | Method & Metadata | Expected Result | Status |
| :---: | :--- | :--- | :--- | :--- | :---: | :---: |
| **C1** | **Safe Method Bypass** | *(None)* | *(None)* | `GET`, `HEAD`, `OPTIONS` | **PASS (Action::Continue)** | **PASS** |
| **C2** | **Simple Request Form Attack** | Valid Cookie | *(Missing Header)* | `POST`, `cross-site` | **REJECTED (403 Forbidden)** | **PASS** |
| **C3** | **Legitimate Double-Submit** | `aurora_csrf=token123` | `X-CSRF-Token: token123` | `POST`, `same-origin` | **ACCEPTED (Action::Continue)** | **PASS** |
| **C4** | **Token Mismatch Attempt** | `csrf=token_victim` | `X-CSRF-Token: token_evil` | `POST`, `same-origin` | **REJECTED (403 Forbidden)** | **PASS** |
| **C5** | **Subdomain Cookie Tossing** | Forged matching token | Forged matching header | `POST` (Invalid HMAC) | **REJECTED (403 Forbidden)** | **PASS** |
| **C6** | **Expired Signed Token** | Token older than TTL | Token older than TTL | `POST` (Valid HMAC, Expired) | **REJECTED (403 Forbidden)** | **PASS** |
| **C7** | **Tampered Signature Byte** | 1 bit altered in token | Matching altered token | `POST` (Signature mismatch) | **REJECTED (403 Forbidden)** | **PASS** |
| **C8** | **Untrusted Cross-Site Origin** | *(Any)* | *(Any)* | `POST`, `Sec-Fetch-Site: cross-site` | **REJECTED (403 Forbidden)** | **PASS** |
| **C9** | **Trusted Cross-Site Partner** | Valid signed token | Matching signed header | `POST`, `Sec-Fetch-Site: cross-site` | **ACCEPTED (Action::Continue)** | **PASS** |
| **C10**| **Origin / Host Mismatch** | Valid token | Valid token | `POST`, `Origin: https://evil.com` | **REJECTED (403 Forbidden)** | **PASS** |

---

## 3. Multi-Phase Pipeline Integration Verification

In [`crates/velda-plugin/tests/csrf_suite_test.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/tests/csrf_suite_test.rs), the CSRF plugin was verified alongside `cors_preflight` through a compiled [`RoutePluginPlan`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/plan.rs).

### Attack Scenario Execution:
```http
POST /api/transfer HTTP/1.1
Host: api.example.com
Origin: https://evil-attacker.com
Content-Type: text/plain
Sec-Fetch-Site: cross-site
Cookie: __Host-velda_csrf=some_token

{"amount": 5000000, "to": "attacker"}
```

**Observed Pipeline Response:**
1. **`pre_route: cors_preflight`**:
   - Request is `POST`, not `OPTIONS`.
   - `cors_preflight` returns `Action::Continue` (as mandated by W3C CORS).
2. **`pre_upstream: csrf`**:
   - Intercepts the request prior to upstream forwarding.
   - Detects missing `X-CSRF-Token` header and `Sec-Fetch-Site: cross-site`.
   - **Immediately terminates request with `403 Forbidden` (`Bytes::from_static`)**.
   - Upstream backend receives 0 requests; financial state remains completely unmutated.

---

## 4. Test Suite Coverage Summary

| Test File | Test Focus | Cases | Status |
| :--- | :--- | :---: | :---: |
| `crates/velda-sync/src/post_sync/plugin/pre_upstream/csrf.rs` | Schema validation, unsafe method rejection, secret key length | 3 | **PASS** |
| `crates/velda-plugin/src/pre_upstream/csrf/plugin.rs` | Unit tests: bitmask bypass, double-submit, signed token lifecycle, cookie tossing defense, Fetch Metadata | 5 | **PASS** |
| `crates/velda-plugin/tests/csrf_suite_test.rs` | Multi-phase pipeline test with `cors_preflight`, simple request neutralization, origin matching | 3 | **PASS** |
| **Total** | **Comprehensive CSRF Protection Verification** | **11** | **100% PASS** |
