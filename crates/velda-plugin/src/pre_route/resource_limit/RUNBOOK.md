# HTTP Request Resource Limiter (`resource_limit`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers configuring, operating, and troubleshooting the `resource_limit` pre-route plugin in Velda Edge.

---

## 1. Configuration Parameter Reference

`resource_limit` enforces strict memory and structural bounds on incoming HTTP request heads prior to routing, regex/WAF scanning, or body streaming (`pre_route`).

```json
{
  "id": "resource_limit",
  "config": {
    "max_uri_length": 8192,
    "max_headers_count": 128,
    "max_header_bytes": 32768,
    "max_single_header_bytes": 16384
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Valid Range | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`max_uri_length`** | `usize` | **Yes** | `64 ..= 65,536` bytes | Maximum allowed length of URI Path + Query string. Exceeding returns `Action::Reject(RejectReason::UriTooLong)` (HTTP `414 URI Too Long`). |
| **`max_headers_count`** | `usize` | **Yes** | `8 ..= 1,024` entries | Maximum total number of HTTP header lines permitted. Exceeding returns `Action::Reject(RejectReason::HeaderFieldsTooLarge)` (HTTP `431`). |
| **`max_header_bytes`** | `usize` | **Yes** | `512 ..= 1,048,576` bytes | Maximum cumulative byte size across all header names and values. Exceeding returns HTTP `431`. |
| **`max_single_header_bytes`** | `usize` | **Yes** | `64 ..= max_header_bytes` | Maximum byte size for any individual header value (e.g. oversized `Cookie` or `Authorization` JWT). Exceeding returns HTTP `431`. |

---

## 2. Scenario-Based Configuration Recipes

### Scenario A: Strict Public REST API Gateway (Anti-DoS)
*Goal: Tight perimeter bounds before `waf_lite` and `jwt_auth`.*

```json
{
  "id": "strict_api_limits",
  "config": {
    "max_uri_length": 4096,
    "max_headers_count": 64,
    "max_header_bytes": 16384,
    "max_single_header_bytes": 8192
  }
}
```
* **Recommended Execution Order**: Place `resource_limit` immediately after `ip_filter` and before `waf_lite` so oversized URIs (`414`) and header floods (`431`) are rejected in O(1) before `waf_lite` scans strings.

### Scenario B: Enterprise SSO / Browser Gateway (Large Cookies & OAuth Redirects)
*Goal: Accommodate large SAML/OIDC state query parameters and multi-cookie browser sessions.*

```json
{
  "id": "sso_gateway_limits",
  "config": {
    "max_uri_length": 16384,
    "max_headers_count": 128,
    "max_header_bytes": 65536,
    "max_single_header_bytes": 32768
  }
}
```

---

## 3. Observability & Troubleshooting Playbook

### Telemetry & Access Log Signals
- **URI Exceeded**: `RejectReason::UriTooLong` $\to$ HTTP `414 URI Too Long`, gRPC `InvalidArgument`.
- **Headers Exceeded**: `RejectReason::HeaderFieldsTooLarge` $\to$ HTTP `431 Request Header Fields Too Large`, gRPC `InvalidArgument`.

### Incident Triage

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| OAuth/OIDC callback URLs fail with `414 URI Too Long` | IdP returns long `code` / `state` / `id_token` query parameters exceeding `max_uri_length`. | Increase `max_uri_length` (e.g., from `4096` to `16384`) on the auth callback route. |
| Browser requests fail with `431` after SSO login | Accumulated session/tracking cookies exceed `max_single_header_bytes` (`Cookie` header) or `max_header_bytes`. | Increase `max_single_header_bytes` (must remain `<= max_header_bytes`) and `max_header_bytes`. |
| Config rejected by `velda-sync` | `max_single_header_bytes > max_header_bytes` or threshold outside allowed range (`64..=65536` for URI, `8..=1024` for count). | Ensure `max_single_header_bytes <= max_header_bytes` and all values fall within documented bounds. |
