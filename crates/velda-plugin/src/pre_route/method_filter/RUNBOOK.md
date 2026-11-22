# HTTP Method Filter (`method_filter`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers configuring, operating, and troubleshooting `method_filter` in Velda Edge.

---

## 1. Configuration Parameter Reference

`method_filter` enforces a **100% Explicit Allowlist Invariant**: only HTTP methods listed in `allowed_methods` are permitted; all other methods (including `TRACE`, `TRACK`, `CONNECT`, `DEBUG`, and WebDAV verbs) are rejected immediately with `405 Method Not Allowed`.

```json
{
  "id": "method_filter",
  "phase": "pre_route",
  "config": {
    "allowed_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Constraints | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`allowed_methods`** | `Vec<String>` | **Yes** | Non-empty array of valid RFC 9110 HTTP tokens (auto-uppercased, sorted, deduplicated) | Standard methods (`GET`..`TRACE`) compile into a 16-bit bitmask. Non-standard methods (e.g. `PROPFIND`) compile into a sorted fallback list. |

---

## 2. Scenario-Based Configuration Recipes

### Scenario A: Standard Browser-Facing REST API (With CORS)
*Goal: Allow full CRUD verbs plus `OPTIONS` preflight, while blocking `TRACE`, `CONNECT`, and WebDAV.*

```json
{
  "id": "rest_api_methods",
  "phase": "pre_route",
  "config": {
    "allowed_methods": ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
  }
}
```

### Scenario B: Read-Only Public Static / CDN Edge
*Goal: Maximum attack surface reduction for static assets or read-only GET APIs.*

```json
{
  "id": "readonly_edge_methods",
  "phase": "pre_route",
  "config": {
    "allowed_methods": ["GET", "HEAD"]
  }
}
```

---

## 3. Observability & Troubleshooting Playbook

### Telemetry & Access Log Signals
- **Wire Response**: HTTP `405 Method Not Allowed` (`b"405 Method Not Allowed\n"`).
- **Internal Access Log**: `completion=reject:method_not_allowed`
- **Prometheus Metrics**: `velda_requests_rejected_total{reason="method_not_allowed"}`

### Incident Triage

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Browser CORS `OPTIONS` preflights fail with `405` | `method_filter` ran before `cors_preflight` and `"OPTIONS"` was omitted from `allowed_methods`. | Add `"OPTIONS"` to `allowed_methods` or place `cors_preflight` before `method_filter`. |
| Config rejected by `velda-sync` | `allowed_methods` is empty `[]` or contains spaces/invalid characters. | Supply at least one valid RFC 9110 method token. |
