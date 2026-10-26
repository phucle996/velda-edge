# HTTP Fault Injection Plugin (`http_fault_injection`) — Test Plan & Security Verification

This document specifies the test methodology, unit test matrix, adversarial attack simulations, and live cluster empirical results for the **`http_fault_injection`** plugin.

---

## 1. Unit Test Matrix

The plugin contains 22 automated unit tests across `velda-plugin` and `velda-sync` covering core functionality, probability distributions, and threat defense vectors:

| Test Name | File | Description | Status |
| :--- | :--- | :--- | :---: |
| `test_fault_injection_scope_bypass` | `plugin.rs` | Verifies that non-matching IP traffic bypasses fault injection immediately. | **PASSED** |
| `test_fault_injection_scope_hit` | `plugin.rs` | Verifies that matching client IP and matching header trigger configured faults. | **PASSED** |
| `test_fault_injection_dynamic_override` | `plugin.rs` | Tests `x-fault-delay-ms` and `x-fault-abort-code` extraction, validation, and header purging. | **PASSED** |
| `test_from_config_complete_with_scope` | `plugin.rs` | Verifies end-to-end deserialization from `PluginConfig` JSON manifest. | **PASSED** |
| `test_extract_client_ip_anti_spoofing` | `condition.rs` | Tests that edge-authoritative `x-real-ip` takes precedence over untrusted `x-forwarded-for`. | **PASSED** |
| `test_ip_matcher_v4_intervals` | `condition.rs` | Tests disjoint interval compilation and binary search matching across IPv4 subnets. | **PASSED** |
| `test_ip_matcher_v6_intervals` | `condition.rs` | Tests disjoint interval compilation and matching across IPv6 `/64` and `/128` prefixes. | **PASSED** |
| `test_header_match_rule_present` | `condition.rs` | Verifies header presence matching regardless of value. | **PASSED** |
| `test_header_match_rule_exact` | `condition.rs` | Verifies exact string value matching for targeting headers. | **PASSED** |
| `test_dynamic_override_slowloris_clamp` | `condition.rs` | Asserts that `x-fault-delay-ms` is clamped to `max_override_delay_ms`. | **PASSED** |
| `test_dynamic_override_status_bounds` | `condition.rs` | Asserts that non-error abort codes (< 400 or > 599) are rejected. | **PASSED** |
| `test_abort_config_validation` | `abort.rs` | Tests validation of abort status codes strictly within `400..=599`. | **PASSED** |
| `test_abort_100_percent` | `abort.rs` | Asserts 100% deterministic hit rate when threshold is max. | **PASSED** |
| `test_abort_0_percent` | `abort.rs` | Asserts 0% hit rate when abort percentage is zero. | **PASSED** |
| `test_abort_override_takes_precedence` | `abort.rs` | Asserts that dynamic status code override bypasses percentage roll. | **PASSED** |
| `test_delay_config_validation` | `delay.rs` | Tests min/max delay validation and percentage bounds. | **PASSED** |
| `test_delay_fixed_range_100_percent` | `delay.rs` | Asserts deterministic fixed delay when min == max and percentage is 100%. | **PASSED** |
| `test_delay_zero_percent` | `delay.rs` | Asserts zero delay injected when percentage is 0%. | **PASSED** |
| `test_delay_override_takes_precedence` | `delay.rs` | Asserts that dynamic delay override bypasses percentage roll. | **PASSED** |
| `test_valid_http_fault_injection` | `sync/validate.rs` | Tests JSON schema validation and default scope compilation. | **PASSED** |
| `test_invalid_http_status_rejected` | `sync/validate.rs` | Tests that status 200 or 302 in abort configuration fails compilation. | **PASSED** |
| `test_allow_header_override_requires_scope` | `sync/validate.rs` | Verifies that open dynamic override without IP/header scope is rejected. | **PASSED** |

---

## 2. Adversarial Security Attack Scenarios

### 2.1 Client IP Spoofing Probe
- **Attack Vector**: An unauthenticated attacker attempts to bypass or trigger chaos injection by forging `X-Real-IP: 8.8.8.8` or `X-Forwarded-For: 1.1.1.1, 8.8.8.8`.
- **Observed Defense**: The gateway overrides `x-real-ip` with the physical socket peer address (`172.16.0.5`); the nearest proxy (right-most entry) is authoritatively evaluated. Spoofed headers are neutralized.

### 2.2 Token Leakage Defense
- **Attack Vector**: Dynamic chaos tokens (`x-fault-delay-ms`, `x-fault-abort-code`) leak into upstream backend microservices, causing unexpected behavior or security bypass in downstream systems.
- **Observed Defense**: Gateway evaluates tokens at ingress and purges them completely from `req.headers` before upstream forwarding (`/inspect` verification confirmed 0 leaked tokens).

### 2.3 Slowloris Worker Starvation Probe
- **Attack Vector**: A client sends `x-fault-delay-ms: 600000` (10 minutes) to hold worker threads or connection multiplexing streams open indefinitely.
- **Observed Defense**: System clamps all dynamic delays to `max_override_delay_ms` (defaults to 5,000ms), and denies dynamic overrides from un-scoped public IP addresses.

### 2.4 RFC 9110 Semantic Corruption Probe
- **Attack Vector**: Client injects `x-fault-abort-code: 200` with the abort plain-text body `"fault filter abort\n"`, corrupting downstream caching logic.
- **Observed Defense**: Range check strictly requires `400..=599`. Non-error status codes are rejected, and the request forwards upstream normally (`200 OK pong`).

---

## 3. Live Azure Cluster Empirical Verification

Live cURL reproduction executed from client Azure VM2 (`172.16.0.5`) against gateway Azure VM1 (`172.16.0.4`):

### 3.1 Scope Bypass (Normal Traffic)
```bash
curl -i http://172.16.0.4:80/api/fault/ping
```
```http
HTTP/1.1 200 OK
content-type: text/plain
content-length: 4
date: Mon, 26 Oct 2026 11:30:36 GMT
x-request-id: 000000006adf39dc000300000000003f

pong
```

### 3.2 Dynamic Delay Override
```bash
time curl -i -H "x-chaos-test: qa-stage" -H "x-fault-delay-ms: 300" http://172.16.0.4:80/api/fault/ping
```
```http
HTTP/1.1 200 OK
content-type: text/plain
content-length: 4
date: Mon, 26 Oct 2026 11:30:42 GMT
x-request-id: 000000006adf39e1000300000000003e

pong
```
*Measured Execution Time: `real 0m0.309s` (300ms injected delay + 9ms RTT).*

### 3.3 Dynamic Short-Circuit Abort
```bash
curl -i -H "x-chaos-test: qa-stage" -H "x-fault-abort-code: 504" http://172.16.0.4:80/api/fault/ping
```
```http
HTTP/1.1 504 Gateway Timeout
content-type: text/plain; charset=utf-8
x-velda-fault: abort
content-length: 19
x-request-id: 000000006adf39ee000200000000003d
cache-control: no-store

fault filter abort
```

### 3.4 Token Sanitization Upstream Reflection
```bash
curl -s -H "x-chaos-test: qa-stage" -H "x-fault-delay-ms: 100" http://172.16.0.4:80/api/fault/inspect | jq .
```
```json
{
  "headers": {
    "accept": "*/*",
    "host": "172.16.0.4",
    "x-chaos-test": "qa-stage",
    "x-forwarded-for": "172.16.0.5, 172.16.0.5",
    "x-forwarded-proto": "http",
    "x-real-ip": "172.16.0.5",
    "x-request-id": "000000006adf39e8000400000000003f"
  },
  "method": "GET",
  "path": "/api/fault/inspect",
  "status": "ok"
}
```
*Note: `x-fault-delay-ms` is completely absent from headers received by the backend.*
