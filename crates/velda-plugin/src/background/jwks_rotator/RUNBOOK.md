# JWKS Key Rotator (`jwks_rotator`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers managing dynamic cryptographic key rotation in Velda Edge.

---

## 1. Configuration Parameter Reference

All fields must be declared under `config` when configuring `jwks_rotator` in `plugins.json`:

```json
{
  "id": "auth0_rotator",
  "config": {
    "target_jwt_auth_id": "api_jwt_auth",
    "jwks_uri": "https://auth.example.com/.well-known/jwks.json",
    "refresh_interval_secs": 300,
    "request_timeout_secs": 10,
    "preserve_static_key": true
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Default | Allowed Values / Constraints | Operational Meaning & Impact |
| :--- | :---: | :---: | :---: | :--- | :--- |
| **`target_jwt_auth_id`** | `String` | **Yes** | — | Non-empty string matching an existing `jwt_auth` plugin instance `id` | Identifies which `jwt_auth` instance receives updated keys. During gateway bootstrap, Velda Edge verifies this link. If the target plugin is missing, initialization halts safely to prevent unauthenticated traffic flow. |
| **`jwks_uri`** | `String` | **Yes** | — | Valid URL starting with `https://` or `http://` (HTTPS strongly required in production) | The public endpoint of your Identity Provider (IdP) publishing RFC 7517 keys. Supports custom ports (`https://auth:8443/...`). SNI and TLS 1.3 are negotiated automatically. |
| **`refresh_interval_secs`** | `u64` | No | `300` (5m) | Minimum: `10` seconds. Recommended: `60` – `3600` seconds | Frequency of polling the IdP for key changes. Clamped to $\ge 10$ seconds to prevent accidental denial-of-service or rate-limiting against external IdP APIs. Evaluated using integer coarse time (`coarse_now_sec`). |
| **`request_timeout_secs`** | `u64` | No | `10` | Minimum: `1` second. Recommended: `5` – `15` seconds | Total non-blocking timeout for the TCP connection, TLS handshake, and body read. Connect timeout is automatically bounded to $\min(\text{timeout}, 5\text{s})$. Prevents stalled network requests from hanging background workers. |
| **`preserve_static_key`** | `bool` | No | `true` | `true` or `false` | When `true`, preserves the static fallback key defined in `jwt_auth` at **Slot 0** across all rotation cycles. The external IdP keys populate slots 1..7. When `false`, IdP keys overwrite all slots including Slot 0. |

---

## 2. Scenario-Based Configuration Recipes

Use these tuned configurations based on your operational environment:

### Scenario A: Production Public Edge Gateway (Auth0 / Okta / Entra ID)
*Goal: Balanced security, robust fallback, zero risk of IdP rate limiting.*

```json
{
  "id": "public_edge_jwks_rotator",
  "config": {
    "target_jwt_auth_id": "edge_jwt_auth",
    "jwks_uri": "https://company.us.auth0.com/.well-known/jwks.json",
    "refresh_interval_secs": 300,
    "request_timeout_secs": 10,
    "preserve_static_key": true
  }
}
```
* **Why this setup**: A 5-minute interval (`300s`) ensures that planned key rotations propagate to your edge within 5 minutes while generating only 12 requests per hour to your IdP. Preserving Slot 0 ensures an emergency static key remains active in RAM if Auth0 experiences a multi-hour global outage.

---

### Scenario B: High-Churn Fast Rotation / Canary Environments
*Goal: Fast propagation for testing key changes and automated CI/CD rotation.*

```json
{
  "id": "canary_jwks_rotator",
  "config": {
    "target_jwt_auth_id": "canary_jwt_auth",
    "jwks_uri": "https://idp-staging.internal.net/.well-known/jwks.json",
    "refresh_interval_secs": 30,
    "request_timeout_secs": 5,
    "preserve_static_key": false
  }
}
```
* **Why this setup**: A 30-second interval propagates new keys almost immediately. A tight 5-second timeout detects network hiccups quickly. `preserve_static_key: false` gives the staging IdP full authority over all 8 table slots.

---

### Scenario C: High-Latency / Remote / Air-Gapped IdP
*Goal: Tolerance for unstable WAN links and slow DNS resolution.*

```json
{
  "id": "wan_jwks_rotator",
  "config": {
    "target_jwt_auth_id": "wan_jwt_auth",
    "jwks_uri": "https://auth.remote-region.corp/.well-known/jwks.json",
    "refresh_interval_secs": 1800,
    "request_timeout_secs": 20,
    "preserve_static_key": true
  }
}
```
* **Why this setup**: Polling every 30 minutes (`1800s`) reduces cross-region WAN bandwidth and latency variance. An extended 20-second timeout prevents premature timeouts over congested links. The static fallback key in Slot 0 provides critical resilience.

---

## 3. Emergency Incident Triage Matrix

| Severity | Symptoms & Alerts | Root Cause Analysis | Rapid Mitigation Action | SLA |
| :---: | :--- | :--- | :--- | :---: |
| **P0** | Gateway fails to start (`Bootstrap Panic: Dependency wiring failed`) | `target_jwt_auth_id` references a non-existent plugin ID or `jwt_auth` failed to compile | Verify `id` match in `plugins.json`; ensure companion `jwt_auth` is enabled | **< 5 min** |
| **P1** | Frontline returns `401 Unauthorized` with `Unknown key ID (kid)` after IdP rotation | Rotator failed to poll new keys (network partition, DNS failure, or IdP 5xx error) | Execute manual curl test to IdP; check rotator logs; verify LKG retention status | **< 15 min** |
| **P2** | Log alert: `JWKS response exceeded maximum allowable size (65536 bytes)` | IdP published excessive keys or metadata exceeding the 64 KB safety buffer ceiling | Contact Identity team to clean up stale retired keys from IdP JWKS payload | **< 1 hour** |
| **P3** | Log warning: `Key in slot X ignored: RSA modulus < 2048 bits` | IdP published a weak RSA key (e.g., 1024-bit) that violates security policies | Update IdP key policy to generate standard RSA 2048/4096-bit keys | **< 4 hours** |

---

## 4. Diagnostic CLI Toolkit

When an alert fires, SSH into the node and run the following checks:

### 4.1 Test External IdP Connectivity & Payload
```bash
# 1. Test direct HTTPS fetch with timing metrics
curl -ivs -m 5 https://auth.example.com/.well-known/jwks.json | jq .

# 2. Check HTTP status code only
curl -s -o /dev/null -w "%{http_code}\n" -m 5 https://auth.example.com/.well-known/jwks.json
```

### 4.2 Inspect Rotator Operational Logs
```bash
# Filter rotator logs for active rotation events
journalctl -u velda-edge -n 100 --no-pager | grep -i "jwks_rotator"

# Check for warnings or failed fetch cycles
journalctl -u velda-edge -f -o cat | grep -E "Successfully rotated|Initial JWKS fetch failed|exceeded maximum allowable"
```

### 4.3 Validate Key Contents in IdP Output
```bash
# Verify active key IDs (kids) and algorithms
curl -s https://auth.example.com/.well-known/jwks.json | jq '.keys[] | {kid: .kid, kty: .kty, alg: .alg}'
```

---

## 5. Step-by-Step Incident Resolution Playbooks

### Playbook 1: External IdP Outage / Network Partition
**Context**: The IdP is down (HTTP 500/503), returning timeouts, or DNS resolution is failing.
1. **Verify Gateway Serving State**:
   - Check if traffic workers are still serving requests:
     ```bash
     curl -i -H "Authorization: Bearer <existing_valid_token>" http://127.0.0.1:8080/api/v1/resource
     ```
   - *Expected Result*: Existing tokens should continue to return `200 OK` due to **In-Memory LKG Preservation**.
2. **If Tokens Signed with New Keys are Failing**:
   - If the IdP rotated keys right before going down, and the gateway did not catch the update:
     - Temporarily inject the new public key as a static key in `jwt_auth` (`public_key_pem`).
     - Trigger configuration reload:
       ```bash
       velda-cli reload
       ```

---

### Playbook 2: IdP Publishes Corrupted or Oversized Keys
**Context**: Log indicates `Failed to parse JWK key` or `JWKS response exceeded maximum allowable size`.
1. **Inspect Raw Payload**:
   ```bash
   curl -s https://auth.example.com/.well-known/jwks.json > /tmp/jwks_raw.json
   ls -lh /tmp/jwks_raw.json
   ```
2. **Remediation**:
   - If size $> 64$ KB: The IdP contains too many obsolete keys. Request the identity team to prune expired keys.
   - If an RSA modulus is under 2048 bits: The rotator intentionally ignores the weak key to protect the gateway. Issue an emergency key generation on the IdP with RSA 2048 or ECDSA P-256.

---

### Playbook 3: Startup Dependency Wiring Failure
**Context**: Gateway fails to boot with `shared key store has not been wired for target 'xyz'`.
1. **Check `plugins.json` Configuration**:
   - Verify that the `target_jwt_auth_id` in `jwks_rotator` exactly matches the `id` of your `jwt_auth` plugin.
   ```json
   {
     "id": "jwt_auth_ingress",      <--- Must match exactly
     ...
   },
   {
     "id": "jwks_rotator_bg",
     "config": {
       "target_jwt_auth_id": "jwt_auth_ingress"  <--- Must match exactly
     }
   }
   ```
2. Validate syntax and restart:
   ```bash
   velda-cli check-config
   systemctl restart velda-edge
   ```
