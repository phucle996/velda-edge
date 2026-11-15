# ACME Certificate Rotator (`acme_cert_rotator`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers managing automated TLS certificate issuance, renewal, and incident response in Velda Edge.

---

## 1. Configuration Parameter Reference

All fields must be declared under `config` when configuring `acme_cert_rotator` in `plugins.json`:

```json
{
  "id": "acme_cert_rotator",
  "type": "acme_cert_rotator",
  "enabled": true,
  "config": {
    "directory_url": "https://acme-v02.api.letsencrypt.org/directory",
    "contact_email": "admin@example.com",
    "domains": ["api.example.com", "edge.example.com"],
    "renew_before_days": 30,
    "schedule": "14 3 * * *",
    "storage_dir": "/var/lib/velda/certs"
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Allowed Values / Constraints | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`directory_url`** | `String` | **Yes** | Absolute HTTPS URL (or `http://127.0.0.1` / `http://localhost` for local testing) | The ACME directory endpoint of the Certificate Authority (CA). Defaults to Let's Encrypt Production. Use Let's Encrypt Staging during testing to avoid production rate limits. |
| **`contact_email`** | `String` | **Yes** | Valid email address (`user@domain.tld`) | Account administrator email used to register the ECDSA P-256 account key with the CA. Used by the CA to send expiration or security revocation notices. |
| **`domains`** | `Array<String>` | **Yes** | Non-empty list of valid FQDNs (no duplicates) | The domain names included in the issued X.509 certificate. The first domain becomes the primary Subject CN; subsequent entries become Subject Alternative Names (SANs). **Wildcard domains (`*.example.com`) are explicitly rejected** because RFC 8555 HTTP-01 challenges cannot validate wildcards. |
| **`renew_before_days`** | `u32` | **Yes** | `1 ..= 89` days (Recommended: `30`) | Threshold in days to trigger certificate renewal before expiration. Setting this to `30` on standard 90-day certificates gives a 30-day window of daily renewal attempts, easily absorbing transient network outages or CA downtime. |
| **`schedule`** | `String` | **Yes** | Standard 5-field cron expression in UTC (`min hour dom month dow`) | Evaluated strictly in UTC time. Determines when the rotator wakes up to inspect certificate expiry and initiate ACME orders. Choosing an off-peak, non-zero minute (e.g. `14 3 * * *` instead of `0 0 * * *`) prevents CA API thundering herds. |
| **`storage_dir`** | `String` | **Yes** | Non-empty directory path (e.g. `/var/lib/velda/certs`) | Local filesystem directory storing persistent account keys (`account.key`) and per-domain certificate chains (`fullchain.pem`, `privkey.pem`). Private keys are written with strict `0600` permissions. |

---

## 2. Scenario-Based Configuration Recipes

Use these tuned configuration presets based on your operational environment:

### Scenario A: Production Public Edge Gateway (Let's Encrypt Production)
*Goal: Automated 90-day certificates, high reliability, jittered schedule to avoid CA traffic spikes.*

```json
{
  "id": "acme_cert_rotator_prod",
  "type": "acme_cert_rotator",
  "enabled": true,
  "config": {
    "directory_url": "https://acme-v02.api.letsencrypt.org/directory",
    "contact_email": "sre-alerts@company.com",
    "domains": ["api.company.com", "edge.company.com"],
    "renew_before_days": 30,
    "schedule": "14 3 * * *",
    "storage_dir": "/var/lib/velda/certs"
  }
}
```
* **Why this setup**: Wakes up daily at **03:14 UTC**. If the current certificate has $> 30$ days of validity remaining, it goes back to sleep with 0 network calls. At day 60 (30 days before expiration), it automatically orders, validates, and hot-swaps a fresh certificate.

---

### Scenario B: Staging / Pre-Production Environment (Rate-Limit Free Testing)
*Goal: Validate automated renewal workflows without consuming Let's Encrypt production quotas.*

```json
{
  "id": "acme_cert_rotator_staging",
  "type": "acme_cert_rotator",
  "enabled": true,
  "config": {
    "directory_url": "https://acme-staging-v02.api.letsencrypt.org/directory",
    "contact_email": "devops-testing@company.com",
    "domains": ["staging.company.com"],
    "renew_before_days": 60,
    "schedule": "0 */4 * * *",
    "storage_dir": "/var/lib/velda/certs-staging"
  }
}
```
* **Why this setup**: Points to the Let's Encrypt Staging environment, which has generous rate limits. Runs every 4 hours (`0 */4 * * *`) with an aggressive 60-day renewal threshold to frequently exercise renewal code paths.

---

### Scenario C: Local Development & CI/CD with Pebble or HashiCorp Vault ACME
*Goal: Fast end-to-end integration testing in isolated or air-gapped networks.*

```json
{
  "id": "acme_cert_rotator_local",
  "type": "acme_cert_rotator",
  "enabled": true,
  "config": {
    "directory_url": "http://127.0.0.1:14000/dir",
    "contact_email": "local-test@example.com",
    "domains": ["local.example.com"],
    "renew_before_days": 10,
    "schedule": "*/10 * * * *",
    "storage_dir": "/tmp/velda-local-certs"
  }
}
```
* **Why this setup**: Connects over plain HTTP to a local Pebble ACME test server (`http://127.0.0.1:14000/dir`). Evaluates renewal every 10 minutes to verify ephemeral route injection and certificate hot-swapping in integration test pipelines.

---

## 3. Emergency Incident Triage Matrix

| Severity | Symptoms & Alerts | Root Cause Analysis | Rapid Mitigation Action | SLA |
| :---: | :--- | :--- | :--- | :---: |
| **P0** | Global HTTPS failure: `NET::ERR_CERT_DATE_INVALID` in user browsers | Certificate expired: ACME renewal failed continuously across the 30-day window | Run manual renewal CLI; verify Port 80 ingress and DNS resolution; hot-swap temporary wildcard cert | **< 10 min** |
| **P1** | Log error: `ACME order failed: 429 Too Many Requests` (Rate Limited by CA) | Exceeded Let's Encrypt limits (e.g. 5 failed validations per hostname per hour) | Switch `directory_url` to alternative ACME CA (e.g. ZeroSSL or Google Trust Services ACME); or wait out rate limit | **< 30 min** |
| **P2** | Log warning: `HTTP-01 challenge verification timed out / unauthorized` | External CA cannot reach Port 80: firewall block, Security Group, or split-horizon DNS | Verify public Port 80 accessibility with external curl; ensure DNS resolves to gateway public IP | **< 1 hour** |
| **P3** | Plugin fails startup: `storage_dir permission denied` | Process user lacks write permissions to `/var/lib/velda/certs` or disk is 100% full | Execute `chmod 700 /var/lib/velda/certs` and `chown -R velda:velda`; verify disk space with `df -h` | **< 2 hours** |

---

## 4. Diagnostic CLI Toolkit

When an ACME alert triggers, SSH into the affected edge gateway and run these commands:

### 4.1 Inspect Certificate Expiry on Local Disk
```bash
# 1. Check expiration date of active certificate chain
openssl x509 -enddate -noout -in /var/lib/velda/certs/api.company.com/fullchain.pem

# 2. Check full certificate details (issuer, SANs, validity)
openssl x509 -text -noout -in /var/lib/velda/certs/api.company.com/fullchain.pem | grep -E "Issuer:|Not After|DNS:"
```

### 4.2 Verify Public Port 80 Reachability for HTTP-01
The ACME CA must be able to reach your gateway on port 80:
```bash
# Test from an external machine outside your network
curl -iv -A "Let's Encrypt ACME Bot" http://api.company.com/.well-known/acme-challenge/health-check

# Verify port 80 is listening on the gateway host
ss -tulpn | grep :80
```
*Note: A 404 response on arbitrary challenge tokens is normal when no active order is pending; connection timeouts or connection refused indicate firewall or routing failures.*

### 4.3 Inspect Rotator Operational Logs
```bash
# Filter journalctl for ACME rotation events
journalctl -u velda-edge -n 100 --no-pager | grep -i "acme_cert_rotator"

# Monitor real-time logs during renewal attempt
journalctl -u velda-edge -f -o cat | grep -E "ACME|renewal|challenge|hot-swapped"
```

---

## 5. Step-by-Step Incident Resolution Playbooks

### Playbook 1: Emergency Manual Renewal when Certificate Expired
**Context**: A certificate has expired or is expiring in $< 24$ hours due to previous renewal failures.
1. **Force Immediate Bootstrap Execution**:
   - The rotator evaluates certificates at bootstrap. Restarting the service triggers an immediate cold-start renewal if the certificate is within `renew_before_days`:
     ```bash
     systemctl restart velda-edge
     ```
2. **Monitor Bootstrap Renewal**:
   ```bash
   journalctl -u velda-edge -f -o cat | grep -E "needs_renewal|Successfully hot-swapped"
   ```
3. **If Renewal Fails (Port 80 Blocked / Cloudflare Proxy)**:
   - If Port 80 cannot be opened immediately, obtain a temporary manual certificate using Certbot or Cloudflare Origin CA, place it in `/var/lib/velda/certs/api.company.com/`, and reload:
     ```bash
     velda-cli reload
     ```

---

### Playbook 2: Recovering from Let's Encrypt 429 Rate Limits
**Context**: Log indicates `urn:ietf:params:acme:error:rateLimited` (5 failed attempts per hour).
1. **Identify the exact constraint**:
   - If failed validations limit (5/hour): Wait exactly 60 minutes from the first failure.
   - If weekly certificates limit (50/week for domain): Add a dummy subdomain to the request (e.g. `api.company.com` + `alt.company.com`), which counts as a distinct SAN order.
2. **Switch to Alternative ACME Provider (e.g. ZeroSSL)**:
   - Update `plugins.json`:
     ```json
     {
       "directory_url": "https://acme.zerossl.com/v2/DV90"
     }
     ```
   - Trigger configuration reload:
     ```bash
     velda-cli reload
     ```

---

### Playbook 3: Debugging HTTP-01 Challenge Validation Failures
**Context**: CA returns `Invalid response from http://api.company.com/.well-known/acme-challenge/...`.
1. **Check DNS Propagation**:
   - Ensure external DNS resolves to the gateway's public IP:
     ```bash
     dig +short api.company.com @8.8.8.8
     ```
2. **Check Port 80 Redirects**:
   - Some cloud load balancers unconditionally redirect HTTP to HTTPS. RFC 8555 allows redirects, but the target HTTPS listener must have a valid or self-signed cert.
   - Ensure port 80 traffic routes directly to `velda-edge`.
3. **Inspect Ephemeral Route Injection**:
   - Check gateway logs for `Ephemeral ACME HTTP-01 challenge route injected into router table`. If absent, verify that the ingress listener ID matches the configured port 80 listener.
