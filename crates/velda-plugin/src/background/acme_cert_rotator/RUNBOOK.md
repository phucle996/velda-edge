# ACME Certificate Rotator — Operations & Troubleshooting Runbook

This runbook guides operators on deploying, configuring, monitoring, and troubleshooting the `acme_cert_rotator` background service.

---

## 1. Quick Start Configuration

Add the plugin to your gateway's configuration file (`plugins.json`):

```json
{
  "id": "acme_cert_rotator",
  "type": "acme_cert_rotator",
  "enabled": true,
  "config": {
    "directory_url": "https://acme-staging-v02.api.letsencrypt.org/directory",
    "contact_email": "ops@example.com",
    "domains": ["api.example.com"],
    "renew_before_days": 30,
    "schedule": "0 2 * * *",
    "storage_dir": "/var/lib/velda/certs"
  }
}
```

> [!TIP]
> Always verify with the **Let's Encrypt Staging Directory** (`https://acme-staging-v02.api.letsencrypt.org/directory`) during testing before switching to production (`https://acme-v02.api.letsencrypt.org/directory`) to avoid strict production rate limits.

---

## 2. Directory Structure & LKG Persistence

Certificates and cryptographic keys are organized in the declared `storage_dir`:

```text
/var/lib/velda/certs/
├── account.key                 # ECDSA P-256 account private key (PKCS#8 PEM)
└── api.example.com/
    ├── fullchain.pem           # Issued X.509 certificate chain (PEM)
    └── privkey.pem             # Domain private key (PEM)
```

---

## 3. Operational Invariants

1. **Firewall & Ingress Routing**:
   The HTTP ingress listener (port 80) must be publicly accessible from the Internet for Let's Encrypt to validate the HTTP-01 challenge (`/.well-known/acme-challenge/*`).
2. **Wildcard Domains**:
   Wildcard certificates (`*.example.com`) are **not** supported via HTTP-01 challenges per RFC 8555 §8.3 (DNS-01 is required for wildcards). Wildcard domain declarations are rejected at configuration validation time.
3. **Permissions**:
   The `storage_dir` must be writable by the user running the `velda-edge` process (`chmod 700 /var/lib/velda/certs`).

---

## 4. Troubleshooting & Common Failures

### 4.1 ACME CA Network Choke / Timeout
- **Symptom**: `ACME renewal cycle failed: Connection timeout to ...`
- **Mitigation**: The service automatically attempts 3 retries with exponential backoff (`5s -> 10s -> 20s`). Check egress firewall rules allowing outbound HTTPS traffic on port 443.

### 4.2 Rate Limiting (`tooManyFailedAuthorizations`)
- **Symptom**: `429 Too Many Requests: error code 'tooManyFailedAuthorizations'`
- **Mitigation**: Switch to the staging environment while debugging. Verify domain DNS records point to the edge gateway's public IP.

### 4.3 Expired Certificate at Startup
- **Symptom**: Warning logged during cold start.
- **Action**: The rotator detects the expired certificate during bootstrap and immediately dispatches an urgent renewal order.
