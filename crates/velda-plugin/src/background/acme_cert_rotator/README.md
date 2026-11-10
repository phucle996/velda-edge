# ACME Automated Certificate Rotator Plugin

`acme_cert_rotator` is an autonomous background logistics plugin (`phase: "background"`) running on dedicated Shadow Logistics workers (`SCHED_IDLE`) to provide zero-downtime, fully automated X.509 certificate issuance and renewal via the ACME protocol (RFC 8555, Let's Encrypt, ZeroSSL, Boulder, Pebble).

---

## Key Highlights

- **Dual-Trigger Execution Model**:
  - **Bootstrap (Cold Start)**: Inspects local LKG certificate validity immediately at gateway startup. If missing or expiring within `renew_before_days` (default: 30 days), initiates immediate renewal.
  - **Cron-Driven Schedule (Warm Routine)**: Evaluates maintenance checks strictly on user-configured 5-field UTC cron expressions (`schedule: "0 2 * * *"`). Zero polling loops.
- **Fail-Safe Exponential Backoff**:
  - Retries up to 3 times with exponential backoff and jitter upon transient network or ACME CA errors (e.g. 503, DNS propagation delay), preventing CA rate-limit exhaustion.
- **Ephemeral Exact Route Injection**:
  - Dynamically registers an ephemeral exact route `/.well-known/acme-challenge/<token>` into `velda_router::Router`.
  - Serves pre-baked wire responses directly from memory without touching upstream pipelines.
  - Enforces unconditional cleanup via RAII `EphemeralRouteGuard`, guaranteeing zero memory or route table leakage on success, error, or timeout.
- **Cache-Pollution Free**:
  - Challenge responses reside safely in L3/RAM during the 2–5s CA verification window, keeping L1D/L2 caches 100% clean for active customer traffic.
- **Zero-Downtime Hot-Swap**:
  - Newly issued certificate chains and ECDSA private keys are atomically swapped into `velda-tls::SniResolver` without dropping existing TLS sessions.

---

## Declarative Configuration

Declared in configuration files without specifying `"phase"` (the phase is automatically inferred as `"background"` by `velda-sync`):

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
    "schedule": "0 2 * * *",
    "storage_dir": "/var/lib/velda/certs"
  }
}
```

### Configuration Parameters

| Field | Type | Description |
| :--- | :--- | :--- |
| `directory_url` | String | ACME directory endpoint (e.g. Let's Encrypt prod or staging). Must start with `https://` (or `http://` for local test CAs). |
| `contact_email` | String | Account administrator email address used for ACME account registration. |
| `domains` | Array<String> | Domain names to include in the SAN certificate. The first domain becomes the primary subject. |
| `renew_before_days` | u32 | Threshold in days (1 to 89) to trigger renewal prior to certificate expiration. |
| `schedule` | String | Standard 5-field cron expression evaluated in UTC (`min hour dom month dow`). |
| `storage_dir` | String | Local directory path to persist account keys and LKG certificates. |
