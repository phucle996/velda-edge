# ACME Automated Certificate Rotator Plugin (`acme_cert_rotator`)

```text
+-----------------------------------------------------------------------------------+
|                        ACME Certificate Rotator Workflow                          |
|                                                                                   |
|  [External ACME CA] (Let's Encrypt / ZeroSSL / Boulder / Pebble)                  |
|         │                                                                         |
|         │ HTTPS JWS (Account Registration & Order Creation)                       |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ acme_cert_rotator (BackgroundPlugin, SCHED_IDLE)                             │  |
|  │  - Runs on background logistics worker (never blocks request traffic)       │  |
|  │  - Dual-Trigger: Cold-start bootstrap expiry check + UTC Cron schedule      │  |
|  │  - Pre-bakes HTTP-01 wire response & injects ephemeral route into Router    │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Ephemeral Exact Route Injection         |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ velda-router::Router (Port 80 Ingress)                                      │  |
|  │  - Serves GET /.well-known/acme-challenge/<token> directly from RAM          │  |
|  │  - RAII Scope Guard (EphemeralRouteGuard) ensures deterministic route cleanup│  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Challenge Validated & Cert Downloaded   |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ velda-tls::SniResolver & Local Disk Storage                                 │  |
|  │  - Zero-downtime atomic hot-swap of CertificateChain into SniResolver       │  |
|  │  - Atomic disk persistence (tmp + sync + rename) to storage_dir             │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------------------------------------------------+
```

The **`acme_cert_rotator`** plugin is Velda Edge's automated certificate lifecycle engine. It autonomously manages X.509 TLS certificate issuance, validation, and renewal via the ACME protocol (RFC 8555) using Let's Encrypt, ZeroSSL, or private ACME CAs, hot-swapping renewed certificates into the data plane with zero downtime and zero dropped connections.

---

## 1. What This Plugin Does

1. **Automated Zero-Downtime TLS Renewal**:
   - Manages the full RFC 8555 lifecycle: account key registration, certificate ordering, HTTP-01 challenge completion, CSR generation, certificate download, and dynamic hot-swap into downstream TLS handshakes.
2. **Dual-Trigger Execution Model**:
   - **Cold-Start Bootstrap**: Inspects certificate validity on local disk at gateway startup. If missing or expiring within `renew_before_days` (default: 30 days), initiates immediate renewal before customer traffic is impacted.
   - **UTC Cron Maintenance**: Evaluates renewal checks strictly on a user-defined 5-field UTC cron schedule (e.g. `0 2 * * *` at 02:00 UTC). Never uses noisy, wasteful polling loops.
3. **Ephemeral Route Injection with RAII Teardown**:
   - Dynamically registers the temporary challenge path (`/.well-known/acme-challenge/<token>`) directly into the in-memory `velda_router::Router`.
   - Challenge responses serve directly from memory without touching upstream backends.
   - Guarded by RAII (`EphemeralRouteGuard`): the challenge route is unconditionally purged from the router table upon success, error, timeout, or task cancellation.
4. **Minimal Transient Footprint**:
   - Challenge responses are compact (~180 bytes) and transient, served directly from memory and naturally evicted by hardware LRU without polluting customer traffic state.
5. **Atomic Disk Persistence (Crash-Safe LKG)**:
   - Persists issued certificates and private keys using atomic file replacement (`.tmp` file + `fsync` + atomic `rename`). Gateway crashes or power losses never result in corrupted PEM files.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/background/acme_cert_rotator/`
- **Execution Stage**: `BackgroundPlugin` (compile-time fixed)
- **Runtime Placement**: Operates entirely on the Shadow Logistics Worker (`velda-shadow-{i}`) running under `SCHED_IDLE`, decoupled from frontline traffic workers.
- **Data Plane Touchpoints**:
  - Injects transient routes into `velda_router::Router`.
  - Hot-swaps certified keys into `velda-tls::SniResolver`.
  - Persists accounts and certificates to local disk storage (`storage_dir`).

---

## 3. Requirements & Prerequisites

1. **Public Port 80 Ingress**: The edge gateway's HTTP ingress port (typically port 80) must be publicly reachable from the Internet so external ACME CAs can validate the HTTP-01 challenge.
2. **Explicit FQDN Domains**: Only fully qualified domain names (e.g. `api.example.com`, `edge.example.com`) are supported.
   - *Limitation*: Per RFC 8555 §8.3, **wildcard domains (`*.example.com`) cannot be validated using HTTP-01 challenges** (DNS-01 is required). Wildcards are rejected at configuration time.
3. **Storage Permissions**: The directory configured in `storage_dir` must be writable by the user running the `velda-edge` process (`chmod 700 /var/lib/velda/certs`).

---

## 4. Quick Start Configuration

Declare the plugin in your `plugins.json`:

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

---

## 5. Documentation Directory

For deeper technical and operational information, refer to the accompanying specifications:

- **[RUNBOOK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/acme_cert_rotator/RUNBOOK.md)**: Field-by-field parameter explanations, tuned setup presets (Production Let's Encrypt vs Staging vs Local Pebble), emergency incident triage matrix, CLI diagnostic toolkit, and troubleshooting playbooks.
- **[ARCHITECTURE.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/acme_cert_rotator/ARCHITECTURE.md)**: Deep micro-architectural specification (SIMD 128-bit vector matcher, RAII route injection, `_mm_clflush` cacheline preservation, atomic disk transactions, and physical hardware boundaries without dogmatic commitments).
- **[BENCHMARK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/acme_cert_rotator/BENCHMARK.md)**: Zero-bloat 5-tier benchmark specifications comparing challenge routing overhead, TLS hot-swap latency delta, and chaos CA blackhole resilience against Caddy, Traefik, and APISIX.
