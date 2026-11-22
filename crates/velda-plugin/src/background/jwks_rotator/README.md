# JWKS Key Rotator Plugin (`jwks_rotator`)

```text
+-----------------------------------------------------------------------------------+
|                        JWKS Key Rotator — High-Level Workflow                     |
|                                                                                   |
|  [External Identity Provider] (Auth0, Okta, Keycloak, Entra ID)                   |
|         │                                                                         |
|         │ Periodic GET /.well-known/jwks.json (HTTPS)                             |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ jwks_rotator (BackgroundPlugin, SCHED_IDLE)                                 │  |
|  │  - Runs on background logistics worker (never blocks request traffic)       │  |
|  │  - Fetches and parses RFC 7517 keys into RAM (up to 8 keys)                 │  |
|  │  - Preserves emergency static key at Slot 0 (optional)                      │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Atomic Pointer Swap (Zero-Lock)         |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ jwt_auth (PreUpstreamPlugin, Frontline Workers)                             │  |
|  │  - Validates inbound JWT tokens against current in-memory key table         │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------------------------------------------------+
```

The **`jwks_rotator`** plugin is a background service in Velda Edge. It automatically polls external Identity Providers (IdPs) like Auth0, Keycloak, or Okta, parses their JSON Web Key Sets (RFC 7517), and supplies updated cryptographic public keys to companion `jwt_auth` plugins in real time without restarting the gateway or dropping active requests.

---

## 1. What This Plugin Does

1. **Automated Key Rotation**:
   - Polls your IdP's `/.well-known/jwks.json` endpoint at a configurable interval (e.g., every 5 minutes).
   - Ingests up to 8 active and transitional signing keys simultaneously, allowing old and new keys to coexist seamlessly during planned key rotations.
2. **Background Logistics Isolation**:
   - All network calls, DNS resolutions, and JSON parsing occur entirely on a dedicated background worker (`velda-shadow-{i}`) running under `SCHED_IDLE`.
   - Request-serving frontline workers (`velda-worker-{id}`) are never blocked and never execute background network I/O.
3. **Outage Resilience (Last Known Good - LKG)**:
   - If your IdP goes offline, experiences network timeouts, or serves malformed JSON, the plugin retains the current valid key table in memory. Your gateway continues serving authenticated traffic without interruption.
4. **Emergency Static Fallback (Slot 0)**:
   - Can permanently preserve a static fallback key in Slot 0 while dynamically updating slots 1..7 from the external IdP.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/background/jwks_rotator/`
- **Execution Stage**: `BackgroundPlugin` (compile-time fixed)
- **Runtime Placement**: Spawns as a cooperative async background daemon when Velda Edge boots, running independently of the L7 request pipeline.
- **Companion Relationship**: Directly pairs with an instance of `jwt_auth`. The rotator updates the key table that `jwt_auth` reads during request validation.

---

## 3. Requirements & Prerequisites

1. **Network Connectivity**: Outbound HTTP or HTTPS connectivity from the gateway node to the configured `jwks_uri`.
2. **Companion Plugin Wiring**: A corresponding `jwt_auth` plugin must be configured in `plugins.json` with an `id` matching `target_jwt_auth_id`.
3. **Supported Key Types**:
   - **RSA**: `RS256`, `RS384`, `RS512` ($\ge 2048$-bit modulus).
   - **ECDSA**: `ES256` (NIST P-256), `ES384` (NIST P-384).
   - **HMAC / Octet**: `HS256`, `HS384`, `HS512`.

---

## 4. Quick Start Configuration

Add the plugin to your `plugins.json`:

```json
{
  "id": "auth0_rotator",
  "config": {
    "target_jwt_auth_id": "api_jwt_auth",
    "jwks_uri": "https://company.us.auth0.com/.well-known/jwks.json",
    "refresh_interval_secs": 300,
    "request_timeout_secs": 10,
    "preserve_static_key": true
  }
}
```

---

## 5. Documentation Directory

For deeper technical and operational information, refer to the accompanying specifications:

- **[RUNBOOK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/jwks_rotator/RUNBOOK.md)**: Field-by-field parameter explanations, tuned setup recipes for production scenarios, incident triage playbooks, and diagnostic CLI tools.
- **[ARCHITECTURE.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/jwks_rotator/ARCHITECTURE.md)**: Deep micro-architectural specification (byte-level hex memory map, cacheline boundaries, MESI cache coherence protocol, and RCU debt tracking).
- **[BENCHMARK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/jwks_rotator/BENCHMARK.md)**: Zero-bloat benchmark specifications comparing key traversal, swap spike latency, and memory retention against Kong and APISIX.
