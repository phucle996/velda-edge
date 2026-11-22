# HTTPS Redirect (`https_redirect`)

```text
+-----------------------------------------------------------------------------------+
|                        HTTPS REDIRECT IN PRE-ROUTE PHASE                          |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  Inbound L7Request ──► [https_redirect] (pre_route, before Router)                |
|                             │                                                     |
|         ┌───────────────────┴───────────────────┐                                 |
|         ▼                                       ▼                                 |
|  x-forwarded-proto == "https"            Plaintext HTTP                           |
|  OR uri.scheme == "https"                  ├── Missing/Invalid Host -> 400        |
|         │                                  └── Valid Host -> 301 / 308            |
|         ▼                                      Location: https://<host>[:port]... |
|   Action::Continue                                                                |
+-----------------------------------------------------------------------------------+
```

## 1. What This Plugin Does

`https_redirect` enforces HTTPS upgrades at the edge boundary before route matching or upstream forwarding. Plaintext HTTP requests receive an immediate `301 Moved Permanently` or `308 Permanent Redirect` pointing to `https://<host>[:port]<path_and_query>`.

## 2. Where It Sits in Velda Edge

- **Execution Phase**: `pre_route` (runs **before** route matching on HTTP listeners).
- **Thread Placement**: Inline on Frontline Traffic Workers (`velda-worker-{i}`).
- **Supported Protocols**: HTTP/1.1, HTTP/2, HTTP/3 (`SupportedProtocols::ALL_HTTP`).
- **Capability**: `PluginCapability::header_only(AccessLevel::Read)` (zero payload buffering; HTTPS requests stream zero-copy).

## 3. Requirements & Prerequisites

- `velda-edge` authoritatively populates `x-forwarded-proto` from the socket's actual TLS state (`meta.is_tls`) **before** `pre_route` plugins execute, preventing client header spoofing on plaintext listeners.

## 4. Quick Start Configuration

```json
{
  "id": "https_redirect",
  "config": {
    "status_code": 308,
    "https_port": 443
  }
}
```

## 5. Documentation Directory

- [**RUNBOOK.md**](RUNBOOK.md): Configuration parameters, `301` vs `308` guidance, and incident triage.
- [**ARCHITECTURE.md**](ARCHITECTURE.md): Cold-path split, `2048B` stack `Location` formatter, CRLF/IPv6 sanitization, and ASM audit.
- [**BENCHMARK.md**](BENCHMARK.md): Verified release assembly profile and KVM `119K req/s` benchmark results.
- [**TEST.md**](TEST.md): Verified unit tests and live KVM redirect checks.
