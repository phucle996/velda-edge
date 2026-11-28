# Unified Fault Injection Plugin (`fault_injection`)

| Metadata | Value |
| :--- | :--- |
| **Plugin ID** | `fault_injection` |
| **Stage** | `pre_upstream` |
| **Protocol Affinity** | All L7 (`HTTP/1.1`, `HTTP/2`, `HTTP/3`, `gRPC` — rejected on `raw` L4 listeners) |
| **PBWW (Streaming)** | `0` (header-only inspection; zero payload buffering) |

```text
  L7 Request ──► Strip `x-fault-delay-ms` & `x-fault-abort-status` (always)
                    │
                    ├─► active_mask == 0 or scope miss ──► Action::Continue
                    │
                    └─► Scope match + single XorShift64* roll
                           ├─► Delay only  ──► Action::Delay(dur, Continue)
                           ├─► Abort only  ──► Stamp bypass `x-velda-fault: abort` + Action::Status(InternalStatus)
                           └─► Delay+Abort ──► Stamp bypass `x-velda-fault: abort` + Action::Delay(dur, Status(..))
                                                     │
                                                     ▼
                                     Protocol Crate Renders Native Wire Response
                                     ├─ velda-http1/2/3 -> HTTP status (e.g. 503, 504, 429)
                                     └─ velda-grpc      -> gRPC Trailers-Only (e.g. UNAVAILABLE)
```

---

## 1. What This Plugin Does

`fault_injection` provides scoped chaos engineering and resilience testing (synthetic non-blocking latency and deterministic request abort) in a single protocol-agnostic plugin. Rather than synthesizing HTTP or gRPC wire frames inside `velda-plugin`, the plugin stamps the MSB bypass header `x-velda-fault: abort` onto `HeaderWorkspace` and returns `Action::Status(InternalStatus)`. Each downstream protocol crate (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`) translates that `InternalStatus` into its native wire status code and response format.

## 2. Where It Sits in Velda Edge

- **Execution Phase**: `pre_upstream` on Frontline Traffic Workers (`velda-worker-{i}`), after route matching and security/auth gates.
- **Hot-Path Cost**: Single `u64` check when inactive, pre-hashed SIMD lookups (`HeaderKey`) for control-header sanitization and matching, binary-search IP interval matching (`IpRangeMatcher`), and one thread-local `XorShift64*` draw shared across delay and abort rolls.
- **Protocol Isolation**: `supported_protocols()` returns `SupportedProtocols::ALL_HTTP.union(SupportedProtocols::GRPC)` (`L7` only; sync rejects attachment to `raw` L4 listeners).

## 3. Requirements & Prerequisites

- Attached routes must belong to an L7 listener (`http`, `http1`, `http2`, `http3`, or `grpc`).
- At least one fault vector (`delay.enabled`, `abort.enabled`, or `scope.allow_header_override`) must be enabled.
- Dynamic header overrides (`scope.allow_header_override: true`) require non-empty targeting (`scope.client_ips` or `scope.header_match`).

## 4. Quick Start Configuration

```json
{
  "id": "fault_injection",
  "config": {
    "scope": {
      "client_ips": ["10.0.0.0/8"],
      "header_match": {
        "name": "x-chaos-test",
        "exact_match": "enabled"
      },
      "allow_header_override": true,
      "max_override_delay_ms": 5000
    },
    "delay": {
      "enabled": true,
      "fixed_delay_ms": 150,
      "percentage": 10
    },
    "abort": {
      "enabled": true,
      "internal_status": "service_unavailable",
      "percentage": 5
    }
  }
}
```

## 5. Documentation Directory

- [RUNBOOK.md](RUNBOOK.md) — Parameter reference, `InternalStatus` wire mapping table, recipes, and incident triage.
- [ARCHITECTURE.md](ARCHITECTURE.md) — Memory layout, PRNG threshold bitpacking, header sanitization, and protocol decoupling.
- [TEST.md](TEST.md) — Verified unit test results (`cargo test -p velda-plugin -- fault_injection`).
