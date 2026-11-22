# HTTP Request Resource Limiter Plugin (`resource_limit`)

```text
+-----------------------------------------------------------------------------------+
|                      Resource Limit Pre-Route Architecture                        |
|                                                                                   |
|  [Inbound L7 Request] (HTTP/1.1, HTTP/2, HTTP/3, gRPC)                            |
|         │                                                                         |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Frontline Traffic Worker (velda-worker-{i}) — Pre-Route Phase               │  |
|  │  - 1. URI Length Check: req.uri.len() > max_uri_length                      │  |
|  │     └─► Action::Reject(UriTooLong)          [HTTP 414 URI Too Long]         │  |
|  │  - 2. Header Count Check: headers.len() > max_headers_count                 │  |
|  │     └─► Action::Reject(HeaderFieldsTooLarge)[HTTP 431 Header Fields Large]  │  |
|  │  - 3. Single & Total Header Byte Bounds:                                    │  |
|  │     └─► Action::Reject(HeaderFieldsTooLarge)[HTTP 431 Header Fields Large]  │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │                                         |
|                         Within All Bounds -> Action::Continue                     |
+-----------------------------------------------------------------------------------+
```

The **`resource_limit`** plugin is Velda Edge's O(1) structural bounds enforcer. It rejects oversized URIs and header floods before downstream routing, WAF scanning, or body streaming consumes CPU or memory.

---

## 1. What This Plugin Does

1. **URI Size Protection**:
   - Rejects requests whose URI Path + Query exceeds `max_uri_length` with `RejectReason::UriTooLong` (HTTP `414`).
2. **Header Flood & Memory Exhaustion Defense**:
   - Enforces `max_headers_count`, `max_header_bytes` (cumulative), and `max_single_header_bytes` (per-header), returning `RejectReason::HeaderFieldsTooLarge` (HTTP `431`) with zero heap allocation.
3. **Downstream CPU Shielding**:
   - Positioned immediately before `waf_lite`, it ensures oversized malicious payloads are terminated in nanoseconds before pattern scanning runs.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_route/resource_limit/`
- **Execution Stage**: `PreRoutePlugin` (compile-time fixed)
- **Runtime Placement**: Executed inline on Frontline Traffic Workers (`velda-worker-{i}`).
- **Recommended Pipeline Order**: `ip_filter` $\to$ `resource_limit` $\to$ `cors_preflight` $\to$ `waf_lite`.

---

## 3. Requirements & Prerequisites

1. **Explicit Configuration Contract**: All 4 bounds (`max_uri_length`, `max_headers_count`, `max_header_bytes`, `max_single_header_bytes`) must be explicitly declared within valid ranges.
2. **Structural Invariant**: `max_single_header_bytes` must be `<= max_header_bytes`.

---

## 4. Quick Start Configuration

```json
{
  "id": "resource_limit",
  "config": {
    "max_uri_length": 8192,
    "max_headers_count": 128,
    "max_header_bytes": 32768,
    "max_single_header_bytes": 16384
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Parameter specifications, strict API vs Enterprise SSO presets, and incident troubleshooting.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Deep internals (Zero-allocation header workspace bounds checking, fast-fail mechanics).
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 5-tier diagnostic benchmark matrix and verified `x86-64-v3` release assembly audit.
- **[TEST.md](./TEST.md)**: Unit and boundary validation test matrix.
