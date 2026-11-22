# HTTP Method Filter Plugin (`method_filter`)

```text
+-----------------------------------------------------------------------------------+
|                       Method Filter Pre-Route Architecture                        |
|                                                                                   |
|  [Inbound L7 Request] (HTTP/1.1, HTTP/2, HTTP/3)                                  |
|         │                                                                         |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Frontline Traffic Worker (velda-worker-{i}) — Pre-Route Phase               │  |
|  │  - HttpMethod::from_bytes(req.method.as_str().as_bytes()) -> u8 (0..=9)     │  |
|  │  - bit = 1u16 << method_id                                                  │  |
|  │  - 1-Cycle Bitwise Check: (self.allowed_mask & bit) != 0 ?                  │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │                                         |
|                             [Allowed by Bitmask?]                                 |
|                              /                 \                                  |
|                            YES                  NO                                |
|                            /                      \                               |
|                 Action::Continue         [Unknown (0) & custom_methods?]          |
|                 (Proceed to Routing)      /                          \            |
|                                         YES (Scan custom_methods)     NO          |
|                                          │                            │           |
|                                          ▼                            ▼           |
|                            Action::Continue / Reject    Action::Reject            |
|                                                         (MethodNotAllowed / 405)  |
+-----------------------------------------------------------------------------------+
```

The **`method_filter`** plugin is Velda Edge's 1-cycle HTTP method allowlist and verb-tampering guard (`phase: "pre_route"`). It converts incoming HTTP request methods into compact `HttpMethod` integer discriminants (`0..=9`) and evaluates them against a pre-compiled 16-bit bitmask before route lookup, WAF scanning, or body streaming.

---

## 1. What This Plugin Does

1. **1-Cycle Bitwise Method Filtering**:
   - Evaluates standard HTTP methods (`GET`, `POST`, `PUT`, `DELETE`, `HEAD`, `OPTIONS`, `PATCH`, `CONNECT`, `TRACE`) via `(allowed_mask & (1u16 << id)) != 0` in a single CPU instruction with zero heap allocations.
2. **Instant Unknown / Verb Tampering Rejection**:
   - Unrecognized or legacy verbs (`TRACK`, `DEBUG`, WebDAV `PROPFIND`, `MKCOL`) map to `HttpMethod::Unknown` (bit `0`), which is automatically rejected unless explicitly declared in `allowed_methods`.
3. **Strict HTTP Protocol Isolation**:
   - Scoped exclusively to `SupportedProtocols::ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3), leaving dedicated gRPC listeners (which always use HTTP/2 `POST`) completely untouched.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_route/method_filter/`
- **Execution Phase**: `phase: "pre_route"`
- **Runtime Placement**: Executed inline on Frontline Traffic Workers (`velda-worker-{i}`).
- **Recommended Pipeline Order**: `ip_filter` $\to$ `method_filter` $\to$ `resource_limit` $\to$ `cors_preflight` $\to$ `waf_lite`.

---

## 3. Requirements & Prerequisites

1. **Explicit Allowlist**: `allowed_methods` must contain at least one valid RFC 9110 HTTP method token.
2. **CORS Compatibility**: When pairing with `cors_preflight` on browser-facing APIs, include `"OPTIONS"` in `allowed_methods` if `method_filter` runs earlier in the chain.

---

## 4. Quick Start Configuration

```json
{
  "id": "method_filter",
  "phase": "pre_route",
  "config": {
    "allowed_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Parameter specifications, REST/Static/WebDAV presets, and troubleshooting playbooks.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Deep internals (16-bit bitmask layout, `HttpMethod` mapping, 1-cycle bitwise math, protocol isolation).
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 5-tier diagnostic benchmark matrix and verified `x86-64-v3` release assembly audit.
- **[TEST.md](./TEST.md)**: Unit and validation test matrix.
