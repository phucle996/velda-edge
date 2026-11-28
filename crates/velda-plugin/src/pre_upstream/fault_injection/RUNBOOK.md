# Operations & Configuration Runbook (`fault_injection`)

## 1. Configuration Parameter Reference

Schema uses `#[serde(deny_unknown_fields)]`; unknown keys fail sync validation immediately.

| Field Path | Type | Required / Default | Constraints |
| :--- | :--- | :--- | :--- |
| `scope` | `object` | Optional (`FaultScopeConfig::default`) | Omit to apply fault rules to all matched traffic on the route |
| `scope.client_ips` | `string[]` | Default `[]` | Valid IPv4/IPv6 addresses or CIDRs (`10.0.0.0/8`, `2001:db8::/32`) |
| `scope.header_match` | `object \| null` | Default `null` | `{ "name": "<tchar-header>", "exact_match": "<string \| null>" }`; pseudo-headers (`:...`) rejected |
| `scope.allow_header_override` | `bool` | Default `false` | If `true`, requires non-empty `scope.client_ips` OR `scope.header_match` |
| `scope.max_override_delay_ms` | `u64` | Default `5000` | Must be `1..=60_000` ms |
| `delay.enabled` | `bool` | Required | Enables synthetic async sleep before downstream continuation/abort |
| `delay.fixed_delay_ms` | `u64` | Required | Must be `1..=60_000` ms when `delay.enabled == true` |
| `delay.percentage` | `u32` | Required | `0..=100`; must be `> 0` if `delay.enabled && !allow_header_override` |
| `abort.enabled` | `bool` | Required | Enables synthetic fault abort via `Action::Status(InternalStatus)` |
| `abort.internal_status` | `string` | Required | Must be an aborted `InternalStatus` name (`is_aborted() == true`; `"completed"` rejected) |
| `abort.percentage` | `u32` | Required | `0..=100`; must be `> 0` if `abort.enabled && !allow_header_override` |

Top-level constraint: at least one of `delay.enabled`, `abort.enabled`, or `scope.allow_header_override` must be `true`.

---

## 2. Dynamic Override Request Headers

When `scope.allow_header_override == true` and the incoming request passes `scope` checks:

- `x-fault-delay-ms: <u64>`: Overrides delay duration in milliseconds, filtered for `ms > 0` and clamped to `scope.max_override_delay_ms`.
- `x-fault-abort-status: <snake_case>`: Overrides abort `InternalStatus` using any aborted variant accepted by `InternalStatus::from_name` (e.g. `upstream_timeout`, `rate_limited`, `service_unavailable`).

Both headers are **always stripped** from `HeaderWorkspace` on arrival (whether scoped or outside scope) so control headers never leak upstream.

---

## 3. Wire Status Mapping & Observability Signals

On abort, `fault_injection` preserves `x-velda-fault: abort` (marked MSB bypass) and delegates wire rendering to the active protocol crate:

| Configured `abort.internal_status` | `TelemetryRecord.internal_status` | HTTP Wire Status (`velda-http1/2/3`) | gRPC Wire Code (`velda-grpc`) |
| :--- | :--- | :--- | :--- |
| `"service_unavailable"` | `ServiceUnavailable` (`7`) | `503 Service Unavailable` | `14 UNAVAILABLE` |
| `"upstream_timeout"` | `UpstreamTimeout` (`3`) | `504 Gateway Timeout` | `4 DEADLINE_EXCEEDED` |
| `"upstream_error"` | `UpstreamError` (`2`) | `502 Bad Gateway` | `14 UNAVAILABLE` |
| `"rate_limited"` | `RateLimited` (`17`) | `429 Too Many Requests` | `8 RESOURCE_EXHAUSTED` |
| `"internal_error"` | `InternalError` (`6`) | `500 Internal Server Error` | `13 INTERNAL` |
| `"bad_request"` | `BadRequest` (`8`) | `400 Bad Request` | `3 INVALID_ARGUMENT` |
| `"unauthorized"` | `Unauthorized` (`9`) | `401 Unauthorized` | `16 UNAUTHENTICATED` |
| `"forbidden"` | `Forbidden` (`10`) | `403 Forbidden` | `7 PERMISSION_DENIED` |
| `"route_not_found"` | `RouteNotFound` (`5`) | `404 Not Found` | `5 NOT_FOUND` |

---

## 4. Configuration Recipes

### QA-Scoped Canary Chaos (Zero Impact on Production)
```json
{
  "scope": {
    "header_match": { "name": "x-qa-fault-token", "exact_match": "chaos-ok" },
    "allow_header_override": true,
    "max_override_delay_ms": 3000
  },
  "delay": { "enabled": false, "fixed_delay_ms": 0, "percentage": 0 },
  "abort": { "enabled": false, "internal_status": "service_unavailable", "percentage": 0 }
}
```

### Probabilistic Downstream Degradation Test (gRPC + HTTP)
```json
{
  "scope": { "client_ips": ["10.24.0.0/16"] },
  "delay": { "enabled": true, "fixed_delay_ms": 200, "percentage": 15 },
  "abort": { "enabled": true, "internal_status": "upstream_timeout", "percentage": 5 }
}
```

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Config rejected with `cannot be enabled without scoping` | `allow_header_override: true` without `client_ips` or `header_match` | Add an internal CIDR to `scope.client_ips` or a gated `scope.header_match` |
| Config rejected with `strictly restricted to 'l7'` | `fault_injection` attached to a `raw` L4 route | Remove `fault_injection` from L4 routes; attach only to HTTP/gRPC listeners |
| Config rejected with `not an aborted fault status` | `abort.internal_status` set to `"completed"` or unknown name | Use an aborted `InternalStatus` name (e.g. `"service_unavailable"`) |
| `x-fault-abort-status` ignored at runtime | Request outside `scope`, or `allow_header_override: false` | Verify client IP / `header_match` and confirm `allow_header_override: true` |
