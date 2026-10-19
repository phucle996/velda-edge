# HTTP Fault Injection Plugin (`http_fault_injection`)

The `http_fault_injection` plugin delivers enterprise-grade **Chaos Engineering & Microservice Resilience Testing** specifically engineered for the HTTP family (HTTP/1.1, HTTP/2 Web/REST, and HTTP/3).

Operating within the `pre_upstream` execution phase, it deliberately introduces artificial network delays and simulated upstream server failures directly before request dispatch to backend services. This enables engineering teams to rigorously test downstream clients, circuit breakers, timeout policies, fallback mechanisms, and automated alerting without modifying application code.

---

## 1. Architectural Mindset & Core Invariants

1. **Protocol Separation Invariant (Rule 2.7)**:
   - Exclusively serves HTTP family traffic. Dedicated gRPC RPC pipelines use `grpc_fault_injection`.
2. **Reverse Permission Granting**:
   - Declares `PluginCapability { header: Read, payload: None }`. Regular production requests that bypass or survive fault injection retain **100% Zero-Copy streaming passthrough**.
3. **Zero-IO Hot-Path & Zero-Allocations**:
   - Zero dynamic JSON parsing, zero disk I/O, zero network RPCs.
   - Response bodies are pre-allocated as `Bytes` at configuration compile time (~10ns clone on abort).
   - Uniform PRNG uses a lock-free, zero-dependency thread-local XorShift64* generator (~1ns per call).
   - Percentages are converted into single-cycle 64-bit integer thresholds (`fast_random_u64() < threshold`).
4. **Disjoint Interval IP Matching**:
   - All IPv4/IPv6 CIDR prefixes are compiled into sorted, disjoint numeric intervals (`[start, end]`).
   - Matching executes in $O(\log N)$ via cache-friendly binary search (`partition_point`) with zero heap allocations and zero linear scanning.
5. **Execution Precedence & Sequence**:
   - When both Delay and Abort criteria match, the gateway delays execution first, then aborts:
     $$\mathbf{Scope\ Filter} \longrightarrow \mathbf{Delay\ (sleep)} \longrightarrow \mathbf{Abort\ (Action::Respond)} \longrightarrow \mathbf{Continue}$$
   - Accurately models real-world cascading database locks and slow service timeouts (e.g. 3-second hang followed by a 504 Gateway Timeout).

---

## 2. Injection Capabilities

### 2.1 Unified Range Delay
Delay is modeled strictly as a contiguous range `[min_ms, max_ms]`.
- **Fixed Delay**: When `min_ms == max_ms` (e.g. `[200, 200]`), it natively executes a fixed 200ms delay.
- **Jitter / Network Range**: When `min_ms < max_ms` (e.g. `[100, 400]`), it uniformly selects an integer in $[min, max]$ via Lemire reduction with zero floating-point division.
- **Asynchronous Execution**: Pauses non-blocking on the Tokio runtime (`Action::Delay`). If downstream clients disconnect during delay, Tokio cleanly aborts the timer and drops the task.

### 2.2 Plain-Text Abort Response
Following industry standards established by Envoy and Nginx:
- **Default Body**: Returns `text/plain; charset=utf-8` with `"fault filter abort\n"`. Extremely lightweight (~20 bytes), eliminating JSON parser overhead in microservices, mobile apps, browsers, and cURL CLI scripts.
- **Custom Body**: Supports optional custom text strings, pre-allocated into `Bytes` during startup.
- **Custom Headers**: Pre-compiles headers such as `x-velda-fault: abort` and user-defined metadata.

---

## 3. Dynamic Header Overrides & Production Security Safeguards

To empower QA automation engineers to dynamically manipulate fault parameters without redeploying configurations, the plugin supports **Header-driven Overrides**.

### 3.1 Override Headers
- `x-fault-delay-ms`: Overrides delay duration to exactly $N$ milliseconds (e.g. `x-fault-delay-ms: 350`).
- `x-fault-abort-code`: Overrides abort status code to the specified HTTP code (e.g. `x-fault-abort-code: 503`).

### 3.2 Security Safeguards (Mandatory)

> [!WARNING]
> Allowing arbitrary downstream clients to dictate server delays exposes gateways to **Slowloris Denial-of-Service** attacks. Velda Edge enforces four strict security barriers:

1. **Default Disabled (`allow_header_override: false`)**:
   - By default, `x-fault-*` headers from clients are **100% ignored**. Gateway administrators must explicitly toggle this flag for testing environments.
2. **Upper Bound Clamping (`max_override_delay_ms`)**:
   - Client-requested delays are clamped to `max_override_delay_ms` (default: `60,000ms` / 60 seconds). A malicious header such as `x-fault-delay-ms: 9999999` is automatically truncated.
3. **HTTP Status Validation**:
   - Only valid HTTP status codes in `200..=599` are accepted. Malformed or out-of-range values are ignored.
4. **Scope Isolation (Dual Binding)**:
   - When `client_ips` or `header_match` are configured, **the scope check executes first**. If an external attacker on the public internet sends `x-fault-delay-ms`, but their IP is not in `client_ips`, the request **bypasses immediately in ~1-2ns** and is served normally!

---

## 4. Configuration Schema

```json
{
  "id": "http_fault_injection",
  "phase": "pre_upstream",
  "config": {
    "delay": {
      "percentage": 10.0,
      "range_ms": [100, 300]
    },
    "abort": {
      "percentage": 5.0,
      "http_status": 503,
      "body": "custom service unavailable message\n",
      "headers": {
        "x-custom-fault": "true"
      }
    },
    "condition": {
      "client_ips": ["10.0.0.0/8", "192.168.1.0/24"],
      "header_match": {
        "name": "x-chaos-token",
        "value": "staging-secret-key-123"
      },
      "allow_header_override": true,
      "max_override_delay_ms": 30000
    }
  }
}
```

### Field Definitions

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `delay.percentage` | float | `0.0` | Probability ($0.0\% - 100.0\%$) of injecting delay into scoped requests. |
| `delay.range_ms` | `[u64, u64]` | None | Range `[min_ms, max_ms]` of delay in milliseconds. |
| `delay.fixed_ms` | u64 | None | Convenience alternative to `range_ms: [fixed, fixed]`. |
| `abort.percentage` | float | `0.0` | Probability ($0.0\% - 100.0\%$) of aborting scoped requests. |
| `abort.http_status` | u16 | `503` | HTTP status code returned upon abort (`200..=599`). |
| `abort.body` | string | `"fault filter abort\n"` | Plain-text response body payload. |
| `abort.headers` | map | `{}` | Key-value pairs injected into the abort response. |
| `condition.client_ips` | array[string] | `[]` | Whitelisted IPv4/IPv6 CIDRs or exact IPs. Non-matching traffic bypasses immediately. |
| `condition.header_match.name` | string | None | Header name required to trigger fault injection. |
| `condition.header_match.value` | string | None | Optional exact value match. If omitted, checks for header presence. |
| `condition.allow_header_override` | bool | `false` | Enables `x-fault-delay-ms` and `x-fault-abort-code` client overrides. |
| `condition.max_override_delay_ms` | u64 | `60000` | Safety clamp ceiling for client-requested delay durations. |

---

## 5. Typical Production & QA Recipes

### Recipe A: Safe Automated QA Testing on Production
Target only internal QA test suites via an IP whitelist and secret token header:
```json
{
  "id": "http_fault_injection",
  "config": {
    "delay": { "percentage": 100.0, "range_ms": [200, 500] },
    "abort": { "percentage": 10.0, "http_status": 503 },
    "condition": {
      "client_ips": ["10.0.0.0/8"],
      "header_match": { "name": "x-qa-auth", "value": "internal-secret" }
    }
  }
}
```

### Recipe B: Production Circuit-Breaker GameDay
Test frontend resilience when a recommendation service experiences partial degradation:
```json
{
  "id": "http_fault_injection",
  "config": {
    "abort": {
      "percentage": 0.05,
      "http_status": 503
    }
  }
}
```
*(Only 5 out of every 10,000 requests are aborted; real users experience zero delay, proving graceful UI degradation).*
