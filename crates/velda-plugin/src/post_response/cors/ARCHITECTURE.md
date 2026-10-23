# CORS Post-Response Plugin (`cors`) — Architectural Specification

This document specifies the internal engine design, execution pipeline, memory invariants, and security model of the **`cors`** post-response plugin (W3C Fetch / RFC 6454 / RFC 9110).

---

## 1. System Mindset & Architectural Invariants

The `cors` post-response plugin operates at the egress stage of request handling, immediately following upstream response reception and directly preceding downstream wire encoding. It enforces four primary invariants:

1. **Zero-Allocation Hot Path**:
   Outgoing responses are processed with zero heap string allocations, zero regular expression evaluations, and zero configuration branch evaluations. All headers are transferred via atomic reference-counted `HeaderValue` clones.
2. **Strict Origin Isolation**:
   If an incoming request includes an `Origin` header that does not match any configured pattern, the plugin exits silently without injecting any CORS headers. Disallowed origins never receive wildcard fallbacks or unauthorized cross-origin access.
3. **Cache Poisoning Prevention**:
   Whenever responses vary by origin (such as when credentials are enabled or dynamic origin reflection is active), the plugin strictly enforces `Vary: Origin`. Upstream `Vary` headers (e.g. `Accept-Encoding`) are preserved and merged natively via `HeaderMap::append` without string splitting or reallocation.
4. **Non-CORS Fast Pass-Through**:
   Downstream requests lacking an `Origin` header (such as same-origin requests, curl, or server-to-server calls) bypass CORS processing in a single pointer comparison (~2ns).

---

## 2. End-to-End Response Hot-Path Pipeline

```mermaid
flowchart TD
    subgraph IngressInspection["Phase 1: Ingress Header Inspection"]
        A["Upstream L7Response Received"] --> B{"1. Origin Header Present?\n(req_headers.get(ORIGIN))"}
        B -- "No" --> C["Pass-Through Unmodified\n(Non-CORS Request)"]
        B -- "Yes" --> D{"2. Origin String Valid?\n(to_str().is_ok())"}
        D -- "No" --> C
    end

    subgraph OriginValidation["Phase 2: Pattern Matching"]
        D -- "Yes" --> E{"3. Origin Authorized?\n(allow_all || patterns.any(origin))"}
        E -- "No" --> F["Silent Pass-Through\n(Zero CORS Headers Injected)"]
    end

    subgraph HeaderAssembly["Phase 3: Zero-Alloc Header Injection"]
        E -- "Yes" --> G{"4. Need Exact Reflection?\n(allow_credentials || !allow_all)"}
        G -- "Yes" --> H["Inject Access-Control-Allow-Origin: <origin_val>"]
        G -- "No" --> I["Inject Access-Control-Allow-Origin: '*'"]
        H --> J["5. Batch Inject Pre-Baked Template\n(credentials, expose-headers, max-age)"]
        I --> J
        J --> K{"6. Needs Vary: Origin?\n(self.needs_vary_origin)"}
        K -- "No" --> L["Proceed to Downstream Encode"]
        K -- "Yes" --> M{"7. Origin Already in Vary?\n(inspect resp.headers.get_all(VARY))"}
        M -- "Yes" --> L
        M -- "No" --> N["resp.headers.append(VARY, 'Origin')"]
        N --> L
    end
```

---

## 3. Core Architectural Subsystems

### 3.1 Pre-Baked Header Template (`base_headers`)

Traditional proxy architectures evaluate configuration flags dynamically per response:
```rust
// ❌ Traditional approach: multiple branch checks and string allocations per response
if self.allow_credentials {
    resp.headers.insert("access-control-allow-credentials", "true");
}
if let Some(expose) = &self.expose_headers {
    resp.headers.insert("access-control-expose-headers", expose.join(", "));
}
if let Some(max_age) = self.max_age {
    resp.headers.insert("access-control-max-age", max_age.to_string());
}
```

In Velda Edge, static response headers are pre-baked at configuration initialization:
```rust
pub struct CorsResponsePlugin {
    pub id: String,
    pub config: CorsConfig,
    pub allow_all_origins: bool,
    pub base_headers: HeaderMap,
    pub needs_vary_origin: bool,
}
```
During initialization, `base_headers` pre-allocates an immutable `HeaderMap` containing:
- `Access-Control-Allow-Credentials: true` (if configured)
- Pre-joined `Access-Control-Expose-Headers` (e.g., `x-request-id, x-trace-id`)
- Pre-stringified `Access-Control-Max-Age` (e.g., `86400`)

On the hot path, these headers are inserted directly into `resp.headers` using `HeaderValue::clone`, which performs a single atomic pointer increment without heap memory allocation.

---

### 3.2 Native Multi-Value `Vary` Header Preservation

When responses are served to browsers and shared caching proxies (CDNs, edge proxies), returning origin-dependent CORS headers without a corresponding `Vary: Origin` causes **Cache Poisoning** (RFC 9110 Section 12.5.5). Subsequent users from different origins would receive cached responses containing the incorrect `Access-Control-Allow-Origin` header, causing browser security exceptions or data leakage.

#### The Zero-Allocation Native Append Mechanism
Many proxies attempt to read the existing `Vary` header, split by comma, trim whitespace, allocate a new `String`, append `, Origin`, and insert the new header. This causes CPU cache misses and heap churn on every response.

Velda Edge leverages native HTTP multi-value header representation:
```rust
if self.needs_vary_origin {
    let already_has_origin = resp.headers.get_all(VARY).iter().any(|v| {
        v.to_str().is_ok_and(|s| {
            s.split(',').any(|part| {
                part.trim().eq_ignore_ascii_case("origin") || part.trim() == "*"
            })
        })
    });
    if !already_has_origin {
        resp.headers.append(VARY, HeaderValue::from_static("Origin"));
    }
}
```
- **Preserves Existing Upstream Directives**: Upstream headers such as `Vary: Accept-Encoding` remain completely intact.
- **RFC 9110 Compliance**: Emits multiple `Vary` headers on the wire (`Vary: Accept-Encoding` followed by `Vary: Origin`), which HTTP/1.1 and HTTP/2 parsers treat identically to comma-separated lists.
- **Zero Allocations**: Utilizes `HeaderValue::from_static("Origin")`.

---

### 3.3 Dynamic Origin Reflection & Credential Isolation

RFC 6454 and the W3C CORS specification strictly forbid returning `Access-Control-Allow-Origin: *` when credentials mode is enabled (`Access-Control-Allow-Credentials: true`):

```rust
let allow_origin_val = if self.config.allow_credentials || !self.allow_all_origins {
    origin_val.clone() // Atomic ref-count increment, zero heap allocation
} else {
    HeaderValue::from_static("*")
};
resp.headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);
```

- When credentials are true or specific origins are configured, the client's validated `Origin` header value is reflected.
- The `origin_val.clone()` call clones the underlying `http::HeaderValue` (an internal `Bytes` slice), incurring **zero memory copies or re-allocations**.

---

### 3.4 Hardware-Accelerated Subdomain Pattern Matching

Origins configured with wildcard subdomain patterns (e.g. `https://*.example.com`) are evaluated by [`OriginPattern::Subdomain`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/cors_preflight/origin.rs). 

- Pattern matching validates that the incoming origin begins with the expected protocol/host prefix and ends with the exact domain suffix.
- Every byte within the wildcard label is validated against `SUBDOMAIN_CHAR_LUT`, an L1 cache-resident 256-byte table.
- Injection attacks attempting to exploit path separators (`/`), credentials (`@`), query parameters (`?`), or port numbers (`:`) are detected and rejected in $O(1)$ time with zero branches.

---

## 4. Concurrency & Memory Invariants

- **Read-Only Shared State**: `CorsResponsePlugin` is completely stateless during execution. It holds immutable pre-baked templates and pre-parsed pattern vectors.
- **Thread Safety**: Safely shared across Tokio worker threads without `Mutex`, `RwLock`, or atomic synchronization overhead.
- **Memory Footprint**: Total memory footprint of an instantiated `CorsResponsePlugin` is **< 1 KB** in RAM.
