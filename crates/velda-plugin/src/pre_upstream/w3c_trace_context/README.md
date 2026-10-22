# W3C TraceContext Plugin (`w3c_trace_context`)

Strict, zero-allocation W3C TraceContext propagation and distributed trace identifier generator for the **Velda Edge Data Plane**. Enforces the W3C Trace Context recommendation across ingress request boundaries, stamping upstream requests with compliant `traceparent` headers and populating downstream response tracking metadata.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `w3c_trace_context` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (Typically declared before upstream connection leasing) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` (Stamps headers on request and response) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, and gRPC) |
| **Hot-Path Overhead** | **$5\ \text{to}\ 10\ \text{ns}$**; zero disk I/O, zero network I/O, zero heap allocation |

### What it Enforces
- **W3C Format Conformance**: Enforces the canonical 55-character format: `version-trace_id-parent_id-trace_flags` (e.g., `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`).
- **Distributed Continuity**: Preserves existing valid client `trace_id` values while rotating `parent_id` to represent the edge gateway hop.
- **Dynamic Generation**: Generates cryptographically collision-resistant 128-bit `trace_id` and 64-bit `span_id` values when clients do not supply a valid `traceparent`.
- **Downstream Tracking**: Automatically injects canonical tracking headers (`x-trace-id`, `x-span-id`, `x-request-id`) onto downstream responses for end-to-end client correlation.

---

## 2. Configuration Field Specification

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: To ensure absolute operational transparency, **every parameter must be explicitly declared** in the JSON configuration. Velda Edge rejects hidden conventions and magic defaults. If any parameter is omitted, configuration validation will fail fast during sync.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `generate_if_missing` | boolean | **Required** | `true` or `false` | `true` | When `true`, generates a new 128-bit trace ID and 64-bit span ID if the downstream request lacks a valid W3C `traceparent`. |
| `forward_tracestate` | boolean | **Required** | `true` or `false` | `true` | When `true`, forwards vendor-specific `tracestate` headers unmodified to upstream backends. |
| `sampled` | boolean | **Required** | `true` or `false` | `true` | Default sampling flag applied to generated traces (`true` produces flags `01` = sampled; `false` produces `00` = not sampled). |

---

## 3. Specification Conformance & Headers Contract

### A. W3C Header Format
The plugin enforces strict byte-level validation according to the W3C TraceContext Recommendation:
1. **Length**: Exactly 55 ASCII characters separated by hyphens (`-`).
2. **Version**: Exactly `00` (Version `ff` or invalid characters are rejected).
3. **Trace ID**: 32 lowercase hex characters (16 bytes). Must not be all zeros (`00000000000000000000000000000000`).
4. **Parent/Span ID**: 16 lowercase hex characters (8 bytes). Must not be all zeros (`0000000000000000`).
5. **Trace Flags**: 2 lowercase hex characters (`01` = sampled, `00` = not sampled).

### B. Headers Inspected & Injected

| Direction | Header Name | Action | Description |
| :--- | :--- | :--- | :--- |
| **Downstream Ingress** | `traceparent` | Inspected | Extracted and validated according to the W3C specification. |
| **Downstream Ingress** | `tracestate` | Inspected | Optional vendor opaque state list forwarded if `forward_tracestate == true`. |
| **Upstream Request** | `traceparent` | **Injected** | Formatted as `00-{trace_id}-{new_span_id}-{flags}`. |
| **Downstream Response** | `x-trace-id` | **Injected** | 32-character lowercase hex string of the active trace ID. |
| **Downstream Response** | `x-span-id` | **Injected** | 16-character lowercase hex string of the gateway span ID. |

---

## 4. Configuration Examples

### Profile 1: Full Propagation & Generation (Recommended Production)
*Ensures every single request traversing the edge has an authoritative distributed trace context.*

```json
{
  "id": "w3c_trace_context",
  "config": {
    "generate_if_missing": true,
    "forward_tracestate": true,
    "sampled": true
  }
}
```

### Profile 2: Strict Pass-Through (Downstream Driven Only)
*Does not generate traces for untraced clients; only forwards traces for clients that already include valid headers.*

```json
{
  "id": "w3c_trace_context",
  "config": {
    "generate_if_missing": false,
    "forward_tracestate": true,
    "sampled": false
  }
}
```

---

## 5. Route Attachment Example (`routes.json`)

To enable W3C TraceContext generation and propagation on an edge route, declare `"w3c_trace_context"` in the route's `plugins` array:

```json
{
  "id": "api-orders-route",
  "path": "/api/v1/orders",
  "upstream": "orders-backend",
  "plugins": [
    "w3c_trace_context",
    "jwt_auth",
    "rate_limit",
    "opentelemetry_logger",
    "opentelemetry_tracer"
  ]
}
```

---

## 6. Ecosystem Integration

```
[ Downstream Client ]
         │
         │ (Optional traceparent)
         ▼
┌──────────────────────────────────────┐
│ Phase 2: w3c_trace_context           │ ── Generates or propagates trace_id & span_id
└──────────────────┬───────────────────┘
                   │
                   │ (Propagates traceparent to Upstream)
                   ▼
┌──────────────────────────────────────┐
│ Upstream Microservices Backend       │
└──────────────────┬───────────────────┘
                   │
                   │ (Response returns to Edge)
                   ▼
┌──────────────────────────────────────┐
│ Phase 3: opentelemetry_tracer        │ ── Exports server span matching trace_id & span_id
├──────────────────────────────────────┤
│ Phase 3: opentelemetry_logger        │ ── Stitches access log record to the same trace_id
└──────────────────────────────────────┘
```
