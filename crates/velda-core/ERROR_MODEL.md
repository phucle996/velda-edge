# velda-core Canonical Error & Rejection Taxonomy

This document specifies the unified error taxonomy, semantic rejection contracts, and downstream wire protocol mapping invariants in `crates/velda-core/src/error.rs`.

---

## 1. Architectural Mindset: Separation of Concerns

`velda-core` enforces a strict architectural boundary between two completely distinct classes of failures:

1. **Subsystem Operational Errors ([`ErrorKind`])**:
   - Internal infrastructure failures occurring during connection lifecycle, socket I/O, routing table lookup, or upstream pool management.
   - Used for internal logging, observability metrics, circuit breaking, and retry decisions.
2. **Semantic Pipeline Rejections ([`RejectReason`])**:
   - Intentional client-facing rejections emitted by edge security policies, authentication filters, WAF rules, or rate limiting buckets.
   - Abstracted entirely as **pure semantic vocabulary** without transport-specific framing or status numbers.

```text
       ┌────────────────────────────────────────────────────────┐
       │              INTERNAL TRANSPORT FAILURES               │
       │                   ErrorKind (1 byte)                   │
       │    [ RouteNotFound | UpstreamUnavailable | Timeout ]   │
       │    Used by edge core for retries and circuit breakers  │
       └────────────────────────────────────────────────────────┘

       ┌────────────────────────────────────────────────────────┐
       │               POLICY & SECURITY FAILURES               │
       │                 RejectReason (1 byte)                  │
       │    [ Unauthorized | Forbidden | TooManyRequests ]      │
       │    Emitted by plugins via Action::Reject(reason)       │
       └───────────────────────────┬────────────────────────────┘
                                   │
                                   ▼ (Protocol-Native Synthesis)
       ┌────────────────────────────────────────────────────────┐
       │                DOWNSTREAM WIRE FRAMING                 │
       │   • HTTP/1.1: ASCII Status Line + Body (velda-http1)   │
       │   • HTTP/2: Zero-Body HEADERS Frame (velda-http2)      │
       │   • HTTP/3: QUIC Datagram HEADERS (velda-http3)        │
       │   • gRPC: Canonical grpc-status Trailer (velda-grpc)   │
       └────────────────────────────────────────────────────────┘
```

---

## 2. Why `velda-core` Strictly Forbids Status Numbers

In legacy proxies, internal plugins return raw integers like `403` or `429`. In Velda Edge, this is considered a **critical architectural smell**:

1. **Protocol Pollution & gRPC Corruption**:
   gRPC runs on HTTP/2 binary framing. When an RPC fails authentication, the gRPC specification mandates returning an HTTP status `:status: 200` with trailers `grpc-status: 16 (UNAUTHENTICATED)` and an optional `grpc-message`. Emitting an HTTP `401 Unauthorized` with an ASCII error body corrupts gRPC client stub deserialization.
2. **Transport Agnosticism**:
   Velda Edge serves diverse traffic including HTTP/1.1, HTTP/2, HTTP/3 (QUIC), gRPC, and future protocols (e.g. MQTT, WebSocket, binary RPC). A core vocabulary crate must not assume that HTTP status numbers exist on every protocol.
3. **Decoupled Framing & Zero-Allocation Synthesis**:
   Keeping `RejectReason` abstract allows protocol drivers (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`) to pre-compile their own static, zero-allocation rejection frames ahead of time, eliminating runtime string formatting (`format!`).

---

## 3. The 17 Canonical `RejectReason` Variants

`RejectReason` represents a closed taxonomy of exactly 17 failure reasons:

### 3.1 Client-Side Rejections (4xx Domain)

| Variant | Semantic Meaning | Recommended Plugin Use Case |
| :--- | :--- | :--- |
| `BadRequest` | Malformed request syntax, illegal characters, bad framing | Protocol codecs, malformed URI |
| `Unauthorized` | Missing, expired, or invalid authentication token/credential | `jwt_auth`, `api_key_auth`, `hmac_auth` |
| `Forbidden` | Request denied by ACL, IP filter, or security policy | `ip_filter`, `waf_lite`, `csrf` |
| `NotFound` | Requested resource, API route, or path does not exist | Core router fallback |
| `MethodNotAllowed` | HTTP method or RPC method verb is not permitted | Method filter, route restriction |
| `RequestTimeout` | Client failed to transmit complete headers within deadline | Ingress idle/read timer |
| `Conflict` | Request conflicts with current backend resource state | Idempotency filter |
| `PayloadTooLarge` | Payload or message size exceeds configured limits | `resource_limit`, WAF body scan |
| `UriTooLong` | Request URI or query string exceeds maximum allowable length | `resource_limit` |
| `UnsupportedMediaType` | Payload Content-Type or encoding is not supported | Content-type validator |
| `TooManyRequests` | Rate limit, concurrency quota, or burst token exhausted | `rate_limit` |
| `HeaderFieldsTooLarge` | Individual header or entire header table exceeds size limit | Ingress header validator, `resource_limit` |
| `ClientClosedRequest` | Client closed connection or reset stream prematurely | Inflight disconnect detection |

### 3.2 Server & Upstream Rejections (5xx Domain)

| Variant | Semantic Meaning | Recommended Edge Use Case |
| :--- | :--- | :--- |
| `InternalError` | Unexpected runtime failure on edge hot path | Uncaught runtime error |
| `BadGateway` | Upstream backend returned an invalid response or reset | Upstream protocol parser error |
| `ServiceUnavailable` | Upstream connection pool exhausted, circuit breaker tripped | Upstream connection failure |
| `GatewayTimeout` | Upstream backend failed to reply within configured timeout | Upstream request timeout |

---

## 4. Downstream Protocol Synthesis Matrix

Each downstream protocol engine maintains its own pre-compiled mapping:

| `RejectReason` | HTTP/1.1 Status | HTTP/2 Status | HTTP/3 Status | gRPC Status | gRPC Trailer Code |
| :--- | :---: | :---: | :---: | :--- | :---: |
| `BadRequest` | 400 | 400 | 400 | `INVALID_ARGUMENT` | 3 |
| `Unauthorized` | 401 | 401 | 401 | `UNAUTHENTICATED` | 16 |
| `Forbidden` | 403 | 403 | 403 | `PERMISSION_DENIED` | 7 |
| `NotFound` | 404 | 404 | 404 | `NOT_FOUND` | 5 |
| `MethodNotAllowed` | 405 | 405 | 405 | `UNIMPLEMENTED` | 12 |
| `RequestTimeout` | 408 | 408 | 408 | `DEADLINE_EXCEEDED` | 4 |
| `Conflict` | 409 | 409 | 409 | `ALREADY_EXISTS` | 6 |
| `PayloadTooLarge` | 413 | 413 | 413 | `FAILED_PRECONDITION` | 9 |
| `UriTooLong` | 414 | 414 | 414 | `INVALID_ARGUMENT` | 3 |
| `UnsupportedMediaType` | 415 | 415 | 415 | `INVALID_ARGUMENT` | 3 |
| `TooManyRequests` | 429 | 429 | 429 | `RESOURCE_EXHAUSTED` | 8 |
| `HeaderFieldsTooLarge` | 431 | 431 | 431 | `RESOURCE_EXHAUSTED` | 8 |
| `ClientClosedRequest` | 499 | 499 | 499 | `CANCELLED` | 1 |
| `InternalError` | 500 | 500 | 500 | `INTERNAL` | 13 |
| `BadGateway` | 502 | 502 | 502 | `UNAVAILABLE` | 14 |
| `ServiceUnavailable` | 503 | 503 | 503 | `UNAVAILABLE` | 14 |
| `GatewayTimeout` | 504 | 504 | 504 | `DEADLINE_EXCEEDED` | 4 |

---

## 5. Header Isolation Invariant on Reject

When `Action::Reject(reason)` is returned:
1. **Bit 0 Purge**: 100% of internal headers, client authorization tokens, and request routing tags stored in `HeaderWorkspace` are dropped and cleared from the response frame.
2. **Bit 1 Retention**: Only headers tagged with `Bit 1 - Bypass on Reject` (queried via `ws.bypass_headers()`) are copied into the client-facing rejection frame.
3. **Slot 0 Guarantee**: The authoritative `x-request-id` pre-loaded in Slot 0 is unconditionally included, ensuring client-side trace correlation on every rejection.
