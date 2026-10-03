# velda-plugin

> **Status: Architectural Design & Future Roadmap**  
> This crate specifies the architectural blueprint and execution model for Velda Edge's plugin system. Implementation will follow in subsequent development phases.

---

## 1. Mission & Architectural Philosophy

Velda Edge rejects the traditional "generic dynamic plugin" model used by legacy API gateways (Kong, APISIX, Spring Cloud Gateway). In those systems, plugins accept a generic `GenericRequest` or a loose data bag (`HashMap<String, Box<dyn Any>>`) configured dynamically via JSON or environment variables.

In high-performance edge computing, that pattern introduces severe operational risks:
1. **Runtime Type-Casting Panics**: When a plugin written for HTTP REST traffic is attached to a gRPC or raw TCP listener, runtime downcasting fails, causing crashes or malformed protocol states.
2. **Leaky Protocol Abstractions**: Forcing gRPC into an HTTP model destroys gRPC semantics (canonical `grpc-status` trailers, 5-byte Length-Prefixed Message binary framing, and bidirectional streaming).
3. **Hot-Path Indirection & Invalidation**: Virtual dispatch, heap-allocated dynamic bags, and JSON inspection on every request violate the Zero-IO hot-path invariant.

### The Velda Invariant: Protocol-Bound Plugins (Compile-Time Protocol Mapping)

Velda Edge establishes **Protocol-Bound Plugins**:
- **Explicit Protocol Binding**: A plugin explicitly declares which protocol(s) it supports directly in code.
- **Zero Runtime Reflection**: Plugins receive native, protocol-specific structures directly (e.g., `Http1RequestHead`, `GrpcServerStream`) rather than generic abstraction layers.
- **Compiled Dispatch**: Plugin attachment is resolved and compiled into RAM ahead of time (`RuntimeSnapshot`). Zero JSON parsing, zero dynamic schema validation, and zero reflection in the request serving path.
- **"Duplicate First, Abstract Second" (Rule 2.1)**: If a plugin author wants to support both HTTP/1.1 and gRPC, they implement dedicated hooks for each. A few lines of duplication guarantee 100% protocol compatibility, zero leaky abstractions, and allow the Rust compiler to monomorphize and inline handlers directly into the serving loop.

---

## 2. Protocol Isolation & Pipeline Alignment

According to **Rule 2.7 of `AGENTS.md`** (*"HTTP is HTTP, gRPC is gRPC. Both protocols have distinct semantics and must remain completely decoupled"*), plugins must align with strict pipeline isolation:

```
[ Ingress Connection ]
         │
         ▼
[ Protocol Classifier (velda-composer) ]
         │
         ├──────────────────────────────┬──────────────────────────────┐
         ▼ (protocol = "http1")         ▼ (protocol = "http2")         ▼ (protocol = "grpc")
┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌──────────────────────────────┐
│ HTTP/1.1 Ingress Pipeline    │ │ HTTP/2 Multiplexed Pipeline  │ │ Dedicated gRPC Pipeline      │
│                              │ │                              │ │                              │
│ Hook: on_request_head        │ │ Hook: on_stream_headers      │ │ Hook: on_stream_start        │
│   (Http1Plugin)              │ │   (Http2Plugin)              │ │   (GrpcPlugin)               │
│                              │ │                              │ │                              │
│ Hook: on_request_body        │ │ Hook: on_stream_data         │ │ Hook: on_message_frame       │
│   (WAF Payload Inspection)   │ │   (WAF Payload Inspection)   │ │   (Protobuf / Auth Check)    │
│                              │ │                              │ │                              │
│ Hook: on_response_head       │ │ Hook: on_response_headers    │ │ Hook: on_stream_trailers     │
│   (HSTS, CORS, Security)     │ │   (HSTS, CORS, Security)     │ │   (grpc-status injection)    │
└──────────────────────────────┘ └──────────────────────────────┘ └──────────────────────────────┘
```

---

## 3. Phase-Separated Hook Architecture

To maximize throughput and protect backend infrastructure from DoS/WAF attacks, plugins operate across distinct lifecycle phases:

### Phase 1: Head / Metadata Phase (`on_request_head`)
* **Authority**: Inspects and mutates request metadata (URI, Method, Headers, Query parameters).
* **Use Cases**: Authentication (JWT / API Key), Rate Limiting (IP/Token bucket), Path Rewriting, CORS pre-flight, Header-based WAF rules (User-Agent, SQLi in Query).
* **Fail-Fast Advantage**: If a request is unauthorized or malicious, the plugin returns `Action::Respond(401/403/429)` or `Action::Reject`. The connection is terminated **without allocating memory or reading a single byte of the request body**.

### Phase 2: Payload / Body Phase (`on_request_body`)
* **Authority**: Inspects or transforms the payload body (streamed chunks or contiguous memory).
* **Use Cases**: Deep WAF scanning (SQLi/XSS in JSON/XML payloads), payload decryption, request decompression.
* **Selective Execution**: Only routes configured with body inspection execute this phase. Standard proxy/GET routes bypass this phase with zero overhead.

### Phase 3: Response Phase (`on_response_head` & `on_response_body`)
* **Authority**: Inspects or mutates response status, headers, and outgoing body.
* **Use Cases**: Security headers injection (HSTS, CSP, X-Frame-Options), response compression (Gzip/Brotli), PII data masking.

---

## 4. Constrained Authority Contract (`Action`)

Plugins in Velda Edge do not own socket connections, thread pools, or routing tables. Their decision authority is strictly bounded to the canonical [`Action`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs) contract:

```rust
pub enum Action<TResponse> {
    /// Allow the request to proceed to the next plugin or upstream.
    Continue,

    /// Short-circuit the workflow and return an immediate response to the client.
    /// Example: 401 Unauthorized, 403 Forbidden, 429 Too Many Requests.
    Respond(TResponse),

    /// Immediately reject and tear down the connection.
    /// Example: Malformed framing, security protocol violation.
    Reject(velda_core::Error),
}
```

---

## 5. Conceptual Trait Signatures (Blueprint)

When implemented, plugins will define narrow, type-safe traits without dynamic downcasting:

### HTTP/1.1 Plugin Contract
```rust
pub trait Http1Plugin: Send + Sync {
    /// Invoked immediately after request line and headers are decoded.
    fn on_request_head(&self, head: &mut velda_http1::RequestHead) -> Action<velda_http1::ResponseHead> {
        Action::Continue
    }

    /// Invoked when request payload is ready for inspection.
    fn on_request_body(&self, body: &mut velda_core::Body) -> Action<velda_http1::ResponseHead> {
        Action::Continue
    }

    /// Invoked before response headers are flushed downstream.
    fn on_response_head(&self, head: &mut velda_http1::ResponseHead) -> Action<()> {
        Action::Continue
    }
}
```

### gRPC Plugin Contract
```rust
pub trait GrpcPlugin: Send + Sync {
    /// Invoked on RPC stream initiation (:path = /service/method).
    fn on_stream_start(&self, parts: &http::request::Parts) -> Action<velda_grpc::GrpcStatus> {
        Action::Continue
    }

    /// Invoked on each 5-byte Length-Prefixed Message frame.
    fn on_message(&self, message_frame: &[u8]) -> Action<velda_grpc::GrpcStatus> {
        Action::Continue
    }

    /// Invoked before trailing metadata (grpc-status) is sent.
    fn on_trailers(&self, status: &mut velda_grpc::GrpcStatus) -> Action<()> {
        Action::Continue
    }
}
```

---

## 6. Implementation Roadmap

- [ ] **Stage 1**: Define core plugin lifecycle contracts in `velda-plugin` (`PluginId`, `Action`, execution chain coordinator).
- [ ] **Stage 2**: Implement `Http1PluginChain` in `velda-edge::pipeline::http1`.
- [ ] **Stage 3**: Implement `GrpcPluginChain` in `velda-edge::pipeline::grpc`.
- [ ] **Stage 4**: Compile-time route-to-plugin mapping in `velda-router`.
- [ ] **Stage 5**: Standard built-in plugins:
  - `cors`: Cross-Origin Resource Sharing header management.
  - `rate_limit`: In-memory sliding window token bucket.
  - `jwt_auth`: Fast zero-copy JWT verification.
  - `waf_header`: Rule-based header, URI, and User-Agent scanner.
