# velda-core Architecture Specification

This document details the architectural design, memory models, data structures, and invariants governing `velda-core`, the foundational vocabulary crate of the **Velda Edge Data Plane**.

---

## 1. System Placement & The Hourglass Model

Within the Velda Edge Data Plane, `velda-core` occupies the absolute bottom of the crate dependency graph. It acts as the shared, authoritative lingua franca connecting the three tiers of the **Hourglass Architecture**:

```text
       ┌────────────────────────────────────────────────────────┐
       │             TOP CONE: INGRESS PROTOCOLS                │
       │       [ velda-http1 | velda-http2 | velda-http3 ]      │
       │       [        velda-grpc | velda-transport     ]      │
       └───────────────────────────┬────────────────────────────┘
                                   │
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │             THE NARROW WAIST: VOCABULARY               │
       │                      velda-core                        │
       │    - Flat Request/Response: L7Request, L7Response      │
       │    - Virtual 3-Column Header Workspace                 │
       │    - Bounded Authority: Action, RejectReason           │
       │    - Physical Destination: Endpoint                    │
       │    - Pre-Warmed Pools: HeaderPool, PayloadSlabPool     │
       └───────────────────────────┬────────────────────────────┘
                                   │
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │             BOTTOM CONE: UPSTREAM & PIPES              │
       │       [  velda-upstream | velda-connection-pool ]      │
       │       [          velda-lb | velda-plugin        ]      │
       └────────────────────────────────────────────────────────┘
```

### Core Invariants
1. **Zero Processing Logic**: `velda-core` strictly defines *what* data and contracts are; it *never* executes network I/O, route matching, load balancing, or plugin hooks.
2. **Zero Protocol Status Leakage**: `velda-core` contains zero HTTP status code numbers (e.g. `403`, `429`) and zero gRPC integer trailers. All error decisions are expressed through semantic vocabulary ([`RejectReason`]).
3. **Flat, Non-Polymorphic Entities**: Avoids deep class hierarchies or dynamic bags (`HashMap<String, Box<dyn Any>>`). State is held in plain, cacheline-aligned structs with explicit ownership.

---

## 2. Virtual 3-Column Header Workspace Architecture

### 2.1 The Vulnerability: Information Leakage on Short-Circuit Errors
In traditional proxies (Envoy, Kong, Nginx), when a security plugin (WAF, IP filter, rate limiter) rejects a request early, standard error handlers often construct responses using or reflecting the incoming request's header map. This introduces severe attack vectors:
- **Header Reflection Vulnerabilities**: Internal headers (e.g. `X-Internal-Secret`, routing tags, tenant IDs) injected by earlier edge filters get reflected back to the unauthenticated caller.
- **Credential Leakage**: Sensitive authorization headers (`Authorization: Bearer <token>`, session cookies) can be echoed in debug or default error frames.

### 2.2 The Solution: Virtual 3-Column Visibility Bit Tagging
`velda-core` solves this by introducing **Virtual 3-Column Header Visibility** inside [`HeaderWorkspace`](src/buffer/header.rs):

$$\text{Header Entry} = (\text{Name}, \text{Value}, \text{VisibilityBit})$$

```text
  ┌───────────────────────┬───────────────────────────────┬─────────────────────────┐
  │ Column 1: HeaderName  │ Column 2: HeaderValue         │ Column 3: VisibilityBit │
  ├───────────────────────┼───────────────────────────────┼─────────────────────────┤
  │ x-request-id          │ 550e8400-e29b-41d4-a716...    │ Bit 1 (Bypass on Error) │ <-- Slot 0 Pre-loaded
  │ authorization         │ Bearer secret_jwt_token       │ Bit 0 (Internal Only)   │ <-- Dropped on Reject
  │ x-internal-routing    │ cluster-zone-west-1           │ Bit 0 (Internal Only)   │ <-- Dropped on Reject
  │ cookie                │ session_id=abc123xyz          │ Bit 0 (Internal Only)   │ <-- Dropped on Reject
  │ retry-after           │ 60                            │ Bit 1 (Bypass on Error) │ <-- Tagged by Plugin
  │ x-ratelimit-remaining │ 0                             │ Bit 1 (Bypass on Error) │ <-- Tagged by Plugin
  └───────────────────────┴───────────────────────────────┴─────────────────────────┘
```

- **Bit 0 (Default — Normal / Upstream Only)**:
  - Standard incoming client headers and internal gateway mutations default to Bit 0.
  - On standard upstream forwarding (**99% hot path**), all headers (Bit 0 and Bit 1) are forwarded directly with zero overhead ($0$ ns).
  - On early short-circuit rejection (**1% error path**), **100% of Bit 0 headers are stripped and dropped**.
- **Bit 1 (Bypass on Reject)**:
  - Headers explicitly authorized to escape short-circuit error responses back to the downstream client (e.g., `Retry-After`, `x-ratelimit-*`, `WWW-Authenticate`, `x-request-id`).
  - Added explicitly by plugins via `insert_bypass(name, value)`.

---

## 3. Pre-Loaded Slot 0 Invariant (`x-request-id`)

### 3.1 Architectural Requirement
In Velda Edge, **no valid L7 transaction ever traverses the Data Plane without an authoritative Request ID**. Distributed tracing and client support require that 100% of responses—especially error rejections—carry the trace correlation ID.

### 3.2 Mechanism: Constructor-Level Pre-loading
Instead of checking or sniffing `x-request-id` dynamically during every `insert()` call, `HeaderWorkspace` enforces the following invariant:

> **Slot 0 Invariant**: Every `HeaderWorkspace` constructor (`new`, `with_request_id`, `from_headers`) and pool reset routine (`reset`, `take`) unconditionally pre-loads **Slot 0** with `(x-request-id, <id>)` as a Bit 1 Bypass header (`bypass_len = 1`).

```rust
// Internal layout inside HeaderWorkspace::new():
map.insert(X_REQUEST_ID, req_id);
let mut bypass_keys = [const { None }; MAX_BYPASS_HEADERS];
bypass_keys[0] = Some(X_REQUEST_ID);
Self {
    headers: map,
    bypass_keys,
    bypass_len: 1, // Slot 0 is permanently committed to X_REQUEST_ID
    initial_capacity: capacity,
}
```

### 3.3 Zero-Sniffing Hot Path ($0$ ns Overhead)
Because `x-request-id` is pre-committed in Slot 0 upon workspace allocation, the hot-path `insert()` function requires **zero dynamic string checks, zero hash lookups, and zero branching**:

```rust
#[inline]
pub fn insert<K: IntoHeaderName>(&mut self, name: K, value: HeaderValue) -> Option<HeaderValue> {
    // Pure zero-cost delegation directly to http::HeaderMap
    self.headers.insert(name, value)
}
```

Normal headers inserted by routing, WAF, or upstream mutation run at raw hardware speed with native `HeaderMap` memory efficiency.

---

## 4. Cacheline-Aligned Stack Sidecar Design

### 4.1 Why Not Wrap `HeaderValue`?
An intuitive way to implement a 3-column table is wrapping the value:
```rust
// ANTI-PATTERN: Breaks zero-copy and ABI compatibility
enum TaggedValue {
    Internal(HeaderValue), // Bit 0
    Bypass(HeaderValue),   // Bit 1
}
```
**Why this is strictly forbidden in Velda Edge**:
1. Wrapping `HeaderValue` would prevent `HeaderWorkspace` from dereferencing to standard `http::HeaderMap`.
2. Egress codecs (`velda-http1`, `velda-http2`, `velda-http3`, `hyper`, `h2`) require native `HeaderMap` slices for zero-copy vector I/O (`writev`). Wrapping values would force copying and allocating thousands of `HeaderMap` objects per second.

### 4.2 The Stack Sidecar Solution
`HeaderWorkspace` retains native `http::HeaderMap` for storage and tracks Bit 1 keys in a fixed, stack-allocated sidecar array:

```rust
pub const MAX_BYPASS_HEADERS: usize = 8;

pub struct HeaderWorkspace {
    headers: HeaderMap,
    bypass_keys: [Option<HeaderName>; MAX_BYPASS_HEADERS],
    bypass_len: u8,
    initial_capacity: usize,
}
```

### 4.3 Cacheline Efficiency & Overhead Analysis
- **Size**: `[Option<HeaderName>; 8]` takes exactly **128 bytes** (2 contiguous 64-byte CPU cachelines).
- **Zero Allocations**: Zero heap allocations, zero dynamic vector resizing.
- **L1 Cache Performance**: Iterating over at most 8 contiguous pointers in L1 cache takes **$< 1$ nanosecond**, vastly faster than querying a heap-allocated hash set (~15–25ns + pointer dereference + L3 cache miss).
- **Slot Capacity Allocation**:
  - **Slot 0**: `x-request-id` (Permanent invariant).
  - **Slots 1..=4**: Policy rate limit headers (`Retry-After`, `x-ratelimit-reset`, `x-ratelimit-limit`, `x-ratelimit-remaining`).
  - **Slots 5..=7**: Reserved for authentication challenges (`WWW-Authenticate`) or W3C baggage.

---

## 5. Bounded Execution Authority (`Action`)

Plugins and lifecycle hooks in Velda Edge are strictly bounded state machines. They do not hold arbitrary handles to sockets, memory allocators, or connection pools.

```rust
pub enum Action {
    /// Proceed to the next hook or pipeline phase.
    Continue,

    /// Short-circuit pipeline and return an explicit custom response.
    /// Used ONLY when the plugin provides explicit body content, custom status codes
    /// (e.g. CORS 204 No Content), or cached payloads.
    Respond(L7Response),

    /// Canonical semantic rejection.
    /// Emits zero-alloc protocol-native error framing via the downstream driver.
    Reject(RejectReason),

    /// Non-blocking asynchronous delay injection before evaluating `and_then`.
    Delay {
        duration: Duration,
        and_then: Box<Action>,
    },
}
```

### Why `Action::Delay` is Boxed
`Action` is passed by value across hook boundaries. Boxing the recursive `and_then` action keeps `std::mem::size_of::<Action>()` compact (small enough to fit inside a single CPU register pair / cacheline), preventing stack bloat during normal non-delayed request execution.

---

## 6. Canonical Rejection Taxonomy (`RejectReason`)

### 6.1 Why Semantic Symbols Instead of Numbers
Legacy proxies embed HTTP status codes (`400`, `401`, `403`, `429`, `503`) throughout internal plugins. That pattern introduces critical design flaws:
1. **gRPC Protocol Violation**: gRPC requires canonical status codes in `grpc-status` HTTP/2 trailers (`UNAUTHENTICATED`, `RESOURCE_EXHAUSTED`). Forcing an HTTP 401 response with a text body breaks gRPC client deserialization.
2. **Future Transport Incompatibility**: Transports like MQTT, raw L4 TCP, or binary RPC do not possess HTTP status numbers.
3. **Information Smuggling**: Hardcoded numeric statuses often bypass downstream security header injection and trace correlation.

### 6.2 The Protocol-Agnostic Contract
`RejectReason` provides a closed enum of 17 semantic failure conditions:
- **Client Errors**: `BadRequest`, `Unauthorized`, `Forbidden`, `NotFound`, `MethodNotAllowed`, `RequestTimeout`, `Conflict`, `PayloadTooLarge`, `UriTooLong`, `UnsupportedMediaType`, `TooManyRequests`, `HeaderFieldsTooLarge`, `ClientClosedRequest`.
- **Upstream / Server Errors**: `InternalError`, `BadGateway`, `ServiceUnavailable`, `GatewayTimeout`.

### 6.3 Downstream Protocol Synthesis Matrix
When `Action::Reject(reason)` is returned, the downstream engine synthesizes its native error format:

| RejectReason | HTTP/1.1 (`velda-http1`) | HTTP/2 (`velda-http2`) | HTTP/3 (`velda-http3`) | gRPC (`velda-grpc`) |
| :--- | :--- | :--- | :--- | :--- |
| `BadRequest` | `400 Bad Request` | `:status 400` Zero-Body | `:status 400` UDP Datagram | `grpc-status: 3 (INVALID_ARGUMENT)` |
| `Unauthorized` | `401 Unauthorized` | `:status 401` Zero-Body | `:status 401` UDP Datagram | `grpc-status: 16 (UNAUTHENTICATED)` |
| `Forbidden` | `403 Forbidden` | `:status 403` Zero-Body | `:status 403` UDP Datagram | `grpc-status: 7 (PERMISSION_DENIED)` |
| `NotFound` | `404 Not Found` | `:status 404` Zero-Body | `:status 404` UDP Datagram | `grpc-status: 5 (NOT_FOUND)` |
| `PayloadTooLarge` | `413 Payload Too Large` | `:status 413` Zero-Body | `:status 413` UDP Datagram | `grpc-status: 9 (FAILED_PRECONDITION)` |
| `TooManyRequests` | `429 Too Many Requests` | `:status 429` Zero-Body | `:status 429` UDP Datagram | `grpc-status: 8 (RESOURCE_EXHAUSTED)` |
| `ServiceUnavailable` | `503 Service Unavailable` | `:status 503` Zero-Body | `:status 503` UDP Datagram | `grpc-status: 14 (UNAVAILABLE)` |
| `GatewayTimeout` | `504 Gateway Timeout` | `:status 504` Zero-Body | `:status 504` UDP Datagram | `grpc-status: 4 (DEADLINE_EXCEEDED)` |

In all cases, only Bit 1 bypass headers (including `x-request-id`) are merged into the rejection frame.

---

## 7. Memory Safety & RAII Lifecycle Guarantees

[`HeaderWorkspaceLease`](src/buffer/header.rs) provides compile-time defense against classic systems programming memory bugs:

1. **Zero Use-After-Free via Compiler Borrow Checker**:
   `Deref` and `DerefMut` produce references `&'a HeaderWorkspace` strictly tied to the lease's lifetime. The moment the lease drops, the compiler statically invalidates all references.
2. **Zero Double-Free via `Option::take()`**:
   Inside `Drop`, `self.workspace.take()` extracts the workspace. Even under panic stack unwinding, it can only be returned to the pool once.
3. **Async Task Cancellation Immunity**:
   When downstream clients abort or Tokio drops a request future mid-await, Rust's stack unwinding invokes `Drop`, safely returning the workspace to the pool without leaks or corrupted state.
