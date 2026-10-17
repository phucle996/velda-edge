# velda-plugin

High-performance, protocol-bound, phase-differentiated Plugin Engine for the **Velda Edge Data Plane**.

> [!NOTE]
> For a step-by-step tutorial on authoring, registering, and testing custom plugins, see the [Plugin Authoring Guide](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/AUTHORING_PLUGINS.md).

---

## 1. Architectural Philosophy & Invariants

Legacy API gateways (Kong, APISIX, Spring Cloud Gateway) rely on generic, dynamic plugin models where plugins accept untyped maps (`HashMap<String, Box<dyn Any>>`) or generic request abstractions. In high-performance edge computing, that pattern introduces severe operational risks:

1. **Runtime Type-Casting Panics**: When an HTTP REST plugin is attached to a gRPC or raw TCP pipeline, runtime downcasting panics or corrupts framing.
2. **Leaky Protocol Abstractions**: Attempting to force gRPC through HTTP semantics destroys gRPC status codes (`grpc-status` trailers), binary framing (5-byte Length-Prefixed Messages), and duplex streaming.
3. **Hot-Path Virtual Indirection**: Dynamic JSON parsing, schema validation, and reflection on every request violate the Zero-IO hot-path invariant.

### The Velda Invariants

- **Protocol-Bound Plugins**: Plugins explicitly declare supported protocols via bitflags ([`SupportedProtocols`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/capability.rs#L28)). Mismatches fail fast at sync/compilation time, never in runtime traffic serving.
- **Reverse Permission Granting ([`PluginCapability`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/capability.rs#L68))**: Plugins declare their exact authority level ([`AccessLevel`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/capability.rs#L15)) separately for Headers and Payloads (`None < Read < Write`).
- **Orthogonal Streaming Pipe Decoupling**: Plugins requiring body write/buffering cannot be attached to streaming pipelines (`streaming.client` or `streaming.server`), preventing deadlocks and memory bloat.
- **Hybrid Deterministic Stable Sort**: Execution order within each phase is resolved strictly via:
  $$\text{Phase Isolation} \implies \text{Priority (Descending: } 1000 \to 1) \implies \text{JSON Declaration Index (Ascending)}$$
- **Zero-IO in Serving**: All plugins are instantiated and compiled ahead of time into a static execution plan ([`RoutePluginPlan`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/plan.rs#L30)). Zero allocations, zero JSON parsing, and zero lock contention on the serving hot path.

---

## 2. Phase-Separated Hook Architecture

Plugins execute across four strictly isolated lifecycle phases:

```
[ Ingress Protocol Decoder ]
             │
             ▼
┌─────────────────────────┐
│ Phase 1: Pre-Route      │ ── Evaluated before route matching & body streaming.
│ (PreRoutePlugin)        │    Fail-fast authentication (JWT), IP filtering, WAF header checks.
└────────────┬────────────┘
             │
             ▼
[ Route Trie Matching & Upstream Selection ]
             │
             ▼
┌─────────────────────────┐
│ Phase 2: Pre-Upstream   │ ── Evaluated after route & backend target are resolved.
│ (PreUpstreamPlugin)     │    Rate limiting, token injection, header mutation.
└────────────┬────────────┘
             │
             ▼
[ Upstream Backend Execution ]
             │
             ▼
┌─────────────────────────┐
│ Phase 3: Post-Response  │ ── Evaluated on response before downstream encoding.
│ (PostResponsePlugin)    │    Security headers (HSTS, CSP), CORS headers, compression.
└────────────┬────────────┘
             │
             ▼
[ Egress Protocol Encoder ]

┌─────────────────────────┐
│ Phase 4: Background     │ ── Runs exclusively on auxiliary supervisor cores.
│ (BackgroundPlugin)      │    Prometheus exporter, health check probes. Zero traffic worker impact.
└─────────────────────────┘
```

### Phase Contracts

| Phase | Trait | Primary Hook | Authority Contract |
| :--- | :--- | :--- | :--- |
| **`pre_route`** | [`PreRoutePlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/mod.rs#L23) | `on_request(&self, req: &mut L7Request) -> Action` | [`Action::Continue`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs), [`Action::Respond(401/403)`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs), [`Action::Reject`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs) |
| **`pre_upstream`** | [`PreUpstreamPlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_upstream/mod.rs#L22) | `on_upstream_request(&self, req: &mut L7Request) -> Action` | [`Action::Continue`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs), [`Action::Respond(429)`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs), [`Action::Reject`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs) |
| **`post_response`** | [`PostResponsePlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/post_response/mod.rs#L22) | `on_response(&self, resp: &mut L7Response)` | In-place header & status mutation |
| **`background`** | [`BackgroundPlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/background/mod.rs#L25) | `run(self, shutdown) -> Future<Output = Result<(), PluginError>>` | Auxiliary supervisory execution loop |

---

## 3. Reverse Permission Granting & Streaming Decoupling

Velda Edge plugins must declare their exact access requirements:

```rust
use velda_plugin::capability::{AccessLevel, PluginCapability, SupportedProtocols};

impl PreRoutePlugin for MyCustomAuthPlugin {
    // 1. Declare exact permission boundaries: Read headers only, zero payload access
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    // 2. Declare supported protocols via bitflags
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::HTTP1 | SupportedProtocols::HTTP2 | SupportedProtocols::HTTP3
    }
    
    // ...
}
```

### Orthogonal Pipe Validation Matrix

| Plugin Payload Authority | `streaming.client == false` (Buffered) | `streaming.client == true` (Stream Upload) |
| :--- | :---: | :---: |
| `payload_access: None` | **Compatible** | **Compatible** |
| `payload_access: Read` | **Compatible** | **Compatible** |
| `payload_access: Write` | **Compatible** | **Rejected at Sync ([`PluginError::StreamingConflict`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/error.rs))** |

> [!IMPORTANT]
> A plugin requiring body write/buffering attached to a streaming pipe will be rejected **fail-fast at configuration compile time**. It will never enter the Data Plane hot path.

---

## 4. Phase Sub-Dispatchers & Open-Closed Factory

To prevent [`factory.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/factory.rs) from becoming a monolithic "God Factory" as the number of plugins grows:

1. **Plugin-Owned Deserialization**: Every plugin implements `from_config(&PluginConfig) -> Result<Self, PluginError>` in its own module.
2. **Phase Sub-Dispatchers**: Each phase module (`pre_route/mod.rs`, `pre_upstream/mod.rs`, `post_response/mod.rs`, `background/mod.rs`) defines a local `instantiate(config)` function.
3. **Razor-Thin Factory**: [`factory::instantiate_plugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/factory.rs#L85) only validates `canonical_phase_for_type` and delegates to the appropriate phase sub-dispatcher.

---

## 5. Built-in Production Plugins

| Plugin Name | Phase | Default Protocols | Description |
| :--- | :---: | :---: | :--- |
| [`jwt_auth`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/jwt_auth.rs) | `pre_route` | HTTP/1.1, HTTP/2, HTTP/3 | Bearer token validation, subject extraction into `x-authenticated-user`. |
| [`ip_filter`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/ip_filter.rs) | `pre_route` | All Protocols (L4/L7) | Client IP allowlist / denylist filtering with `X-Forwarded-For` support. |
| [`rate_limit`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_upstream/rate_limit.rs) | `pre_upstream` | All Protocols | In-memory token bucket rate limiting with `Retry-After` header generation. |
| [`header_mutation`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_upstream/header_mutation.rs) | `pre_upstream` | All L7 | Sets or strips HTTP headers before upstream backend forwarding. |
| [`security_headers`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/post_response/security_headers.rs) | `post_response` | All L7 | Injects enterprise security headers (`HSTS`, `X-Frame-Options`, `X-Content-Type-Options`). |
| [`cors`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/post_response/cors.rs) | `post_response` | All L7 | Injects CORS headers (`Access-Control-Allow-Origin`, `Access-Control-Allow-Credentials`). |
| [`prometheus_exporter`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/background/prometheus/mod.rs) | `background` | Management (:9090) | High-performance lock-free OpenMetrics telemetry exporter running on Auxiliary Core. |
| [`health_probe`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/background/health_probe.rs) | `background` | Management (:8081) | Kubernetes liveness and readiness probe HTTP endpoint. |

---

## 6. Verification & Test Suite

Run unit and integration tests across the plugin crate:

```bash
# Check formatting
rtk cargo fmt --check -p velda-plugin

# Strict clippy linting
rtk cargo clippy -p velda-plugin --all-targets --all-features -- -D warnings

# Execute test suite
rtk cargo test -p velda-plugin
```
