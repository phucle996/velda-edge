# Authoring Plugins for Velda Edge

A step-by-step developer guide for designing, implementing, registering, and testing custom plugins in the **Velda Edge Data Plane**.

---

## 1. Architectural Mindset

Before writing code, understand the core constraints of the Velda Edge Data Plane:

1. **Flat Workflows (AGENTS.md §2.2 & §2.3)**:
   - Keep workflows linear and readable top-to-bottom.
   - Do not chop cohesive logic into artificial micro-helpers or broad trait hierarchies.
2. **Hot-Path Zero-IO (AGENTS.md §2.4)**:
   - Plugins execute on the request serving hot path.
   - **NEVER** parse JSON, perform disk I/O, make network/RPC calls, or allocate unbounded heap memory in `on_request`, `on_upstream_request`, or `on_response`.
   - All configuration parsing occurs **once** at sync time via `from_config`.
3. **Module Ownership (AGENTS.md §2.2)**:
   - Each plugin owns its own configuration schema, validation, and defaults inside its own module file.
   - Central factories only dispatch; they do not parse plugin-specific configurations.

---

## 2. Step 1: Select the Execution Phase

Choose the earliest phase that satisfies your plugin's requirements:

| Phase | Trait | Location | When to Use |
| :--- | :--- | :--- | :--- |
| **`pre_route`** | [`PreRoutePlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/mod.rs#L23) | `src/pre_route/` | Authentication, IP firewalls, WAF header checks, DDoS filtering. Fails fast **before** route trie matching and body reading. |
| **`pre_upstream`** | [`PreUpstreamPlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_upstream/mod.rs#L22) | `src/pre_upstream/` | Request header mutations, backend authentication token injection, local token bucket rate limiting. Runs **after** the target upstream pool/endpoint is chosen. |
| **`post_response`** | [`PostResponsePlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/post_response/mod.rs#L22) | `src/post_response/` | Security response headers (HSTS, CSP), CORS response headers, response header scrubbing. Mutates [`L7Response`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/l7/response.rs) in place. |
| **`background`** | [`BackgroundPlugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/background/mod.rs#L25) | `src/background/` | Long-lived auxiliary services (telemetry exporters, health probes). Runs strictly on dedicated Auxiliary Cores. |

---

## 3. Step 2: Implement the Plugin Struct

Create a new file in the appropriate directory (e.g., `crates/velda-plugin/src/pre_route/api_key.rs`).

Define a plain struct holding the compiled, read-only configuration in RAM:

```rust
use std::collections::HashSet;
use http::header::HeaderName;
use http::StatusCode;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use super::PreRoutePlugin;

/// Example API Key Authentication Plugin.
#[derive(Debug, Clone)]
pub struct ApiKeyAuthPlugin {
    pub id: String,
    pub header_name: HeaderName,
    pub valid_keys: HashSet<String>,
}

impl ApiKeyAuthPlugin {
    /// Constructs a new instance directly from parameters.
    pub fn new(
        id: impl Into<String>,
        header_name: HeaderName,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            id: id.into(),
            header_name,
            valid_keys: keys.into_iter().map(|k| k.into()).collect(),
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    ///
    /// # Module Ownership Invariant
    /// Configuration field extraction, defaults, and sanitization belong strictly here.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let header_str = config
            .config
            .get("header_name")
            .and_then(|v| v.as_str())
            .unwrap_or("x-api-key");

        let header_name = HeaderName::from_bytes(header_str.as_bytes()).map_err(|e| {
            PluginError::Config(format!(
                "Invalid header_name '{header_str}' for plugin '{}': {e}",
                config.id
            ))
        })?;

        let mut keys = HashSet::new();
        if let Some(arr) = config.config.get("keys").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(key_str) = item.as_str() {
                    keys.insert(key_str.trim().to_string());
                }
            }
        }

        if keys.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{}' must specify at least one valid key in 'keys'",
                config.id
            )));
        }

        Ok(Self::new(&config.id, header_name, keys))
    }
}
```

---

## 4. Step 3: Implement the Phase Trait

Implement the corresponding phase trait on your struct:

```rust
impl PreRoutePlugin for ApiKeyAuthPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        let key_opt = req
            .headers
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok());

        match key_opt {
            Some(key) if self.valid_keys.contains(key) => {
                // Key matches: continue request processing
                Action::Continue
            }
            Some(_) => {
                // Invalid key: short-circuit with 401 Unauthorized
                Action::Respond(L7Response::from_bytes(
                    StatusCode::UNAUTHORIZED,
                    "{\"error\":\"Invalid API Key\"}",
                ))
            }
            None => {
                // Missing key: short-circuit with 401 Unauthorized
                Action::Respond(L7Response::from_bytes(
                    StatusCode::UNAUTHORIZED,
                    "{\"error\":\"Missing API Key header\"}",
                ))
            }
        }
    }

    /// Declares the reverse-permission capabilities required by this plugin.
    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        // We only read request headers; zero payload access required.
        PluginCapability::header_only(AccessLevel::Read)
    }

    /// Hardcoded network/application protocols supported by this plugin.
    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        // Compatible with HTTP/1.1, HTTP/2, and HTTP/3
        SupportedProtocols::ALL_HTTP
    }
}
```

### Understanding Authority Contracts ([`Action`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/lifecycle.rs))

* **`Action::Continue`**: Allows the pipeline to proceed to the next plugin or route matching.
* **`Action::Respond(response)`**: Short-circuits immediately. The request body is not read, upstream connections are not leased, and `response` is serialized directly downstream.
* **`Action::Reject(error)`**: Drops and tears down the connection immediately without sending an HTTP response (for severe protocol violations or security attacks).

---

## 5. Step 4: Register in the Phase Sub-Dispatcher

### 1. Export in Phase Module
Open the `mod.rs` of your selected phase (e.g., [`crates/velda-plugin/src/pre_route/mod.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/mod.rs)):

```rust
pub mod api_key;
pub use api_key::ApiKeyAuthPlugin;

pub fn instantiate(
    config: &velda_sync::post_sync::plugin::PluginConfig,
) -> Result<Arc<dyn PreRoutePlugin>, crate::error::PluginError> {
    match config.id.as_str() {
        "jwt_auth" => Ok(Arc::new(JwtAuthPlugin::from_config(config)?)),
        "ip_filter" => Ok(Arc::new(IpFilterPlugin::from_config(config)?)),
        // Add your plugin here:
        "api_key" => Ok(Arc::new(ApiKeyAuthPlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported pre_route plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}
```

### 2. Register Canonical Phase in velda-sync
Open [`crates/velda-sync/src/post_sync/plugin/mod.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-sync/src/post_sync/plugin/mod.rs) and add your id string to [`canonical_phase_for_id`](file:///home/phucle/Desktop/velda-edge/crates/velda-sync/src/post_sync/plugin/mod.rs):

```rust
pub fn canonical_phase_for_id(id: &str) -> Option<&'static str> {
    match id {
        "jwt_auth" | "ip_filter" | "resource_limit" | "waf" | "api_key" => Some("pre_route"),
        "rate_limit" | "header_mutation" => Some("pre_upstream"),
        "security_headers" | "cors" | "access_log" => Some("post_response"),
        "health_probe" | "prometheus_exporter" => Some("background"),
        _ => None,
    }
}
```

> [!TIP]
> Notice that you **do not** touch [`instantiate_plugin`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/factory.rs#L85) or [`build_route_plugin_plan`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/factory.rs#L120). The factory is open for extension but closed for modification.

---

## 6. Step 5: Write Unit & Integration Tests

Always include tests covering:
1. Valid request handling (`Action::Continue`).
2. Rejection handling (`Action::Respond`).
3. Configuration deserialization (`from_config`).

Example test block in `api_key.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use serde_json::json;

    #[test]
    fn test_api_key_auth_flow() {
        let plugin = ApiKeyAuthPlugin::new(
            "auth-1",
            HeaderName::from_static("x-api-key"),
            vec!["secret-key-123"],
        );

        // 1. Missing header -> 401
        let mut req = L7Request::new(Method::GET, "/api/data");
        match plugin.on_request(&mut req) {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::UNAUTHORIZED),
            _ => panic!("Expected 401 response"),
        }

        // 2. Invalid header -> 401
        req.headers.insert("x-api-key", "wrong-key".parse().unwrap());
        match plugin.on_request(&mut req) {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::UNAUTHORIZED),
            _ => panic!("Expected 401 response"),
        }

        // 3. Valid key -> Continue
        req.headers.insert("x-api-key", "secret-key-123".parse().unwrap());
        assert_eq!(plugin.on_request(&mut req), Action::Continue);
    }

    #[test]
    fn test_api_key_from_config() {
        let config = PluginConfig {
            id: "api_key".into(),
            phase: "pre_route".into(),
            priority: Some(500),
            config: json!({
                "header_name": "x-custom-key",
                "keys": ["key-alpha", "key-beta"]
            }),
        };

        let plugin = ApiKeyAuthPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "api_key");
        assert_eq!(plugin.header_name, HeaderName::from_static("x-custom-key"));
        assert!(plugin.valid_keys.contains("key-alpha"));
    }
}
```

---

## 7. Step 6: Verification Pipeline

Before submitting your code, verify the full quality suite:

```bash
# 1. Format check
rtk cargo fmt --check

# 2. Strict clippy linting (zero warnings permitted)
rtk cargo clippy --all-targets --all-features -- -D warnings

# 3. Unit test verification
rtk cargo test -p velda-plugin

# 4. Monorepo workspace test suite
rtk cargo test --workspace
```
