# Authoring Plugins for Velda Edge

A step-by-step developer guide for designing, scheduling (**PBWW**), registering, and verifying plugins in the **Velda Edge Data Plane**.

---

## 1. Engineering Mindset & Non-Negotiables

Before writing code, review the core invariants in `AGENTS.md` and `.agent/rules/code-architecture-rules.md`:

1. **Flat Workflows (`AGENTS.md §2.2 & §2.3`)**:
   - Keep request evaluation linear and readable top-to-bottom within a single cohesive function.
   - Include an ASCII mechanism diagram in the top-level module doc (`//!`) of every core implementation file.
2. **Zero-IO Hot Path (`AGENTS.md §2.4`)**:
   - **NEVER** parse JSON, perform disk I/O, acquire coarse locks across threads, allocate unbounded heap memory, or open network connections in `on_request`, `on_upstream_request`, or `on_response`.
   - All JSON parsing, validation, regex/CIDR compilation, and arena allocation happen **once** inside `from_config`.
3. **Never Spawn Detached OS Threads or Tasks**:
   - **NEVER** call `std::thread::spawn` or `tokio::spawn` inside `from_config`.
   - Persistent background loops (Redis sync, exporters, key rotators) **MUST** be scheduled through the unified **PBWW** scheduler (`pbww()` + `run_background()`) so Velda places them on `SCHED_IDLE` Shadow Workers and shuts them down cleanly on hot-reload.
4. **Module Ownership**:
   - Each plugin owns its `config.rs`, `validation.rs`, and `plugin.rs`. Central dispatchers never inspect plugin-specific JSON fields.

---

## 2. Step 1: Choose Traffic Phase & Background Scheduling Mode

First, pick the earliest **Traffic Phase** (where it intercepts live traffic):

| Phase | Trait | Directory | Primary Purpose |
| :--- | :--- | :--- | :--- |
| **`pre_route`** | [`PreRoutePlugin`](src/pre_route/mod.rs) | `src/pre_route/` | Auth, IP filtering, WAF checks, URI/header bounds. Runs **before** router lookup. |
| **`pre_upstream`** | [`PreUpstreamPlugin`](src/pre_upstream/mod.rs) | `src/pre_upstream/` | Header injection/mutation, rate limiting, canary rerouting. Runs **after** endpoint selection. |
| **`post_response`** | [`PostResponsePlugin`](src/post_response/mod.rs) | `src/post_response/` | Response token metering, security headers, CORS, compression. Mutates [`L7Response`](../velda-core/src/l7/response.rs). |
| **`background`** | [`BackgroundPlugin`](src/background/mod.rs) | `src/background/` | Standalone daemons with no traffic hook (telemetry exporters, ACME/JWKS rotators, probes). |

Second, determine your plugin's **Shadow Logistics Model**:

- **Hot-Path Only (`pbww() == 0`)**: Does zero work off the request path. Simply return `0` from `pbww()`.
- **Hybrid Hot-Path + Background Daemon (`pbww() ∈ 1..=100`)**: Hot path updates lock-free atomics/arenas on RAM; a persistent loop syncs with Redis or external systems in the background. Return `0` if background sync is disabled in config (e.g. `local` mode), or `1..=100` when active, and implement `run_background()`.
- **Per-Request One-Shot Offload (`pbww() == 0` + [`try_dispatch_shadow`](src/hook.rs))**: For sporadic tasks triggered by an individual request (e.g., `request_mirror`, stale token refresh in `oauth2_introspection`), dispatch a one-shot `ShadowTask` to the core-local paired Shadow Worker via `crate::hook::try_dispatch_shadow(task)`.

---

## 3. Step 2: Implement Struct & `from_config`

Create a module folder under your chosen phase (e.g., `src/pre_route/api_key/plugin.rs`). Keep `from_config` strictly CPU-only (zero socket creation):

```rust
use std::collections::HashSet;
use http::header::HeaderName;
use velda_core::{Action, L7Request, RejectReason};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Example API Key Authentication Plugin.
#[derive(Debug, Clone)]
pub struct ApiKeyPlugin {
    pub id: String,
    pub header_name: HeaderName,
    pub valid_keys: HashSet<String>,
}

impl ApiKeyPlugin {
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

        let mut valid_keys = HashSet::new();
        if let Some(arr) = config.config.get("keys").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str() {
                    let trimmed = s.trim();
                    if !trimmed.is_empty() {
                        valid_keys.insert(trimmed.to_string());
                    }
                }
            }
        }

        if valid_keys.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{}' must specify at least one valid key in 'keys'",
                config.id
            )));
        }

        Ok(Self {
            id: config.id.clone(),
            header_name,
            valid_keys,
        })
    }
}
```

---

## 4. Step 3: Implement the Phase Trait & Declare `pbww()`

### Option A: Pure Hot-Path Plugin (`pbww() == 0`)

Notice `pbww(&self) -> u32` has **no default implementation** — the compiler requires every plugin to explicitly declare its background weight:

```rust
impl PreRoutePlugin for ApiKeyPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        let Some(key_val) = req.headers.get(&self.header_name) else {
            return Action::Reject(RejectReason::Unauthorized);
        };
        let Ok(key_str) = key_val.to_str() else {
            return Action::Reject(RejectReason::Unauthorized);
        };

        if self.valid_keys.contains(key_str) {
            Action::Continue
        } else {
            Action::Reject(RejectReason::Unauthorized)
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}
```

### Option B: Hybrid Plugin with Background Sync Loop (`pbww() > 0`)

If a traffic-phase plugin also maintains a persistent background loop (e.g., Redis lease synchronization):

1. Return `0` when the background feature is disabled in config, or `1..=100` (e.g. `self.shadow_weight`) when enabled.
2. Override `run_background(self: Arc<Self>, shutdown: watch::Receiver<bool>)`.
3. Open outbound sockets (TCP/TLS/Redis) **lazily inside the returned future** — they will execute directly on whichever `velda-shadow-{i}` core the Greedy LPT scheduler assigns to this plugin.
4. Select cleanly on `shutdown.changed()` so hot-reloads tear down old connections immediately.

```rust
impl PostResponsePlugin for MyDistributedCounterPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        if self.distributed_enabled {
            self.shadow_weight.clamp(1, 100)
        } else {
            0
        }
    }

    fn run_background(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<crate::BackgroundTaskFuture> {
        if !self.distributed_enabled {
            return None;
        }
        Some(Box::pin(async move {
            // Open Redis / remote client HERE on the assigned Shadow Worker runtime.
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Sync lock-free shared state deltas
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            Ok(())
        }))
    }

    // on_response(), capability(), supported_protocols() ...
}
```

### Understanding `Action` Contracts (`velda-core`)

- **`Action::Continue`**: Pass to the next plugin or stage.
- **`Action::Respond(L7Response)`**: Short-circuit immediately with a custom [`L7Response`](../velda-core/src/l7/response.rs).
- **`Action::Reject(RejectReason)`**: Protocol-agnostic rejection (`Unauthorized`, `Forbidden`, `TooManyRequests`, etc.). The ingress protocol engine renders the native HTTP or gRPC error response.
  - All Bit-0 headers in `req.headers` are stripped on rejection.
  - Use `req.headers.insert_bypass(name, val)` / `insert_bypass_u64(...)` for client-facing rejection headers (`Retry-After`, `WWW-Authenticate`).
- **`Action::Delay { duration, and_then }`**: Non-blocking async delay before applying `and_then`.

---

## 5. Step 4: Register in Your Stage Sub-Dispatcher

Each plugin's execution stage (`pre_route`, `pre_upstream`, `post_response`, or `background`) is fixed at compile time by the module and trait it implements.

1. **Register in your Stage Sub-Dispatcher** (e.g., [`crates/velda-plugin/src/pre_route/mod.rs`](src/pre_route/mod.rs)):
   ```rust
   pub mod api_key;
   pub use api_key::ApiKeyPlugin;

   // 1. Add to owns_id(id):
   pub fn owns_id(id: &str) -> bool {
       matches!(id, "ip_filter" | "waf_lite" | "api_key" /* ... */)
   }

   // 2. Inside instantiate(config):
   "api_key" => Ok(Arc::new(ApiKeyPlugin::from_config(config)?)),
   ```
2. *(Optional)* If your plugin has a standalone schema validator (`validate_*_config`), wire its match arm inside [`crates/velda-plugin/src/validation.rs`](src/validation.rs).

   You do **not** need to modify [`factory.rs`](src/factory.rs) or [`velda-edge/src/runtime/plugin.rs`](../velda-edge/src/runtime/plugin.rs) — [`LoadedPlugin`](src/factory.rs) automatically discovers `owns_id()`, `pbww()`, and `run_background()` across all stages.

---

## 6. Step 5: Write Unit & Integration Tests

Every plugin should include tests covering:
1. Valid traffic execution (`Action::Continue` / response header mutations).
2. Rejection / bypass header behavior (`Action::Reject`).
3. Invalid configuration rejection (`from_config` / validator errors).
4. If `pbww() > 0`: verify `pbww()` returns `0` when disabled and `1..=100` when enabled, and verify `run_background(shutdown)` exits cleanly when `shutdown_tx.send(true)` fires.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method};
    use serde_json::json;

    #[test]
    fn test_api_key_flow() {
        let cfg = PluginConfig {
            id: "api_key".into(),
            config: json!({
                "header_name": "x-api-key",
                "keys": ["secret-key-123"]
            }),
        };

        let plugin = ApiKeyPlugin::from_config(&cfg).expect("valid config");
        assert_eq!(plugin.pbww(), 0);

        let mut req = L7Request::new(Method::GET, "/api/data");
        assert_eq!(plugin.on_request(&mut req), Action::Reject(RejectReason::Unauthorized));

        req.headers.insert("x-api-key", HeaderValue::from_static("secret-key-123"));
        assert_eq!(plugin.on_request(&mut req), Action::Continue);
    }
}
```

---

## 7. Step 6: Verification Commands

```bash
# 1. Check formatting
cargo fmt --check

# 2. Strict linting across plugin & edge crates (zero warnings)
cargo clippy -p velda-plugin -p velda-edge -p velda-sync --all-targets -- -D warnings

# 3. Run velda-plugin unit & integration tests
cargo test -p velda-plugin

# 4. Run velda-edge scheduler & runtime tests
cargo test -p velda-edge --lib
```
