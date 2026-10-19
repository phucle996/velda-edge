//! Auxiliary Health & Readiness Probe Background Plugin (`health_probe`).
//!
//! Exposes cold-path Kubernetes / Load Balancer liveness (`/healthz`) and readiness (`/readyz`)
//! endpoints on a dedicated auxiliary port, completely isolated from Data Plane traffic worker loops.
//!
//! # Strict Architectural Invariants
//! - **Auxiliary Port Isolation**: Runs on an auxiliary port strictly separated from Data Plane listeners.
//! - **Zero Payload**: Returns empty bodies (`Content-Length: 0`) using immutable static byte
//!   constants (`&'static [u8]`) embedded directly in `.rodata`. Zero allocations, zero heap format strings.
//! - **Lock-Free State Query**: Health queries inspect lock-free atomic states (`AtomicBool`), never
//!   contending with Data Plane worker threads.
//! - **Graceful Draining Semantics**: When shutdown is signaled (e.g. SIGTERM), readiness immediately flips
//!   to `503 Service Unavailable`, allowing Kubernetes / upstream load balancers to route traffic away
//!   before active connections are drained.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::background::BackgroundPlugin;
use crate::error::PluginError;

// ============================================================================
// Zero-Allocation Static HTTP/1.1 Responses (Zero-Byte Body)
// ============================================================================

const RESP_200: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const RESP_503: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const RESP_404: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// Shared lock-free health and readiness state of the Velda Edge Data Plane.
#[derive(Debug, Clone)]
pub struct HealthState {
    is_live: Arc<AtomicBool>,
    is_ready: Arc<AtomicBool>,
    is_draining: Arc<AtomicBool>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Creates a new health state with liveness enabled and readiness pending.
    pub fn new() -> Self {
        Self {
            is_live: Arc::new(AtomicBool::new(true)),
            is_ready: Arc::new(AtomicBool::new(false)),
            is_draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Creates a health state with both liveness and readiness initialized to `true`.
    pub fn live_and_ready() -> Self {
        let state = Self::new();
        state.set_ready(true);
        state
    }

    /// Marks the Data Plane readiness state.
    #[inline]
    pub fn set_ready(&self, ready: bool) {
        self.is_ready.store(ready, Ordering::Release);
    }

    /// Marks the Data Plane liveness state.
    #[inline]
    pub fn set_live(&self, live: bool) {
        self.is_live.store(live, Ordering::Release);
    }

    /// Marks whether the Data Plane has entered the graceful draining phase.
    #[inline]
    pub fn set_draining(&self, draining: bool) {
        self.is_draining.store(draining, Ordering::Release);
    }

    /// Returns `true` if the process is live (event loop responsive, not deadlocked).
    #[inline]
    pub fn is_live(&self) -> bool {
        self.is_live.load(Ordering::Acquire)
    }

    /// Returns `true` if the Data Plane is ready to accept incoming user traffic.
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire)
    }

    /// Returns `true` if the process has received a shutdown signal and is actively draining connections.
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.is_draining.load(Ordering::Acquire)
    }
}

/// Configuration settings for the health probe background plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthProbeConfig {
    /// Auxiliary TCP socket address to bind and listen on (must NOT collide with traffic listeners).
    pub listen_addr: SocketAddr,
    /// Path for the Kubernetes / Orchestrator liveness probe.
    pub liveness_path: String,
    /// Path for the Kubernetes / Orchestrator readiness probe.
    pub readiness_path: String,
}

impl Default for HealthProbeConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 9091)),
            liveness_path: "/healthz".into(),
            readiness_path: "/readyz".into(),
        }
    }
}

/// Auxiliary Health and Readiness Probe Background Service.
#[derive(Debug, Clone)]
pub struct HealthProbePlugin {
    pub id: String,
    pub config: HealthProbeConfig,
    pub state: HealthState,
}

impl HealthProbePlugin {
    /// Constructs a new health probe service with default paths and a specific listen address.
    pub fn new(id: impl Into<String>, listen_addr: SocketAddr) -> Self {
        Self::with_config(
            id,
            HealthProbeConfig {
                listen_addr,
                ..HealthProbeConfig::default()
            },
        )
    }

    /// Constructs a new health probe service with full configuration and a fresh default state.
    pub fn with_config(id: impl Into<String>, config: HealthProbeConfig) -> Self {
        Self {
            id: id.into(),
            config,
            state: HealthState::new(),
        }
    }

    /// Constructs a new health probe service with custom configuration and shared health state.
    pub fn with_state(
        id: impl Into<String>,
        config: HealthProbeConfig,
        state: HealthState,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            state,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = HealthProbeConfig::default();

        if config.config.is_null() {
            return Ok(Self::with_config(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. listen_addr (defaults to 0.0.0.0:9091)
        let listen_addr = if let Some(addr_str) = raw.get("listen_addr").and_then(|v| v.as_str()) {
            addr_str.parse::<SocketAddr>().map_err(|e| {
                PluginError::Config(format!(
                    "Invalid listen_addr '{addr_str}' for plugin '{}': {e}",
                    config.id
                ))
            })?
        } else {
            defaults.listen_addr
        };

        // 2. liveness_path
        let liveness_path = raw
            .get("liveness_path")
            .and_then(|v| v.as_str())
            .unwrap_or(&defaults.liveness_path)
            .to_string();

        // 3. readiness_path
        let readiness_path = raw
            .get("readiness_path")
            .and_then(|v| v.as_str())
            .unwrap_or(&defaults.readiness_path)
            .to_string();

        Ok(Self::with_config(
            &config.id,
            HealthProbeConfig {
                listen_addr,
                liveness_path,
                readiness_path,
            },
        ))
    }

    /// Determines the zero-byte static response for an HTTP request path.
    #[inline(always)]
    pub fn evaluate_path(
        config: &HealthProbeConfig,
        state: &HealthState,
        path: &str,
    ) -> &'static [u8] {
        if path == config.liveness_path {
            if state.is_live() { RESP_200 } else { RESP_503 }
        } else if path == config.readiness_path {
            if state.is_ready() && !state.is_draining() {
                RESP_200
            } else {
                RESP_503
            }
        } else {
            RESP_404
        }
    }

    /// Handles a single incoming probe connection.
    pub async fn handle_connection(
        config: &HealthProbeConfig,
        state: &HealthState,
        mut stream: tokio::net::TcpStream,
    ) {
        let mut buf = [0u8; 512];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        let req_str = std::str::from_utf8(&buf[..n]).unwrap_or("");

        let line = req_str.lines().next().unwrap_or("");
        let path = line.split_whitespace().nth(1).unwrap_or("");

        let response = Self::evaluate_path(config, state, path);
        let _ = stream.write_all(response).await;
    }
}

impl BackgroundPlugin for HealthProbePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    async fn run(self: Box<Self>, mut shutdown: watch::Receiver<bool>) -> Result<(), PluginError> {
        let listener = TcpListener::bind(self.config.listen_addr)
            .await
            .map_err(|e| {
                PluginError::Config(format!(
                    "Failed to bind health probe auxiliary service to {}: {e}",
                    self.config.listen_addr
                ))
            })?;

        tracing::info!(
            addr = %self.config.listen_addr,
            liveness = %self.config.liveness_path,
            readiness = %self.config.readiness_path,
            "Auxiliary health & readiness probe service listening"
        );

        let config = Arc::new(self.config);
        let state = self.state.clone();

        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, _) = match res {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::warn!(error = %e, "Health probe accept error");
                            continue;
                        }
                    };

                    let cfg_clone = config.clone();
                    let state_clone = state.clone();
                    tokio::spawn(async move {
                        Self::handle_connection(&cfg_clone, &state_clone, stream).await;
                    });
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("Health probe received shutdown signal; entering draining state");
                        state.set_draining(true);
                        break;
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_state_transitions() {
        let state = HealthState::new();
        assert!(state.is_live());
        assert!(!state.is_ready());
        assert!(!state.is_draining());

        state.set_ready(true);
        assert!(state.is_ready());

        state.set_draining(true);
        assert!(state.is_draining());
    }

    #[test]
    fn test_health_probe_from_config() {
        let json_cfg = serde_json::json!({
            "listen_addr": "127.0.0.1:9095",
            "liveness_path": "/livez",
            "readiness_path": "/readyz"
        });
        let config = PluginConfig {
            id: "health_probe".into(),
            phase: String::new(),
            config: json_cfg,
        };

        let plugin = HealthProbePlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "health_probe");
        assert_eq!(
            plugin.config.listen_addr,
            "127.0.0.1:9095".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(plugin.config.liveness_path, "/livez");
        assert_eq!(plugin.config.readiness_path, "/readyz");
    }

    #[test]
    fn test_zero_byte_response_evaluation() {
        let config = HealthProbeConfig::default();
        let state = HealthState::new();

        // 1. Liveness: live by default -> 200
        let resp = HealthProbePlugin::evaluate_path(&config, &state, "/healthz");
        assert_eq!(resp, RESP_200);

        state.set_live(false);
        let resp_dead = HealthProbePlugin::evaluate_path(&config, &state, "/healthz");
        assert_eq!(resp_dead, RESP_503);

        // 2. Readiness: not ready by default -> 503
        let resp_not_ready = HealthProbePlugin::evaluate_path(&config, &state, "/readyz");
        assert_eq!(resp_not_ready, RESP_503);

        state.set_ready(true);
        let resp_ready = HealthProbePlugin::evaluate_path(&config, &state, "/readyz");
        assert_eq!(resp_ready, RESP_200);

        // 3. Readiness during draining -> 503
        state.set_draining(true);
        let resp_draining = HealthProbePlugin::evaluate_path(&config, &state, "/readyz");
        assert_eq!(resp_draining, RESP_503);

        // 4. Unknown path -> 404
        let resp_unknown = HealthProbePlugin::evaluate_path(&config, &state, "/status");
        assert_eq!(resp_unknown, RESP_404);
    }
}
