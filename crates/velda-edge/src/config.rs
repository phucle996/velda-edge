//! Edge configuration structures and binary artifact loader.
//!
//! Loads domain-isolated binary artifacts (*.bin) compiled by `velda-sync`
//! from the durable Last Known Good (LKG) storage directory.

use std::path::{Path, PathBuf};

use velda_sync::post_sync::listener::{self, ListenerConfig};
use velda_sync::post_sync::plugin::{self, PluginConfig};
use velda_sync::post_sync::route::{self, RouteConfig};
use velda_sync::post_sync::tls::{self, TlsConfig};
use velda_sync::post_sync::upstream::{self, UpstreamConfig};

use crate::error::EdgeError;

/// Static configuration parameters for the `velda-edge` process.
#[derive(Debug, Clone)]
pub struct EdgeConfig {
    /// Root directory storing LKG configurations (e.g. `/var/lib/velda` or `./storage`).
    pub storage_dir: PathBuf,
    /// Unix Domain Socket path for IPC notifications from `velda-sync` (e.g. `/run/velda/edge.sock`).
    pub socket_path: PathBuf,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        let storage_dir = std::env::var("VELDA_STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/velda"));

        let socket_path = std::env::var("VELDA_SOCKET_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/run/velda/edge.sock"));

        Self {
            storage_dir,
            socket_path,
        }
    }
}

impl EdgeConfig {
    /// Creates a new configuration targeting specified storage and socket paths.
    pub fn new(storage_dir: impl Into<PathBuf>, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            storage_dir: storage_dir.into(),
            socket_path: socket_path.into(),
        }
    }

    /// Returns the path to the runtime binary directory (`storage_dir/runtime`).
    pub fn runtime_dir(&self) -> PathBuf {
        self.storage_dir.join("runtime")
    }

    /// Returns the path to a specific domain's binary artifact.
    pub fn domain_binary_path(&self, domain: &str) -> PathBuf {
        self.runtime_dir().join(format!("{domain}.bin"))
    }
}

// ============================================================================
// LKG Binary Artifact Loaders
// ============================================================================

/// Reads and unpacks `listeners.bin` from storage if present.
pub fn load_listeners(runtime_dir: &Path) -> Result<Vec<ListenerConfig>, EdgeError> {
    let path = runtime_dir.join("listeners.bin");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(&path)?;
    let (_header, listeners) = listener::unpack_listeners_from_binary(&bytes)?;
    Ok(listeners)
}

/// Reads and unpacks `routes.bin` from storage if present.
pub fn load_routes(runtime_dir: &Path) -> Result<Vec<RouteConfig>, EdgeError> {
    let path = runtime_dir.join("routes.bin");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(&path)?;
    let (_header, routes) = route::unpack_routes_from_binary(&bytes)?;
    Ok(routes)
}

/// Reads and unpacks `upstreams.bin` from storage if present.
pub fn load_upstreams(runtime_dir: &Path) -> Result<Vec<UpstreamConfig>, EdgeError> {
    let path = runtime_dir.join("upstreams.bin");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(&path)?;
    let (_header, upstreams) = upstream::unpack_upstreams_from_binary(&bytes)?;
    Ok(upstreams)
}

/// Reads and unpacks `plugins.bin` from storage if present.
pub fn load_plugins(runtime_dir: &Path) -> Result<Vec<PluginConfig>, EdgeError> {
    let path = runtime_dir.join("plugins.bin");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(&path)?;
    let (_header, plugins) = plugin::unpack_plugins_from_binary(&bytes)?;
    Ok(plugins)
}

/// Reads and unpacks `tls.bin` from storage if present.
pub fn load_tls(runtime_dir: &Path) -> Result<Vec<TlsConfig>, EdgeError> {
    let path = runtime_dir.join("tls.bin");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(&path)?;
    let (_header, tls) = tls::unpack_tls_from_binary(&bytes)?;
    Ok(tls)
}
