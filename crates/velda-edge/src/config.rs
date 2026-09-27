//! Edge configuration structures and binary artifact loader.
//!
//! Loads domain-isolated binary artifacts (*.bin) compiled by `velda-sync`
//! from the durable Last Known Good (LKG) storage directory.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use thiserror::Error;
use velda_sync::post_sync::listener::{self, ListenerConfig};
use velda_sync::post_sync::plugin::{self, PluginConfig};
use velda_sync::post_sync::route::{self, RouteConfig};
use velda_sync::post_sync::tls::{self, TlsProfileConfig};
use velda_sync::post_sync::upstream::{self, UpstreamConfig};
use velda_transport::IngressBinding;

/// Error conditions encountered within `velda-edge`.
#[derive(Debug, Error)]
pub enum EdgeError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Sync domain error in '{domain}': {reason}")]
    SyncDomain { domain: String, reason: String },

    #[error("Invalid socket address '{addr}' for listener '{id}': {reason}")]
    InvalidAddress {
        id: String,
        addr: String,
        reason: String,
    },

    #[error("Transport error: {0}")]
    Transport(#[from] velda_transport::TransportError),

    #[error("Configuration reload failed: {0}")]
    Reload(String),
}

impl From<velda_sync::SyncError> for EdgeError {
    fn from(err: velda_sync::SyncError) -> Self {
        match err {
            velda_sync::SyncError::Io(e) => EdgeError::Io(e),
            velda_sync::SyncError::Compile { domain, reason } => {
                EdgeError::SyncDomain { domain, reason }
            }
            velda_sync::SyncError::Validation { domain, reason } => {
                EdgeError::SyncDomain { domain, reason }
            }
            other => EdgeError::SyncDomain {
                domain: "edge".into(),
                reason: other.to_string(),
            },
        }
    }
}

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
pub fn load_tls(runtime_dir: &Path) -> Result<Vec<TlsProfileConfig>, EdgeError> {
    let path = runtime_dir.join("tls.bin");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(&path)?;
    let (_header, file) = tls::unpack_tls_from_binary(&bytes)?;
    Ok(file.profiles)
}

/// Converts a declarative `ListenerConfig` into a `velda-transport` `IngressBinding`.
pub fn listener_to_binding(config: &ListenerConfig) -> Result<IngressBinding, EdgeError> {
    let addr: SocketAddr = config
        .address
        .parse()
        .map_err(|e| EdgeError::InvalidAddress {
            id: config.id.clone(),
            addr: config.address.clone(),
            reason: format!("{e}"),
        })?;

    let proto_str = match config.application.protocol.as_str() {
        "raw" => config.transport.protocol.as_str(),
        "http" => match (
            config.transport.protocol.as_str(),
            config.application.version.as_deref(),
        ) {
            ("udp", _) | (_, Some("3")) => "http3",
            (_, Some("2")) => "http2",
            (_, Some("1.1") | Some("1.0")) => "http1",
            _ => "http",
        },
        other => other,
    };

    let binding = IngressBinding::new(&config.id, addr, proto_str, config.tls.enabled)?;

    Ok(binding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    };
    use velda_transport::PathKind;

    #[test]
    fn test_listener_to_binding_mapping() {
        // Raw TCP -> L4Direct
        let raw_tcp = ListenerConfig {
            id: "tcp-raw".into(),
            address: "127.0.0.1:9000".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "raw".into(),
                version: None,
            },
            tls: ListenerTlsConfig::default(),
        };
        let binding = listener_to_binding(&raw_tcp).unwrap();
        assert_eq!(binding.protocol, "tcp");
        assert_eq!(binding.path, PathKind::L4Direct);

        // HTTP/1.1 over TCP -> Http1
        let http1 = ListenerConfig {
            id: "http".into(),
            address: "127.0.0.1:80".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("1.1".into()),
            },
            tls: ListenerTlsConfig::default(),
        };
        let binding = listener_to_binding(&http1).unwrap();
        assert_eq!(binding.protocol, "http1");
        assert_eq!(binding.path, PathKind::Http1);

        // HTTP/2 over TCP -> Http2
        let http2 = ListenerConfig {
            id: "http2".into(),
            address: "127.0.0.1:8080".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("2".into()),
            },
            tls: ListenerTlsConfig::default(),
        };
        let binding = listener_to_binding(&http2).unwrap();
        assert_eq!(binding.protocol, "http2");
        assert_eq!(binding.path, PathKind::Http2);

        // HTTP over UDP -> Http3
        let http3 = ListenerConfig {
            id: "http3".into(),
            address: "127.0.0.1:443".into(),
            transport: ListenerTransportConfig {
                protocol: "udp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("3".into()),
            },
            tls: ListenerTlsConfig { enabled: true },
        };
        let binding = listener_to_binding(&http3).unwrap();
        assert_eq!(binding.protocol, "http3");
        assert_eq!(binding.path, PathKind::Http3);
        assert!(binding.tls_enabled);
    }
}
