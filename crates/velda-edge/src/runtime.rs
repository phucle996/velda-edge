//! In-memory Runtime snapshot and lock-free state owner.
//!
//! Owns the compiled runtime snapshot holding all domain models (listeners,
//! routes, upstreams, plugins, tls) loaded from binary artifacts (*.bin).
//! Wrapped in [`ArcSwap`] to enable zero-overhead, lock-free $O(1)$ reads on the
//! request serving hot path, and atomic swaps upon configuration reloads.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use velda_sync::post_sync::listener::ListenerConfig;
use velda_sync::post_sync::plugin::PluginConfig;
use velda_sync::post_sync::route::RouteConfig;
use velda_sync::post_sync::tls::TlsProfileConfig;
use velda_sync::post_sync::upstream::UpstreamConfig;
use velda_transport::IngressBinding;

use crate::config::{
    EdgeError, load_listeners, load_plugins, load_routes, load_tls, load_upstreams,
};

/// Read-only snapshot of all active runtime domains compiled in RAM.
#[derive(Debug, Clone, Default)]
pub struct Runtime {
    /// Active revision number of this runtime state.
    pub revision: u64,
    /// Active listener declarations (drives IngressBinding on TrafficEngine).
    pub listeners: Vec<ListenerConfig>,
    /// Active routing rules for request dispatching.
    pub routes: Vec<RouteConfig>,
    /// Active upstream target definitions and load-balancing metadata.
    pub upstreams: Vec<UpstreamConfig>,
    /// Active plugin hook chain configurations.
    pub plugins: Vec<PluginConfig>,
    /// Active TLS profiles and certificate references.
    pub tls: Vec<TlsProfileConfig>,
}

/// Thread-safe, lock-free container for the active [`Runtime`] snapshot.
pub type SharedRuntime = Arc<ArcSwap<Runtime>>;

impl Runtime {
    /// Creates a new empty runtime snapshot with revision 0.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Loads a complete runtime snapshot from the LKG binary artifacts in `runtime_dir`.
    pub fn load_from_storage(runtime_dir: &Path, revision: u64) -> Result<Self, EdgeError> {
        let listeners = load_listeners(runtime_dir)?;
        let routes = load_routes(runtime_dir)?;
        let upstreams = load_upstreams(runtime_dir)?;
        let plugins = load_plugins(runtime_dir)?;
        let tls = load_tls(runtime_dir)?;

        Ok(Self {
            revision,
            listeners,
            routes,
            upstreams,
            plugins,
            tls,
        })
    }

    /// Converts active listener configurations into `velda-transport` [`IngressBinding`]s.
    pub fn active_bindings(&self) -> Result<Vec<IngressBinding>, EdgeError> {
        let mut bindings = Vec::with_capacity(self.listeners.len());
        for cfg in &self.listeners {
            bindings.push(listener_to_binding(cfg)?);
        }
        Ok(bindings)
    }

    /// Returns the number of configured listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }

    /// Returns the number of configured routes.
    #[inline]
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    /// Returns the number of configured upstreams.
    #[inline]
    pub fn upstream_count(&self) -> usize {
        self.upstreams.len()
    }
}

/// Helper function to create a new shared runtime holder initialized with the given snapshot.
pub fn new_shared_runtime(initial: Runtime) -> SharedRuntime {
    Arc::new(ArcSwap::from_pointee(initial))
}

/// Converts a declarative `ListenerConfig` into a `velda-transport` `IngressBinding`.
fn listener_to_binding(config: &ListenerConfig) -> Result<IngressBinding, EdgeError> {
    let addr: SocketAddr = config
        .address
        .parse()
        .map_err(|e| EdgeError::InvalidAddress {
            id: config.id.clone(),
            addr: config.address.clone(),
            reason: format!("{e}"),
        })?;

    let binding = IngressBinding::from_protocols(
        &config.id,
        addr,
        &config.transport.protocol,
        &config.application.protocol,
        config.tls.enabled,
    )?;

    Ok(binding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerTlsConfig, ListenerTransportConfig,
    };
    use velda_transport::PathKind;

    #[test]
    fn test_empty_runtime() {
        let rt = Runtime::empty();
        assert_eq!(rt.revision, 0);
        assert_eq!(rt.listener_count(), 0);
        assert_eq!(rt.route_count(), 0);
    }

    #[test]
    fn test_shared_runtime_atomic_swap() {
        let initial = Runtime {
            revision: 1,
            ..Default::default()
        };
        let shared = new_shared_runtime(initial);
        assert_eq!(shared.load().revision, 1);

        let updated = Runtime {
            revision: 2,
            ..Default::default()
        };
        shared.store(Arc::new(updated));
        assert_eq!(shared.load().revision, 2);
    }

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

        // HTTP/1.1 over TCP -> L7Handoff
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
        assert_eq!(binding.protocol, "tcp");
        assert_eq!(binding.path, PathKind::L7Handoff);

        // HTTP/2 over TCP -> L7Handoff
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
        assert_eq!(binding.protocol, "tcp");
        assert_eq!(binding.path, PathKind::L7Handoff);

        // HTTP over UDP -> L7Handoff
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
        assert_eq!(binding.protocol, "udp");
        assert_eq!(binding.path, PathKind::L7Handoff);
        assert!(binding.tls_enabled);
    }
}
