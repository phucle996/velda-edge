//! In-memory Runtime snapshot and lock-free state owner.
//!
//! Owns the compiled runtime snapshot holding all domain models (listeners,
//! routes, upstreams, plugins, tls) loaded from binary artifacts (*.bin).
//! Wrapped in [`ArcSwap`] to enable zero-overhead, lock-free $O(1)$ reads on the
//! request serving hot path, and atomic swaps upon configuration reloads.

pub(crate) mod composer;
pub(crate) mod h3;
pub(crate) mod tls;
mod transport;

use std::sync::Arc;

use arc_swap::ArcSwap;
use velda_composer::Composer;
use velda_sync::post_sync::listener::ListenerConfig;
use velda_sync::post_sync::plugin::PluginConfig;
use velda_sync::post_sync::route::RouteConfig;
use velda_sync::post_sync::tls::TlsConfig;
use velda_sync::post_sync::upstream::UpstreamConfig;
use velda_tls::TlsServerEngine;
use velda_transport::IngressBinding;

use crate::error::EdgeError;

/// Read-only snapshot of declarative domain configurations loaded into RAM.
#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    /// Active listener declarations (drives IngressBinding on TrafficEngine).
    pub listeners: Vec<ListenerConfig>,
    /// Active routing rules for request dispatching.
    pub routes: Vec<RouteConfig>,
    /// Active upstream target definitions and load-balancing metadata.
    pub upstreams: Vec<UpstreamConfig>,
    /// Active plugin hook chain configurations.
    pub plugins: Vec<PluginConfig>,
    /// Active TLS configurations and certificate references.
    pub tls: Vec<TlsConfig>,
}

impl RuntimeConfig {
    /// Converts active listener configurations into `velda-transport` [`IngressBinding`]s.
    pub fn active_bindings(&self) -> Result<Vec<IngressBinding>, EdgeError> {
        transport::active_bindings(&self.listeners)
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

/// Active in-memory runtime snapshot holding declarative domain configurations
/// and pre-compiled execution engines for hot-path request serving.
#[derive(Debug, Clone, Default)]
pub struct Runtime {
    /// Active revision number of this runtime state.
    pub revision: u64,
    /// Declarative configuration domains loaded from binary artifacts (*.bin).
    pub config: RuntimeConfig,
    /// Pre-compiled Composer with typed listener composition entries for O(1) hot-path lookup.
    pub composer: Composer,
    /// Pre-compiled downstream TLS server engine for O(1) hot-path handshake execution.
    pub tls_server: Option<TlsServerEngine>,
    /// Pre-compiled packet-driven HTTP/3 QUIC engine (shared state across multiplexed UDP datagrams).
    pub h3_engine: Option<Arc<tokio::sync::Mutex<velda_http::Http3Engine>>>,
}

/// Thread-safe, lock-free container for the active [`Runtime`] snapshot.
pub type SharedRuntime = Arc<ArcSwap<Runtime>>;

impl Runtime {
    /// Creates a new empty runtime snapshot with revision 0.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Converts active listener configurations into `velda-transport` [`IngressBinding`]s.
    pub fn active_bindings(&self) -> Result<Vec<IngressBinding>, EdgeError> {
        self.config.active_bindings()
    }

    /// Returns the number of configured listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.config.listener_count()
    }

    /// Returns the number of configured routes.
    #[inline]
    pub fn route_count(&self) -> usize {
        self.config.route_count()
    }

    /// Returns the number of configured upstreams.
    #[inline]
    pub fn upstream_count(&self) -> usize {
        self.config.upstream_count()
    }
}

/// Helper function to create a new shared runtime holder initialized with the given snapshot.
pub fn new_shared_runtime(initial: Runtime) -> SharedRuntime {
    Arc::new(ArcSwap::from_pointee(initial))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
