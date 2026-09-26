//! Discovery lifecycle and background synchronization service.
//!
//! Maintains current [`EndpointSet`] using lock-free atomic swaps.
//! Strict invariant: DNS resolution runs strictly in the background;
//! request serving hot path never performs DNS lookups.

use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::dns::{DnsResolverProvider, DnsServerProvider, DnsTransport};
use crate::endpoint::{Endpoint, EndpointSet};

/// Discovery mode declared for an upstream target.
#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    /// Static explicit endpoint list configured ahead of time.
    Explicit(Vec<Endpoint>),
    /// Dynamic DNS resolution against a hostname and port.
    Dns {
        host: String,
        port: u16,
        refresh_interval: Duration,
    },
}

/// Manages endpoint discovery and background synchronization.
pub struct Discovery {
    current: ArcSwap<EndpointSet>,
    shutdown_tx: watch::Sender<bool>,
}

impl Discovery {
    /// Creates a static discovery instance with fixed explicit endpoints.
    pub fn new_explicit(endpoints: Vec<Endpoint>) -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        let set = EndpointSet::new(endpoints, 1);
        Arc::new(Self {
            current: ArcSwap::from_pointee(set),
            shutdown_tx,
        })
    }

    /// Creates a discovery instance from the configured [`DiscoveryMode`].
    pub fn from_mode<S: DnsServerProvider, T: DnsTransport>(
        mode: DiscoveryMode,
        resolver: Option<Arc<DnsResolverProvider<S, T>>>,
    ) -> crate::error::Result<Arc<Self>> {
        match mode {
            DiscoveryMode::Explicit(endpoints) => Ok(Self::new_explicit(endpoints)),
            DiscoveryMode::Dns {
                host,
                port,
                refresh_interval,
            } => {
                let res =
                    resolver.ok_or_else(|| crate::error::DiscoveryError::DnsResolutionFailed {
                        host: host.clone(),
                        reason: "DNS resolver required for dynamic DNS mode".into(),
                    })?;
                Ok(Self::new_dns(host, port, refresh_interval, res))
            }
        }
    }

    /// Creates a dynamic DNS discovery instance and spawns a background refresh task.
    ///
    /// Preserves Last-Known-Good (LKG) endpoint state on transient DNS errors.
    pub fn new_dns<S: DnsServerProvider, T: DnsTransport>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let initial_set = EndpointSet::empty();

        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(initial_set),
            shutdown_tx,
        });

        let disc_clone = Arc::clone(&discovery);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(refresh_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut current_gen = 0;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match resolver.resolve(&host, port).await {
                            Ok(endpoints) => {
                                current_gen += 1;
                                let new_set = EndpointSet::new(endpoints, current_gen);
                                disc_clone.current.store(Arc::new(new_set));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    host = %host,
                                    error = %e,
                                    "Background DNS discovery refresh failed; retaining Last-Known-Good endpoints"
                                );
                            }
                        }
                    }
                    res = shutdown_rx.changed() => {
                        if res.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        discovery
    }

    /// Returns the current active snapshot of discovered endpoints (zero-allocation read).
    #[inline]
    pub fn current_endpoints(&self) -> Arc<EndpointSet> {
        self.current.load_full()
    }

    /// Signals the background refresh loop to gracefully shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}
