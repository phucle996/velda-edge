//! Discovery lifecycle and background synchronization service.
//!
//! Maintains current [`EndpointSet`] using lock-free atomic swaps.
//! Strict invariant: DNS resolution runs strictly in the background;
//! request serving hot path never performs DNS lookups.

use arc_swap::ArcSwap;
use std::net::SocketAddr;
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

    /// Asynchronously creates a discovery instance from [`DiscoveryMode`], eagerly resolving
    /// the first endpoint snapshot to eliminate cold-start 503 gaps.
    pub async fn from_mode_async<S: DnsServerProvider, T: DnsTransport>(
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
                Ok(Self::new_dns_eager(host, port, refresh_interval, res).await)
            }
        }
    }

    /// Creates a dynamic DNS discovery instance initialized with optional explicit seed endpoints.
    ///
    /// Preserves Last-Known-Good (LKG) endpoint state on transient DNS errors.
    pub fn new_dns_with_initial<S: DnsServerProvider, T: DnsTransport>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
        initial_endpoints: Vec<Endpoint>,
    ) -> Arc<Self> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut published: Vec<SocketAddr> =
            initial_endpoints.iter().map(|ep| ep.address).collect();
        published.sort_unstable();

        let initial_set = if initial_endpoints.is_empty() {
            EndpointSet::empty()
        } else {
            EndpointSet::new(initial_endpoints, 1)
        };

        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(initial_set),
            shutdown_tx,
        });

        let disc_clone = Arc::clone(&discovery);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                // Floor keeps a TTL of ~0 from turning into a busy re-resolve loop.
                let min_wait = refresh_interval.min(Duration::from_secs(1));
                let mut current_gen = if published.is_empty() { 0 } else { 1 };

                loop {
                    match resolver.resolve(&host, port).await {
                        Ok(endpoints) => {
                            let mut addrs: Vec<SocketAddr> =
                                endpoints.iter().map(|ep| ep.address).collect();
                            addrs.sort_unstable();
                            if addrs != published {
                                current_gen += 1;
                                disc_clone.update_endpoints(endpoints, current_gen);
                                published = addrs;
                            }
                        }

                        Err(e) => {
                            tracing::warn!(
                                host = %host,
                                error = %e,
                                "Background DNS discovery refresh failed; retaining Last-Known-Good endpoints"
                            );
                        }
                    }

                    // Wake when the cache entry expires; the small margin avoids waking a
                    // hair early, hitting the still-valid entry, and then sleeping a full floor.
                    let wait = resolver
                        .cache()
                        .ttl_remaining(&host)
                        .map_or(refresh_interval, |t| {
                            (t + Duration::from_millis(5)).clamp(min_wait, refresh_interval)
                        });

                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        res = shutdown_rx.changed() => {
                            if res.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            });
        } else {
            tracing::warn!(
                host = %host,
                "No active Tokio runtime handle; DNS background refresh task not spawned"
            );
        }

        discovery
    }

    /// Creates a dynamic DNS discovery instance and spawns a background refresh task.
    ///
    /// Preserves Last-Known-Good (LKG) endpoint state on transient DNS errors.
    ///
    /// `refresh_interval` is the longest the published set may go without a re-check.
    /// The task wakes earlier when the cached answer expires (record TTL, or the
    /// negative TTL after a failure), and only publishes a new generation when the
    /// set of addresses actually changed.
    pub fn new_dns<S: DnsServerProvider, T: DnsTransport>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        Self::new_dns_with_initial(host, port, refresh_interval, resolver, Vec::new())
    }

    /// Asynchronously creates a dynamic DNS discovery instance, resolving the initial
    /// endpoint snapshot eagerly to eliminate cold-start 503 gaps before returning.
    pub async fn new_dns_eager<S: DnsServerProvider, T: DnsTransport>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut initial_gen = 0;
        let mut published = Vec::new();

        let initial_set = match resolver.resolve(&host, port).await {
            Ok(endpoints) => {
                let mut addrs: Vec<SocketAddr> = endpoints.iter().map(|ep| ep.address).collect();
                addrs.sort_unstable();
                published = addrs;
                initial_gen = 1;
                EndpointSet::new(endpoints, initial_gen)
            }
            Err(e) => {
                tracing::warn!(
                    host = %host,
                    error = %e,
                    "Initial eager DNS discovery resolution failed; starting with empty set"
                );
                EndpointSet::empty()
            }
        };

        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(initial_set),
            shutdown_tx,
        });

        let disc_clone = Arc::clone(&discovery);
        tokio::spawn(async move {
            let min_wait = refresh_interval.min(Duration::from_secs(1));
            let mut current_gen = initial_gen;

            // Wait until the initial cache entry expires before starting the background refresh loop
            let initial_wait = resolver
                .cache()
                .ttl_remaining(&host)
                .map_or(refresh_interval, |t| {
                    (t + Duration::from_millis(5)).clamp(min_wait, refresh_interval)
                });

            tokio::select! {
                _ = tokio::time::sleep(initial_wait) => {}
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
            }

            loop {
                match resolver.resolve(&host, port).await {
                    Ok(endpoints) => {
                        let mut addrs: Vec<SocketAddr> =
                            endpoints.iter().map(|ep| ep.address).collect();
                        addrs.sort_unstable();
                        if addrs != published {
                            current_gen += 1;
                            disc_clone.update_endpoints(endpoints, current_gen);
                            published = addrs;
                        }
                    }

                    Err(e) => {
                        tracing::warn!(
                            host = %host,
                            error = %e,
                            "Background DNS discovery refresh failed; retaining Last-Known-Good endpoints"
                        );
                    }
                }

                let wait = resolver
                    .cache()
                    .ttl_remaining(&host)
                    .map_or(refresh_interval, |t| {
                        (t + Duration::from_millis(5)).clamp(min_wait, refresh_interval)
                    });

                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
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

    /// Atomically updates the active endpoint snapshot with a new generation.
    pub fn update_endpoints(&self, endpoints: Vec<Endpoint>, generation: u64) {
        let new_set = EndpointSet::new(endpoints, generation);
        self.current.store(Arc::new(new_set));
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
