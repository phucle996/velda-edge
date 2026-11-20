//! Discovery lifecycle and background synchronization service.
//!
//! Maintains current [`EndpointSet`] using lock-free atomic swaps.
//! Strict invariant: DNS resolution runs strictly in the centralized background scheduler
//! on Shadow Worker 0 (`SCHED_IDLE`); request serving hot path never performs DNS lookups.

use arc_swap::ArcSwap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::dns::{DnsResolverProvider, DnsServerProvider, DnsTransport};
use crate::endpoint::{Endpoint, EndpointSet};
use crate::scheduler::{
    ChangeCallback, DiscoveryScheduler, DnsDiscoveryTarget, DnsSrvDiscoveryTarget,
    global_discovery_scheduler,
};

/// Discovery mode declared for an upstream target.
#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    /// Static explicit endpoint list configured ahead of time.
    Explicit(Vec<Endpoint>),
    /// Dynamic DNS A/AAAA resolution against a hostname and port.
    Dns {
        host: String,
        port: u16,
        refresh_interval: Duration,
    },
    /// Dynamic DNS SRV resolution (RFC 2782) with dynamic port and weight.
    DnsSrv {
        service: String,
        refresh_interval: Duration,
    },
    /// External/control-plane registry-backed endpoint feed.
    Registry {
        service_id: String,
        initial_endpoints: Vec<Endpoint>,
    },
}

/// Manages endpoint discovery and background synchronization.
pub struct Discovery {
    current: ArcSwap<EndpointSet>,
    shutdown: AtomicBool,
    on_change: std::sync::Mutex<Option<ChangeCallback>>,
}

impl Discovery {
    /// Creates a static discovery instance with fixed explicit endpoints.
    pub fn new_explicit(endpoints: Vec<Endpoint>) -> Arc<Self> {
        let set = EndpointSet::new(endpoints, 1);
        Arc::new(Self {
            current: ArcSwap::from_pointee(set),
            shutdown: AtomicBool::new(false),
            on_change: std::sync::Mutex::new(None),
        })
    }

    /// Creates a discovery instance from the configured [`DiscoveryMode`].
    pub fn from_mode<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
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
            DiscoveryMode::DnsSrv {
                service,
                refresh_interval,
            } => {
                let res =
                    resolver.ok_or_else(|| crate::error::DiscoveryError::DnsResolutionFailed {
                        host: service.clone(),
                        reason: "DNS resolver required for dynamic DNS SRV mode".into(),
                    })?;
                Ok(Self::new_dns_srv(service, refresh_interval, res))
            }
            DiscoveryMode::Registry {
                service_id: _,
                initial_endpoints,
            } => Ok(Self::new_explicit(initial_endpoints)),
        }
    }

    /// Asynchronously creates a discovery instance from [`DiscoveryMode`], eagerly resolving
    /// the first endpoint snapshot to eliminate cold-start 503 gaps.
    pub async fn from_mode_async<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
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
            DiscoveryMode::DnsSrv {
                service,
                refresh_interval,
            } => {
                let res =
                    resolver.ok_or_else(|| crate::error::DiscoveryError::DnsResolutionFailed {
                        host: service.clone(),
                        reason: "DNS resolver required for dynamic DNS SRV mode".into(),
                    })?;
                Ok(Self::new_dns_srv_eager(service, refresh_interval, res).await)
            }
            DiscoveryMode::Registry {
                service_id: _,
                initial_endpoints,
            } => Ok(Self::new_explicit(initial_endpoints)),
        }
    }

    /// Creates a dynamic DNS discovery instance initialized with optional explicit seed endpoints,
    /// registering it into [`global_discovery_scheduler`] (zero per-service `tokio::spawn` tasks).
    pub fn new_dns_with_initial<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
        initial_endpoints: Vec<Endpoint>,
    ) -> Arc<Self> {
        Self::new_dns_scheduled(
            host,
            port,
            refresh_interval,
            resolver,
            initial_endpoints,
            global_discovery_scheduler(),
            None,
        )
    }

    /// Creates a dynamic DNS discovery instance and registers it with the specified
    /// [`DiscoveryScheduler`] and optional change callback.
    pub fn new_dns_scheduled<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
        initial_endpoints: Vec<Endpoint>,
        scheduler: &DiscoveryScheduler,
        on_change: Option<ChangeCallback>,
    ) -> Arc<Self> {
        let initial_gen = if initial_endpoints.is_empty() { 0 } else { 1 };
        let initial_set = if initial_endpoints.is_empty() {
            EndpointSet::empty()
        } else {
            EndpointSet::new(initial_endpoints.clone(), initial_gen)
        };

        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(initial_set),
            shutdown: AtomicBool::new(false),
            on_change: std::sync::Mutex::new(None),
        });

        let target = Box::new(DnsDiscoveryTarget {
            host,
            port,
            resolver,
        });
        scheduler.register(
            &discovery,
            target,
            refresh_interval,
            &initial_endpoints,
            initial_gen,
            on_change,
        );

        discovery
    }

    /// Creates a dynamic DNS discovery instance registered with [`global_discovery_scheduler`].
    pub fn new_dns<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        Self::new_dns_with_initial(host, port, refresh_interval, resolver, Vec::new())
    }

    /// Asynchronously creates a dynamic DNS discovery instance, resolving the initial
    /// endpoint snapshot eagerly to eliminate cold-start 503 gaps before returning,
    /// and registers it into [`global_discovery_scheduler`].
    pub async fn new_dns_eager<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        Self::new_dns_eager_scheduled(
            host,
            port,
            refresh_interval,
            resolver,
            global_discovery_scheduler(),
            None,
        )
        .await
    }

    /// Asynchronously creates an eagerly-resolved dynamic DNS discovery instance and registers it
    /// with the specified [`DiscoveryScheduler`] and optional change callback.
    pub async fn new_dns_eager_scheduled<
        S: DnsServerProvider + 'static,
        T: DnsTransport + 'static,
    >(
        host: String,
        port: u16,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
        scheduler: &DiscoveryScheduler,
        on_change: Option<ChangeCallback>,
    ) -> Arc<Self> {
        let (initial_endpoints, initial_gen, initial_set) =
            match resolver.resolve(&host, port).await {
                Ok(endpoints) => {
                    let set = EndpointSet::new(endpoints.clone(), 1);
                    (endpoints, 1, set)
                }
                Err(e) => {
                    tracing::warn!(
                        host = %host,
                        error = %e,
                        "Initial eager DNS discovery resolution failed; starting with empty set"
                    );
                    (Vec::new(), 0, EndpointSet::empty())
                }
            };

        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(initial_set),
            shutdown: AtomicBool::new(false),
            on_change: std::sync::Mutex::new(None),
        });

        let target = Box::new(DnsDiscoveryTarget {
            host,
            port,
            resolver,
        });
        scheduler.register(
            &discovery,
            target,
            refresh_interval,
            &initial_endpoints,
            initial_gen,
            on_change,
        );

        discovery
    }

    /// Creates a dynamic DNS SRV discovery instance registered with [`global_discovery_scheduler`].
    pub fn new_dns_srv<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
        service: String,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(EndpointSet::empty()),
            shutdown: AtomicBool::new(false),
            on_change: std::sync::Mutex::new(None),
        });
        let target = Box::new(DnsSrvDiscoveryTarget { service, resolver });
        global_discovery_scheduler().register(&discovery, target, refresh_interval, &[], 0, None);
        discovery
    }

    /// Asynchronously creates an eagerly-resolved dynamic DNS SRV discovery instance.
    pub async fn new_dns_srv_eager<S: DnsServerProvider + 'static, T: DnsTransport + 'static>(
        service: String,
        refresh_interval: Duration,
        resolver: Arc<DnsResolverProvider<S, T>>,
    ) -> Arc<Self> {
        let (initial_endpoints, initial_gen, initial_set) =
            match resolver.resolve_srv(&service).await {
                Ok(endpoints) => {
                    let set = EndpointSet::new(endpoints.clone(), 1);
                    (endpoints, 1, set)
                }
                Err(e) => {
                    tracing::warn!(
                        service = %service,
                        error = %e,
                        "Initial eager DNS SRV resolution failed; starting with empty set"
                    );
                    (Vec::new(), 0, EndpointSet::empty())
                }
            };

        let discovery = Arc::new(Self {
            current: ArcSwap::from_pointee(initial_set),
            shutdown: AtomicBool::new(false),
            on_change: std::sync::Mutex::new(None),
        });
        let target = Box::new(DnsSrvDiscoveryTarget { service, resolver });
        global_discovery_scheduler().register(
            &discovery,
            target,
            refresh_interval,
            &initial_endpoints,
            initial_gen,
            None,
        );
        discovery
    }

    /// Registers an event-driven callback invoked whenever endpoints are updated.
    pub fn set_on_change(&self, cb: ChangeCallback) {
        if let Ok(mut guard) = self.on_change.lock() {
            *guard = Some(cb);
        }
    }

    /// Returns the current active snapshot of discovered endpoints (zero-allocation read).
    #[inline]
    pub fn current_endpoints(&self) -> Arc<EndpointSet> {
        self.current.load_full()
    }

    /// Atomically updates the active endpoint snapshot with a new generation.
    pub fn update_endpoints(&self, endpoints: Vec<Endpoint>, generation: u64) {
        let new_set = EndpointSet::new(endpoints, generation);
        let new_arc = Arc::new(new_set);
        let cb = self
            .on_change
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(Arc::clone));
        self.current.store(Arc::clone(&new_arc));
        if let Some(cb) = cb {
            cb(new_arc.all_endpoints(), generation);
        }
    }

    /// Signals the scheduler to stop refreshing this discovery instance.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Returns `true` if [`Discovery::shutdown`] has been called.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}
