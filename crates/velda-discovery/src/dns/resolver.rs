//! Phase 3: Resolver Phase.
//!
//! Responsible for coordinating domain resolution of backend targets:
//! - Checking Cache (Phase 4).
//! - Checking in-memory static hosts (Phase 1).
//! - Singleflight in-flight query deduplication (prevents Thundering Herd / Cache Stampede).
//! - Querying upstream DNS nameservers (Phase 2) via `DnsTransport` with timeout and sequential failover.
//! - Engaging Last-Known-Good (LKG) resilience fallback upon network errors.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::broadcast;

use crate::dns::bootstrap::HostsFileSource;
use crate::dns::cache::{CacheLookup, DnsCache};
use crate::dns::server::DnsServerProvider;
use crate::endpoint::Endpoint;
use crate::error::{DiscoveryError, Result};

// ============================================================================
// 1. Wire Transport Trait
// ============================================================================

/// Asynchronous wire transport for querying DNS nameservers.
pub trait DnsTransport: Send + Sync + 'static {
    /// Queries the target DNS server for `host` A and AAAA records.
    fn query(
        &self,
        server: SocketAddr,
        host: &str,
    ) -> impl std::future::Future<Output = Result<Vec<IpAddr>>> + Send;
}

impl<T: DnsTransport + ?Sized> DnsTransport for Arc<T> {
    fn query(
        &self,
        server: SocketAddr,
        host: &str,
    ) -> impl std::future::Future<Output = Result<Vec<IpAddr>>> + Send {
        (**self).query(server, host)
    }
}

// ============================================================================
// 2. Resolver Configuration
// ============================================================================

/// TTL configurations for DNS resolution and caching.
#[derive(Debug, Clone, Copy)]
pub struct DnsResolverConfig {
    /// Positive cache TTL for nameserver responses (default: 30s).
    pub positive_ttl: Duration,
    /// Positive cache TTL for static `/etc/hosts` entries (default: 300s).
    pub hosts_ttl: Duration,
    /// Negative cache TTL for non-existent domains (default: 5s).
    pub negative_ttl: Duration,
    /// FIX (Blocker 4 - UDP Hang / Timeout): Strict timeout per nameserver query attempt (default: 2s).
    pub query_timeout: Duration,
}

impl Default for DnsResolverConfig {
    fn default() -> Self {
        Self {
            positive_ttl: Duration::from_secs(30),
            hosts_ttl: Duration::from_secs(300),
            negative_ttl: Duration::from_secs(5),
            query_timeout: Duration::from_secs(2),
        }
    }
}

// ============================================================================
// 3. Primary Resolver Provider
// ============================================================================

/// Primary DNS resolver coordinating cache lookups, hosts resolution, singleflight, and wire queries.
pub struct DnsResolverProvider<S: DnsServerProvider, T: DnsTransport> {
    servers: S,
    hosts: HostsFileSource,
    transport: T,
    cache: DnsCache,
    config: DnsResolverConfig,
    lkg: RwLock<HashMap<String, Vec<Endpoint>>>,
    // FIX (Blocker 3 - Cache Stampede / Thundering Herd): In-flight query deduplication map
    inflight: tokio::sync::Mutex<HashMap<String, broadcast::Sender<Result<Vec<Endpoint>>>>>,
}

impl<S: DnsServerProvider, T: DnsTransport> DnsResolverProvider<S, T> {
    /// Creates a new resolver with system hosts file and default configuration.
    pub fn new(servers: S, transport: T) -> Self {
        let hosts = HostsFileSource::from_system().unwrap_or_default();
        Self::with_hosts_and_config(servers, hosts, transport, DnsResolverConfig::default())
    }

    /// Creates a resolver with custom hosts source and default configuration.
    pub fn with_hosts(servers: S, hosts: HostsFileSource, transport: T) -> Self {
        Self::with_hosts_and_config(servers, hosts, transport, DnsResolverConfig::default())
    }

    /// Creates a resolver with fully customized dependencies and TTL settings.
    pub fn with_hosts_and_config(
        servers: S,
        hosts: HostsFileSource,
        transport: T,
        config: DnsResolverConfig,
    ) -> Self {
        Self {
            servers,
            hosts,
            transport,
            cache: DnsCache::new(),
            config,
            lkg: RwLock::new(HashMap::new()),
            inflight: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Returns a reference to the inner in-memory cache.
    pub fn cache(&self) -> &DnsCache {
        &self.cache
    }

    /// Resolves `host` into a list of endpoints with `port`.
    ///
    /// Resolution Order:
    /// 1. `DnsCache::get` (Positive Hit -> Return Ok; Negative Hit -> Return Err)
    /// 2. `HostsFileSource::lookup` (Hit -> Insert into DnsCache with `hosts_ttl` -> Return Ok)
    /// 3. In-flight Deduplication (Singleflight): Coalesces simultaneous requests for the same domain.
    /// 4. Upstream Nameserver Wire Query with Timeout & Failover.
    /// 5. Fallback to Last-Known-Good (LKG) or populate negative cache.
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<Endpoint>> {
        let key = host.to_lowercase();

        // -------------------------------------------------------------
        // Step 1: DnsCache is the Single Source of Truth on Hot Path
        // -------------------------------------------------------------
        match self.cache.get(&key) {
            CacheLookup::Hit(ips) => {
                let endpoints = ips_to_endpoints(&key, port, ips);
                return Ok(endpoints);
            }
            CacheLookup::NegativeHit => {
                return Err(DiscoveryError::NegativeCacheHit {
                    host: host.to_string(),
                });
            }
            CacheLookup::Miss => {}
        }

        // -------------------------------------------------------------
        // Step 2: Local Hosts File Lookup
        // -------------------------------------------------------------
        if let Some(ip) = self.hosts.lookup(&key) {
            let ips = vec![ip];
            // Cache static hosts entry uniformly into DnsCache
            self.cache
                .insert_positive(&key, ips.clone(), self.config.hosts_ttl);

            let endpoints = ips_to_endpoints(&key, port, ips);
            return Ok(endpoints);
        }

        // -------------------------------------------------------------
        // Step 3: FIX (Blocker 3 - Cache Stampede / Thundering Herd):
        // Singleflight query deduplication to prevent flooding upstream DNS.
        // -------------------------------------------------------------
        let mut rx = {
            let mut inflight = self.inflight.lock().await;
            if let Some(tx) = inflight.get(&key) {
                // Another task is currently resolving this exact domain; subscribe and wait!
                tx.subscribe()
            } else {
                // We are the leader task performing the wire query
                let (tx, _) = broadcast::channel(1);
                inflight.insert(key.clone(), tx);
                drop(inflight);

                // Execute the resolution workflow
                let outcome = self.execute_wire_query(host, &key, port).await;

                // Remove from inflight map and broadcast result to all awaiting subscriber tasks
                let mut inflight = self.inflight.lock().await;
                if let Some(tx) = inflight.remove(&key) {
                    let _ = tx.send(outcome.clone());
                }

                return outcome;
            }
        };

        // Awaiting existing in-flight resolution result
        match rx.recv().await {
            Ok(result) => result,
            Err(_) => {
                // If leader cancelled or channel dropped, retry direct resolution
                self.execute_wire_query(host, &key, port).await
            }
        }
    }

    /// Internal execution of wire queries against nameservers with timeouts and failover.
    async fn execute_wire_query(&self, host: &str, key: &str, port: u16) -> Result<Vec<Endpoint>> {
        let server_list = self.servers.servers();
        if server_list.is_empty() {
            return Err(DiscoveryError::EmptyServerList);
        }

        let mut last_err = None;
        for &server in &server_list {
            // FIX (Blocker 4 - UDP Hang / Timeout): Wrap wire query with strict timeout to prevent indefinite hangs
            let query_future = self.transport.query(server, key);
            match tokio::time::timeout(self.config.query_timeout, query_future).await {
                Ok(Ok(ips)) if !ips.is_empty() => {
                    // Populate positive cache
                    self.cache
                        .insert_positive(key, ips.clone(), self.config.positive_ttl);

                    let endpoints = ips_to_endpoints(key, port, ips);

                    // Update LKG (Last-Known-Good)
                    self.lkg
                        .write()
                        .unwrap()
                        .insert(key.to_string(), endpoints.clone());

                    return Ok(endpoints);
                }
                Ok(Ok(_)) => {
                    // Empty IP list from this nameserver -> record error and failover to next server
                    last_err = Some(DiscoveryError::DnsResolutionFailed {
                        host: host.to_string(),
                        reason: "no A/AAAA records returned".into(),
                    });
                }
                Ok(Err(err)) => {
                    last_err = Some(err);
                }
                Err(_) => {
                    // FIX (Blocker 4 - UDP Hang / Timeout): Nameserver timed out; failover to next
                    tracing::warn!(
                        server = %server,
                        host = %host,
                        timeout_ms = self.config.query_timeout.as_millis(),
                        "DNS nameserver query timed out; failing over to next server"
                    );
                    last_err = Some(DiscoveryError::ServerUnreachable {
                        address: server,
                        reason: format!("query timed out after {:?}", self.config.query_timeout),
                    });
                }
            }
        }

        // -------------------------------------------------------------
        // Step 4: Resilience Fallback (LKG) or Negative Cache
        // -------------------------------------------------------------
        if let Some(lkg_endpoints) = self.lkg.read().unwrap().get(key) {
            tracing::warn!(
                host = %host,
                "DNS lookup failed across all nameservers; falling back to Last-Known-Good (LKG) endpoints"
            );
            return Ok(lkg_endpoints.clone());
        }

        // Populate negative cache on complete failure
        self.cache.insert_negative(key, self.config.negative_ttl);

        Err(
            last_err.unwrap_or_else(|| DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "all nameservers failed".into(),
            }),
        )
    }
}

/// Helper converting IP addresses and a port into canonical `Endpoint` entities.
fn ips_to_endpoints(host: &str, port: u16, ips: Vec<IpAddr>) -> Vec<Endpoint> {
    ips.into_iter()
        .map(|ip| {
            let addr = SocketAddr::new(ip, port);
            Endpoint::new(host, addr, 1)
        })
        .collect()
}
