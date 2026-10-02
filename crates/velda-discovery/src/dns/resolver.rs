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

/// TTL and capacity configurations for DNS resolution, caching, and LKG resilience.
#[derive(Debug, Clone, Copy)]
pub struct DnsResolverConfig {
    /// Positive cache TTL for nameserver responses (default: 30s).
    pub positive_ttl: Duration,
    /// Positive cache TTL for static `/etc/hosts` entries (default: 300s).
    pub hosts_ttl: Duration,
    /// Negative cache TTL for non-existent domains (default: 5s).
    pub negative_ttl: Duration,
    /// Strict timeout per nameserver query attempt (default: 2s).
    pub query_timeout: Duration,
    /// Maximum capacity of entries per cache table (positive and negative).
    pub cache_capacity: usize,
    /// Maximum capacity of entries in the Last-Known-Good (LKG) resilience map.
    pub lkg_capacity: usize,
    /// Maximum UDP packet buffer size for wire responses (default: 1024).
    pub max_packet_size: usize,
}

impl Default for DnsResolverConfig {
    fn default() -> Self {
        Self {
            positive_ttl: Duration::from_secs(30),
            hosts_ttl: Duration::from_secs(300),
            negative_ttl: Duration::from_secs(5),
            query_timeout: Duration::from_secs(2),
            cache_capacity: 50_000,
            lkg_capacity: 10_000,
            max_packet_size: 1024,
        }
    }
}

impl DnsResolverConfig {
    /// Constructs a resolver configuration tailored to the host's [`velda_core::MemoryTier`].
    pub fn for_tier(tier: velda_core::MemoryTier) -> Self {
        match tier {
            velda_core::MemoryTier::Constrained => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(15),
                query_timeout: Duration::from_secs(3),
                cache_capacity: 1_000,
                lkg_capacity: 500,
                max_packet_size: 1024,
            },
            velda_core::MemoryTier::Small => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(10),
                query_timeout: Duration::from_secs(2),
                cache_capacity: 10_000,
                lkg_capacity: 2_000,
                max_packet_size: 1024,
            },
            velda_core::MemoryTier::Medium => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(5),
                query_timeout: Duration::from_secs(2),
                cache_capacity: 50_000,
                lkg_capacity: 10_000,
                max_packet_size: 1024,
            },
            velda_core::MemoryTier::Large => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(5),
                query_timeout: Duration::from_millis(1500),
                cache_capacity: 150_000,
                lkg_capacity: 30_000,
                max_packet_size: 1024,
            },
            velda_core::MemoryTier::XLarge => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(5),
                query_timeout: Duration::from_secs(1),
                cache_capacity: 400_000,
                lkg_capacity: 80_000,
                max_packet_size: 1024,
            },
            velda_core::MemoryTier::TwoXLarge => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(5),
                query_timeout: Duration::from_secs(1),
                cache_capacity: 1_000_000,
                lkg_capacity: 200_000,
                max_packet_size: 1024,
            },
            velda_core::MemoryTier::Ultra => Self {
                positive_ttl: Duration::from_secs(30),
                hosts_ttl: Duration::from_secs(300),
                negative_ttl: Duration::from_secs(5),
                query_timeout: Duration::from_secs(1),
                cache_capacity: 2_500_000,
                lkg_capacity: 500_000,
                max_packet_size: 1024,
            },
        }
    }
}

// ============================================================================
// 3. Primary Resolver Provider
// ============================================================================

type InflightMap = HashMap<String, broadcast::Sender<Result<Arc<[IpAddr]>>>>;

/// Primary DNS resolver coordinating cache lookups, hosts resolution, singleflight, and wire queries.
pub struct DnsResolverProvider<S: DnsServerProvider, T: DnsTransport> {
    servers: S,
    hosts: HostsFileSource,
    transport: T,
    cache: DnsCache,
    config: DnsResolverConfig,
    lkg: RwLock<HashMap<String, Arc<[IpAddr]>>>,
    // FIX (Blocker 3 - Cache Stampede / Thundering Herd): In-flight query deduplication map
    inflight: tokio::sync::Mutex<InflightMap>,
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
        let cache = DnsCache::with_capacity(config.cache_capacity);
        Self {
            servers,
            hosts,
            transport,
            cache,
            config,
            lkg: RwLock::new(HashMap::new()),
            inflight: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Returns a reference to the inner in-memory cache.
    pub fn cache(&self) -> &DnsCache {
        &self.cache
    }

    /// Resolves `host` into a list of IP addresses with zero heap allocations on cache hits.
    ///
    /// Resolution Order:
    /// 1. `DnsCache::get` (Positive Hit -> Return Ok; Negative Hit -> Return Err)
    /// 2. `HostsFileSource::lookup` (Hit -> Insert into DnsCache with `hosts_ttl` -> Return Ok)
    /// 3. In-flight Deduplication (Singleflight): Coalesces simultaneous requests for the same domain.
    /// 4. Upstream Nameserver Wire Query with Timeout & Failover.
    /// 5. Fallback to Last-Known-Good (LKG) or populate negative cache.
    pub async fn resolve_ips(&self, host: &str) -> Result<Arc<[IpAddr]>> {
        // -------------------------------------------------------------
        // Step 1: DnsCache is the Single Source of Truth on Hot Path
        // FIX (Optimization): Pass &str directly to DnsCache without allocating a lowercase String
        // -------------------------------------------------------------
        match self.cache.get(host) {
            CacheLookup::Hit(ips) => {
                return Ok(ips);
            }
            CacheLookup::NegativeHit => {
                return Err(DiscoveryError::NegativeCacheHit {
                    host: host.to_string(),
                });
            }
            CacheLookup::Miss => {}
        }

        let key = host.to_lowercase();

        // -------------------------------------------------------------
        // Step 2: Local Hosts File Lookup
        // -------------------------------------------------------------
        if let Some(ip) = self.hosts.lookup(&key) {
            let ips: Arc<[IpAddr]> = Arc::from([ip]);
            // Cache static hosts entry uniformly into DnsCache
            self.cache
                .insert_positive(&key, vec![ip], self.config.hosts_ttl);

            return Ok(ips);
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
                let outcome = self.execute_wire_query(host, &key).await;

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
                self.execute_wire_query(host, &key).await
            }
        }
    }

    /// Resolves `host` into a list of endpoints with `port`.
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<Endpoint>> {
        let ips = self.resolve_ips(host).await?;
        Ok(ips_to_endpoints(host, port, &ips))
    }

    /// Internal execution of wire queries against nameservers with timeouts and failover.
    async fn execute_wire_query(&self, host: &str, key: &str) -> Result<Arc<[IpAddr]>> {
        let server_list = self.servers.servers();
        if server_list.is_empty() {
            return Err(DiscoveryError::EmptyServerList);
        }

        let mut last_err = None;
        for &server in &server_list {
            // FIX (Blocker 4 - UDP Hang / Timeout): Wrap wire query with strict timeout to prevent indefinite hangs
            let query_future = self.transport.query(server, key);
            match tokio::time::timeout(self.config.query_timeout, query_future).await {
                Ok(Ok(ips_vec)) if !ips_vec.is_empty() => {
                    let ips: Arc<[IpAddr]> = Arc::from(ips_vec);

                    // Populate positive cache
                    self.cache
                        .insert_positive(key, ips.to_vec(), self.config.positive_ttl);

                    // Update LKG (Last-Known-Good) with bounded capacity
                    {
                        let mut lkg = self.lkg.write().unwrap();
                        if !lkg.contains_key(key)
                            && lkg.len() >= self.config.lkg_capacity
                            && let Some(first_key) = lkg.keys().next().cloned()
                        {
                            lkg.remove(&first_key);
                        }
                        lkg.insert(key.to_string(), Arc::clone(&ips));
                    }

                    return Ok(ips);
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
        if let Some(lkg_ips) = self.lkg.read().unwrap().get(key) {
            tracing::warn!(
                host = %host,
                "DNS lookup failed across all nameservers; falling back to Last-Known-Good (LKG) addresses"
            );
            // FIX (Resilience & Anti-Storm - Stale-If-Error RFC 5861):
            // Cache the Last-Known-Good addresses for a short grace period (`negative_ttl`).
            // This prevents thundering herds and endless 14-alloc query loops from hammering
            // dead upstream nameservers on every subsequent request during an outage!
            self.cache
                .insert_positive(key, lkg_ips.to_vec(), self.config.negative_ttl);
            return Ok(Arc::clone(lkg_ips));
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
fn ips_to_endpoints(host: &str, port: u16, ips: &[IpAddr]) -> Vec<Endpoint> {
    ips.iter()
        .map(|&ip| {
            let addr = SocketAddr::new(ip, port);
            Endpoint::new(host, addr, 1)
        })
        .collect()
}
