//! Redis Cluster multi-shard pool (`RedisClusterPool`) managing `CLUSTER SLOTS` discovery,
//! wait-free [`ArcSwap`] [`ClusterTopology`] swaps, per-Master [`RedisStandalonePool`] arenas,
//! and self-healing `-MOVED` / `-ASK` node resolution.
//!
//! ```text
//!   RedisClusterPool
//!     ├── config: RedisEtpConfig
//!     └── topology: ArcSwap<ClusterTopology>
//!           ├── slot_map: SlotMap (Ranges <= 64 | #[cold] Dense4Bit / DenseU8)
//!           └── nodes:    Vec<ClusterNode { addr: String, pool: Arc<RedisStandalonePool> }>
//!
//!   Warm / Lazy Init (refresh_lock prevents concurrent stampedes):
//!     seed_nodes[i] ──► connect + AUTH ──► CLUSTER SLOTS
//!       └──► reuse unchanged Master node pools, spin up new ones, build SlotMap, store ArcSwap
//!
//!   -MOVED <slot> <host:port> Feedback from Plugin:
//!     report_moved(slot, addr) ──► ensure Node Pool exists -> patch SlotMap -> spawn background refresh
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use crate::error::EtpError;
use crate::redis::cluster::slots::{SlotMap, fetch_cluster_slots};
use crate::redis::config::RedisEtpConfig;
use crate::redis::lease::RedisLease;
use crate::redis::standalone::RedisStandalonePool;

/// Resolved node pool entry keyed by canonical `host:port`.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub addr: String,
    pub pool: Arc<RedisStandalonePool>,
}

/// Snapshot-immutable cluster slot mapping and node pool vector swapped atomically.
#[derive(Debug, Default)]
pub struct ClusterTopology {
    pub slot_map: SlotMap,
    pub nodes: Vec<ClusterNode>,
}

/// Dynamic Master-shard connection manager for Redis Cluster (`mode == "cluster"`).
pub struct RedisClusterPool {
    pub config: RedisEtpConfig,
    topology: ArcSwap<ClusterTopology>,
    refresh_lock: Mutex<()>,
    rr_cursor: AtomicUsize,
}

impl fmt::Debug for RedisClusterPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topo = self.topology.load();
        f.debug_struct("RedisClusterPool")
            .field("id", &self.config.id)
            .field("seed_nodes", &self.config.seed_nodes)
            .field("node_count", &topo.nodes.len())
            .finish()
    }
}

impl RedisClusterPool {
    /// Creates an empty [`RedisClusterPool`] ready for `warm()` or lazy bootstrap.
    pub fn new(config: RedisEtpConfig) -> Result<Self, EtpError> {
        Ok(Self {
            config,
            topology: ArcSwap::from_pointee(ClusterTopology::default()),
            refresh_lock: Mutex::new(()),
            rr_cursor: AtomicUsize::new(0),
        })
    }

    /// Discovers Master shards from `seed_nodes` via `CLUSTER SLOTS` and pre-warms
    /// baseline connections across every Master node pool.
    pub async fn warm(&self) -> Result<(), EtpError> {
        self.refresh_slots().await?;
        let topo = self.topology.load();
        for node in &topo.nodes {
            node.pool.warm().await?;
        }
        Ok(())
    }

    /// Ensures topology has been bootstrapped from `seed_nodes` at least once.
    #[inline]
    pub async fn ensure_initialized(&self) -> Result<(), EtpError> {
        if !self.topology.load().nodes.is_empty() {
            return Ok(());
        }
        self.refresh_slots().await
    }

    /// Wait-free O(1) lookup of the Master node index currently owning `slot` (`0..=16383`).
    #[inline]
    pub fn node_for_slot(&self, slot: u16) -> Option<usize> {
        self.topology.load().slot_map.lookup(slot)
    }

    /// Borrows a [`RedisLease`] targeting a specific discovered Master `node_idx`.
    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<RedisLease, EtpError> {
        self.ensure_initialized().await?;
        let pool = {
            let topo = self.topology.load();
            topo.nodes.get(node_idx).map(|n| Arc::clone(&n.pool))
        };
        match pool {
            Some(p) => p.acquire().await,
            None => self.acquire_any().await,
        }
    }

    /// Borrows a [`RedisLease`] targeting whichever Master currently owns `slot`.
    pub async fn acquire_for_slot(&self, slot: u16) -> Result<RedisLease, EtpError> {
        self.ensure_initialized().await?;
        let pool = {
            let topo = self.topology.load();
            topo.slot_map
                .lookup(slot)
                .and_then(|idx| topo.nodes.get(idx))
                .map(|n| Arc::clone(&n.pool))
        };
        if let Some(p) = pool {
            return p.acquire().await;
        }
        self.refresh_slots().await?;
        let retry_pool = {
            let topo = self.topology.load();
            topo.slot_map
                .lookup(slot)
                .and_then(|idx| topo.nodes.get(idx))
                .map(|n| Arc::clone(&n.pool))
                .or_else(|| topo.nodes.first().map(|n| Arc::clone(&n.pool)))
        };
        match retry_pool {
            Some(p) => p.acquire().await,
            None => Err(EtpError::Config(format!(
                "Redis Cluster '{}' has no available master nodes for slot {slot}",
                self.config.id
            ))),
        }
    }

    /// Acquires a round-robin [`RedisLease`] across active Master shards.
    pub async fn acquire_any(&self) -> Result<RedisLease, EtpError> {
        self.ensure_initialized().await?;
        let pool = {
            let topo = self.topology.load();
            let len = topo.nodes.len();
            if len == 0 {
                None
            } else {
                let idx = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % len;
                Some(Arc::clone(&topo.nodes[idx].pool))
            }
        };
        match pool {
            Some(p) => p.acquire().await,
            None => Err(EtpError::Config(format!(
                "Redis Cluster '{}' failed to discover master shards",
                self.config.id
            ))),
        }
    }

    /// Non-blocking lease attempt across active Master shards.
    pub fn try_acquire_any(&self) -> Option<RedisLease> {
        let topo = self.topology.load();
        let len = topo.nodes.len();
        if len == 0 {
            return None;
        }
        let start = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % len;
        for off in 0..len {
            if let Some(lease) = topo.nodes[(start + off) % len].pool.try_acquire() {
                return Some(lease);
            }
        }
        None
    }

    /// Returns a [`RedisLease`] directly for a target `host:port` (used for `-ASK` redirects
    /// without altering the canonical `SlotMap`).
    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<RedisLease, EtpError> {
        let canonical = host_port.trim();
        if let Some(existing) = {
            let topo = self.topology.load();
            topo.nodes
                .iter()
                .find(|n| n.addr == canonical)
                .map(|n| Arc::clone(&n.pool))
        } {
            return existing.acquire().await;
        }

        let (host, port) = split_host_port(canonical, self.config.port, &self.config.id)?;
        let temp_pool = Arc::new(self.build_node_pool(&host, port)?);
        temp_pool.acquire().await
    }

    /// Called by plugins upon encountering `-MOVED <slot> <host:port>`.
    /// Registers the target node pool if new, updates `slot -> target_idx` immediately, and
    /// schedules a background `CLUSTER SLOTS` coalescing refresh.
    pub fn report_moved(
        self: &Arc<Self>,
        slot: u16,
        target_host_port: &str,
    ) -> Result<usize, EtpError> {
        let canonical = target_host_port.trim();
        let (host, port) = split_host_port(canonical, self.config.port, &self.config.id)?;
        let current = self.topology.load();

        let (mut next_nodes, target_idx) =
            if let Some(idx) = current.nodes.iter().position(|n| n.addr == canonical) {
                (current.nodes.clone(), idx)
            } else {
                let mut cloned = current.nodes.clone();
                let new_pool = Arc::new(self.build_node_pool(&host, port)?);
                let idx = cloned.len();
                cloned.push(ClusterNode {
                    addr: canonical.to_string(),
                    pool: new_pool,
                });
                (cloned, idx)
            };

        let target_u16 = u16::try_from(target_idx).map_err(|_| {
            EtpError::Config(format!(
                "Redis Cluster '{}' exceeded u16 node count limit",
                self.config.id
            ))
        })?;
        let next_slot_map = current
            .slot_map
            .with_moved_slot(slot, target_u16, next_nodes.len());
        let swapped = ClusterTopology {
            slot_map: next_slot_map,
            nodes: std::mem::take(&mut next_nodes),
        };
        self.topology.store(Arc::new(swapped));

        // Coalesce fragmented MOVED overrides back into contiguous Ranges asynchronously.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(self);
            handle.spawn(async move {
                if let Some(cluster) = weak.upgrade() {
                    let _ = cluster.refresh_slots().await;
                }
            });
        }

        Ok(target_idx)
    }

    /// Connects to seed nodes (or known nodes), executes `CLUSTER SLOTS`, and atomically
    /// publishes updated Master node pools + compacted [`SlotMap`].
    pub async fn refresh_slots(&self) -> Result<(), EtpError> {
        let _guard = self.refresh_lock.lock().await;

        let mut candidates: Vec<String> = self.config.seed_nodes.clone();
        {
            let existing = self.topology.load();
            for n in &existing.nodes {
                if !candidates.contains(&n.addr) {
                    candidates.push(n.addr.clone());
                }
            }
        }

        let mut last_err: Option<EtpError> = None;
        let mut discovered = None;
        for addr in &candidates {
            let Ok((host, port)) = split_host_port(addr, self.config.port, &self.config.id) else {
                continue;
            };
            let seed_pool = match self.build_node_pool(&host, port) {
                Ok(p) => p,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
            match seed_pool.connect_authenticated().await {
                Ok(mut stream) => {
                    match fetch_cluster_slots(&mut stream, &host, &self.config.id).await {
                        Ok(ranges) => {
                            discovered = Some(ranges);
                            break;
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                Err(e) => last_err = Some(e),
            }
        }

        let ranges = match discovered {
            Some(r) => r,
            None => {
                return Err(last_err.unwrap_or_else(|| {
                    EtpError::Config(format!(
                        "Redis Cluster '{}' could not query CLUSTER SLOTS from any seed node",
                        self.config.id
                    ))
                }));
            }
        };

        let existing = self.topology.load();
        let existing_by_addr: HashMap<&str, &Arc<RedisStandalonePool>> = existing
            .nodes
            .iter()
            .map(|n| (n.addr.as_str(), &n.pool))
            .collect();

        let mut nodes: Vec<ClusterNode> = Vec::new();
        let mut node_index_map: HashMap<String, u16> = HashMap::new();
        let mut raw_slots: Vec<(u16, u16, u16)> = Vec::with_capacity(ranges.len());

        for span in ranges {
            let addr = format_host_port(&span.host, span.port);
            let idx = if let Some(&existing_idx) = node_index_map.get(&addr) {
                existing_idx
            } else {
                let pool = if let Some(&reused) = existing_by_addr.get(addr.as_str()) {
                    Arc::clone(reused)
                } else {
                    Arc::new(self.build_node_pool(&span.host, span.port)?)
                };
                let new_idx = u16::try_from(nodes.len()).map_err(|_| {
                    EtpError::Config(format!(
                        "Redis Cluster '{}' exceeded u16 master shard count",
                        self.config.id
                    ))
                })?;
                nodes.push(ClusterNode {
                    addr: addr.clone(),
                    pool,
                });
                node_index_map.insert(addr, new_idx);
                new_idx
            };
            raw_slots.push((span.start, span.end, idx));
        }

        let slot_map = SlotMap::from_ranges(raw_slots, nodes.len());
        self.topology
            .store(Arc::new(ClusterTopology { slot_map, nodes }));
        Ok(())
    }

    fn build_node_pool(&self, host: &str, port: u16) -> Result<RedisStandalonePool, EtpError> {
        // Redis Cluster nodes strictly operate on db 0.
        RedisStandalonePool::from_endpoint(
            &self.config.id,
            host,
            port,
            self.config.username.as_deref(),
            self.config.password.as_deref(),
            0,
            &self.config.pool,
            self.config.tls.as_ref(),
        )
    }
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn split_host_port(raw: &str, default_port: u16, id: &str) -> Result<(String, u16), EtpError> {
    let trimmed = raw.trim();
    if let Some(after) = trimmed.strip_prefix('[') {
        let (host, rest) = after.split_once(']').ok_or_else(|| {
            EtpError::Config(format!(
                "Redis Cluster '{id}' invalid IPv6 node '{trimmed}'"
            ))
        })?;
        let port = if let Some(p) = rest.strip_prefix(':') {
            p.parse::<u16>().map_err(|_| {
                EtpError::Config(format!("Redis Cluster '{id}' invalid port in '{trimmed}'"))
            })?
        } else {
            default_port
        };
        return Ok((host.to_string(), port));
    }

    if let Some((host, p)) = trimmed.rsplit_once(':')
        && !host.contains(':')
    {
        let port = p.parse::<u16>().map_err(|_| {
            EtpError::Config(format!("Redis Cluster '{id}' invalid port in '{trimmed}'"))
        })?;
        return Ok((host.to_string(), port));
    }

    Ok((trimmed.to_string(), default_port))
}
