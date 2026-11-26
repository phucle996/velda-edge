//! Valkey standalone and cluster connection pools (`ValkeyNodePool`, `ValkeyClusterPool`).
//!
//! ```text
//!   ValkeyBranch::Standalone(Arc<ValkeyNodePool>)
//!   ValkeyBranch::Cluster(Arc<ValkeyClusterPool>) -> ArcSwap<ValkeyClusterTopology> + SlotMap
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::{Mutex, Semaphore};

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpPoolSettings;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTlsConfig, build_tls_target};
use crate::error::EtpError;
use crate::valkey::config::ValkeyEtpConfig;
use crate::valkey::lease::ValkeyLease;
use crate::valkey::slots::{SlotMap, fetch_cluster_slots, format_host_port, split_host_port};

/// Single-endpoint Valkey pool storing authenticated connections in [`EtpPageArena`].
pub struct ValkeyNodePool {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub db: u16,
    pub blueprint: ColdConnectBlueprint,
    prebaked_handshake: Option<Box<[u8]>>,
    handshake_replies: u8,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
}

impl fmt::Debug for ValkeyNodePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValkeyNodePool")
            .field("id", &self.id)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl ValkeyNodePool {
    pub fn from_config(config: &ValkeyEtpConfig) -> Result<Self, EtpError> {
        Self::from_endpoint(
            &config.id,
            &config.host,
            config.port,
            config.username.as_deref(),
            config.password.as_deref(),
            config.db,
            &config.pool,
            config.tls.as_ref(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_endpoint(
        id: &str,
        host: &str,
        port: u16,
        username: Option<&str>,
        password: Option<&str>,
        db: u16,
        pool_settings: &EtpPoolSettings,
        tls: Option<&EtpTlsConfig>,
    ) -> Result<Self, EtpError> {
        let tls_target = build_tls_target(id, host, tls)?;
        let connect_timeout = Duration::from_millis(pool_settings.connect_timeout_ms.max(1));
        let idle_timeout_secs = pool_settings.idle_timeout_ms.div_ceil(1000);
        let blueprint = ColdConnectBlueprint::new(id, host, port, tls_target, connect_timeout);

        let mut handshake = Vec::new();
        let mut handshake_replies: u8 = 0;
        if let Some(pw) = password {
            handshake.extend_from_slice(&build_auth_command(username, pw));
            handshake_replies += 1;
        }
        if db > 0 {
            handshake.extend_from_slice(&build_select_command(db));
            handshake_replies += 1;
        }
        let prebaked_handshake = if handshake.is_empty() {
            None
        } else {
            Some(handshake.into_boxed_slice())
        };

        let arena = EtpPageArena::new(pool_settings.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            id: id.to_string(),
            host: host.to_string(),
            port,
            db,
            blueprint,
            prebaked_handshake,
            handshake_replies,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    pub async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        let mut stream = self.blueprint.connect_stream().await?;
        let Some(ref frame) = self.prebaked_handshake else {
            return Ok(stream);
        };

        let handshake = async {
            stream.write_all(frame).await?;
            stream.flush().await?;
            for step in 0..self.handshake_replies {
                let reply = read_resp_line(&mut stream).await?;
                if !reply.starts_with("+OK") {
                    return Err(EtpError::Config(format!(
                        "Valkey '{}' handshake step {step} failed: {reply}",
                        self.id
                    )));
                }
            }
            Ok(stream)
        };

        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
        tokio::time::timeout(self.connect_timeout, handshake)
            .await
            .map_err(|_| EtpError::Timeout {
                id: self.id.clone(),
                timeout_ms,
            })?
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        let needed = self
            .arena
            .baseline_slots
            .saturating_sub(self.arena.baseline_active_count());
        if needed == 0 {
            return Ok(());
        }
        let mut permits = Vec::with_capacity(needed);
        for _ in 0..needed {
            match self.semaphore.try_acquire() {
                Ok(p) => permits.push(p),
                Err(_) => break,
            }
        }
        if permits.is_empty() {
            return Ok(());
        }
        let mut first_err: Option<EtpError> = None;
        for _ in 0..permits.len() {
            let Some(guard) = self.arena.claim_empty_slot(false) else {
                break;
            };
            match self.connect_authenticated().await {
                Ok(stream) => guard.commit_warm(stream),
                Err(err) => {
                    drop(guard);
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        drop(permits);
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<ValkeyLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(ValkeyLease::new(Arc::clone(self), slot_idx))
            }
            ReadyAcquire::Empty { discarded } => {
                drop(permit);
                if discarded {
                    self.trigger_replenish();
                }
                None
            }
        }
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<ValkeyLease, EtpError> {
        let permit =
            match tokio::time::timeout(self.connect_timeout, self.semaphore.acquire()).await {
                Ok(Ok(permit)) => permit,
                _ => {
                    return Err(EtpError::PoolExhausted {
                        id: self.id.clone(),
                    });
                }
            };
        if let ReadyAcquire::Hit(slot_idx) =
            self.arena.try_acquire_ready_slot(self.idle_timeout_secs)
        {
            permit.forget();
            return Ok(ValkeyLease::new(Arc::clone(self), slot_idx));
        }
        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.id.clone(),
            });
        };
        let stream = self.connect_authenticated().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(ValkeyLease::new(Arc::clone(self), slot_idx))
    }

    pub(crate) fn trigger_replenish(self: &Arc<Self>) {
        if !self.arena.try_begin_replenish() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.arena.abort_replenish();
            return;
        };
        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            loop {
                let Some(pool) = weak.upgrade() else {
                    break;
                };
                pool.arena.clear_replenish_pending();
                let deficit = pool
                    .arena
                    .baseline_slots
                    .saturating_sub(pool.arena.baseline_active_count());
                for _ in 0..deficit {
                    let Ok(permit) = pool.semaphore.try_acquire() else {
                        break;
                    };
                    let Some(guard) = pool.arena.claim_empty_slot(false) else {
                        drop(permit);
                        break;
                    };
                    match pool.connect_authenticated().await {
                        Ok(stream) => {
                            guard.commit_warm(stream);
                            drop(permit);
                        }
                        Err(err) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(id = %pool.id, error = %err, "Valkey replenish failed");
                            pool.arena.abort_replenish();
                            return;
                        }
                    }
                }
                if pool.arena.try_finish_replenish() {
                    break;
                }
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct ValkeyClusterNode {
    pub addr: String,
    pub pool: Arc<ValkeyNodePool>,
}

#[derive(Debug, Default)]
pub struct ValkeyClusterTopology {
    pub slot_map: SlotMap,
    pub nodes: Vec<ValkeyClusterNode>,
}

/// Multi-shard Valkey Cluster pool with `CLUSTER SLOTS` discovery and wait-free `ArcSwap`.
pub struct ValkeyClusterPool {
    pub config: ValkeyEtpConfig,
    topology: ArcSwap<ValkeyClusterTopology>,
    refresh_lock: Mutex<()>,
    rr_cursor: AtomicUsize,
}

impl fmt::Debug for ValkeyClusterPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topo = self.topology.load();
        f.debug_struct("ValkeyClusterPool")
            .field("id", &self.config.id)
            .field("node_count", &topo.nodes.len())
            .finish()
    }
}

impl ValkeyClusterPool {
    pub fn new(config: ValkeyEtpConfig) -> Self {
        Self {
            config,
            topology: ArcSwap::from_pointee(ValkeyClusterTopology::default()),
            refresh_lock: Mutex::new(()),
            rr_cursor: AtomicUsize::new(0),
        }
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        self.refresh_slots().await?;
        let topo = self.topology.load();
        for n in &topo.nodes {
            n.pool.warm().await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn ensure_initialized(&self) -> Result<(), EtpError> {
        if !self.topology.load().nodes.is_empty() {
            return Ok(());
        }
        self.refresh_slots().await
    }

    #[inline]
    pub fn node_for_slot(&self, slot: u16) -> Option<usize> {
        self.topology.load().slot_map.lookup(slot)
    }

    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<ValkeyLease, EtpError> {
        self.ensure_initialized().await?;
        let pool = self
            .topology
            .load()
            .nodes
            .get(node_idx)
            .map(|n| Arc::clone(&n.pool));
        match pool {
            Some(p) => p.acquire().await,
            None => self.acquire_any().await,
        }
    }

    pub async fn acquire_for_slot(&self, slot: u16) -> Result<ValkeyLease, EtpError> {
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
        let retry = {
            let topo = self.topology.load();
            topo.slot_map
                .lookup(slot)
                .and_then(|idx| topo.nodes.get(idx))
                .map(|n| Arc::clone(&n.pool))
                .or_else(|| topo.nodes.first().map(|n| Arc::clone(&n.pool)))
        };
        match retry {
            Some(p) => p.acquire().await,
            None => Err(EtpError::Config(format!(
                "Valkey Cluster '{}' has no available master nodes for slot {slot}",
                self.config.id
            ))),
        }
    }

    pub async fn acquire_any(&self) -> Result<ValkeyLease, EtpError> {
        self.ensure_initialized().await?;
        let pool = {
            let topo = self.topology.load();
            if topo.nodes.is_empty() {
                None
            } else {
                let idx = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % topo.nodes.len();
                Some(Arc::clone(&topo.nodes[idx].pool))
            }
        };
        match pool {
            Some(p) => p.acquire().await,
            None => Err(EtpError::Config(format!(
                "Valkey Cluster '{}' failed to discover master shards",
                self.config.id
            ))),
        }
    }

    pub fn try_acquire_any(&self) -> Option<ValkeyLease> {
        let topo = self.topology.load();
        if topo.nodes.is_empty() {
            return None;
        }
        let start = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % topo.nodes.len();
        for off in 0..topo.nodes.len() {
            if let Some(l) = topo.nodes[(start + off) % topo.nodes.len()]
                .pool
                .try_acquire()
            {
                return Some(l);
            }
        }
        None
    }

    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<ValkeyLease, EtpError> {
        let canonical = host_port.trim();
        if let Some(existing) = self
            .topology
            .load()
            .nodes
            .iter()
            .find(|n| n.addr == canonical)
            .map(|n| Arc::clone(&n.pool))
        {
            return existing.acquire().await;
        }
        let (host, port) = split_host_port(canonical, self.config.port, &self.config.id)?;
        let temp = Arc::new(self.build_node_pool(&host, port)?);
        temp.acquire().await
    }

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
                let pool = Arc::new(self.build_node_pool(&host, port)?);
                let idx = cloned.len();
                cloned.push(ValkeyClusterNode {
                    addr: canonical.to_string(),
                    pool,
                });
                (cloned, idx)
            };
        let target_u16 = u16::try_from(target_idx).map_err(|_| {
            EtpError::Config(format!(
                "Valkey Cluster '{}' exceeded u16 node count limit",
                self.config.id
            ))
        })?;
        let next_slot_map = current
            .slot_map
            .with_moved_slot(slot, target_u16, next_nodes.len());
        self.topology.store(Arc::new(ValkeyClusterTopology {
            slot_map: next_slot_map,
            nodes: std::mem::take(&mut next_nodes),
        }));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(self);
            handle.spawn(async move {
                if let Some(c) = weak.upgrade() {
                    let _ = c.refresh_slots().await;
                }
            });
        }
        Ok(target_idx)
    }

    pub async fn refresh_slots(&self) -> Result<(), EtpError> {
        let _guard = self.refresh_lock.lock().await;
        let mut candidates = self.config.seed_nodes.clone();
        for n in &self.topology.load().nodes {
            if !candidates.contains(&n.addr) {
                candidates.push(n.addr.clone());
            }
        }
        let mut last_err = None;
        let mut discovered = None;
        for addr in &candidates {
            let Ok((host, port)) = split_host_port(addr, self.config.port, &self.config.id) else {
                continue;
            };
            let seed = match self.build_node_pool(&host, port) {
                Ok(p) => p,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
            match seed.connect_authenticated().await {
                Ok(mut s) => match fetch_cluster_slots(&mut s, &host, &self.config.id).await {
                    Ok(r) => {
                        discovered = Some(r);
                        break;
                    }
                    Err(e) => last_err = Some(e),
                },
                Err(e) => last_err = Some(e),
            }
        }

        let ranges = discovered.ok_or_else(|| {
            last_err.unwrap_or_else(|| {
                EtpError::Config(format!(
                    "Valkey Cluster '{}' failed to query CLUSTER SLOTS",
                    self.config.id
                ))
            })
        })?;

        let existing = self.topology.load();
        let existing_map: HashMap<&str, &Arc<ValkeyNodePool>> = existing
            .nodes
            .iter()
            .map(|n| (n.addr.as_str(), &n.pool))
            .collect();
        let mut nodes = Vec::new();
        let mut node_idx_map = HashMap::new();
        let mut raw_slots = Vec::with_capacity(ranges.len());

        for span in ranges {
            let addr = format_host_port(&span.host, span.port);
            let idx = if let Some(&i) = node_idx_map.get(&addr) {
                i
            } else {
                let pool = if let Some(&reused) = existing_map.get(addr.as_str()) {
                    Arc::clone(reused)
                } else {
                    Arc::new(self.build_node_pool(&span.host, span.port)?)
                };
                let new_idx = u16::try_from(nodes.len()).map_err(|_| {
                    EtpError::Config(format!(
                        "Valkey Cluster '{}' exceeded u16 shard count",
                        self.config.id
                    ))
                })?;
                nodes.push(ValkeyClusterNode {
                    addr: addr.clone(),
                    pool,
                });
                node_idx_map.insert(addr, new_idx);
                new_idx
            };
            raw_slots.push((span.start, span.end, idx));
        }

        let slot_map = SlotMap::from_ranges(raw_slots, nodes.len());
        self.topology
            .store(Arc::new(ValkeyClusterTopology { slot_map, nodes }));
        Ok(())
    }

    fn build_node_pool(&self, host: &str, port: u16) -> Result<ValkeyNodePool, EtpError> {
        ValkeyNodePool::from_endpoint(
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

async fn read_resp_line(stream: &mut EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Valkey closed connection before completing RESP line",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 16 * 1024 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Valkey RESP line exceeded 16 KiB limit",
            )));
        }
    }
}

fn build_auth_command(username: Option<&str>, password: &str) -> Vec<u8> {
    match username {
        Some(user) => format!(
            "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            user.len(),
            user,
            password.len(),
            password
        )
        .into_bytes(),
        None => format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
            password.len(),
            password
        )
        .into_bytes(),
    }
}

fn build_select_command(db: u16) -> Vec<u8> {
    let db_str = db.to_string();
    format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).into_bytes()
}
