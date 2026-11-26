//! GCP Memorystore (`velda-etp::gcp_memorystore`) connection pools (`GcpMemorystoreNodePool` &
//! `GcpMemorystoreClusterPool`) with cached OAuth2 / GCE Metadata / Service Account RS256 JWT
//! access-token injection (`AUTH <username> <access_token>`).
//!
//! ```text
//!   connect_authenticated():
//!     1. ColdConnectBlueprint::connect_stream() ──► TLS/TCP stream to Memorystore node
//!     2. GcpTokenProvider::get_token() (shared across Standalone & all Cluster shards):
//!          ├─ Cache hit (expires_at > now + 30s)? ──► return cached OAuth2 bearer token
//!          └─ Cache miss:
//!               ├─ MetadataEndpoint: GET <url> + Header "Metadata-Flavor: Google"
//!               └─ ServiceAccount:   RS256 JWT assertion ──► POST <token_uri> (x-www-form-urlencoded)
//!               └──► Parse JSON { access_token, expires_in } ──► cache & return
//!     3. Send RESP2 AUTH <username> <access_token> (+ optional SELECT <db>) ──► "+OK"
//! ```

use std::collections::HashMap;
use std::fmt;
use std::io::BufReader;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde_json::json;
use tokio::sync::{Mutex, Semaphore};

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpPoolSettings;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTlsConfig, build_tls_target};
use crate::error::EtpError;
use crate::gcp_memorystore::config::{
    DEFAULT_GCP_SCOPE, GcpIamAuthSource, GcpMemorystoreEtpConfig,
};
use crate::gcp_memorystore::lease::GcpMemorystoreLease;
use crate::gcp_memorystore::slots::{
    SlotMap, fetch_cluster_slots, format_host_port, split_host_port,
};

#[derive(Clone, Debug)]
struct CachedGcpToken {
    token: String,
    expires_at_unix: u64,
}

enum ResolvedGcpSource {
    Metadata {
        host_header: String,
        path: String,
        blueprint: ColdConnectBlueprint,
    },
    ServiceAccount {
        client_email: String,
        token_uri: String,
        host_header: String,
        path: String,
        key_pair: Arc<RsaKeyPair>,
        rng: SystemRandom,
        blueprint: ColdConnectBlueprint,
    },
}

/// Shared, TTL-cached GCP OAuth2 IAM token provider reused across all cluster shard pools.
pub struct GcpTokenProvider {
    id: String,
    username: String,
    timeout: Duration,
    source: ResolvedGcpSource,
    cache: Mutex<Option<CachedGcpToken>>,
}

impl fmt::Debug for GcpTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpTokenProvider")
            .field("id", &self.id)
            .field("username", &self.username)
            .finish()
    }
}

impl GcpTokenProvider {
    pub fn new(
        id: &str,
        username: String,
        auth_source: &GcpIamAuthSource,
        tls_override: Option<&EtpTlsConfig>,
        timeout: Duration,
    ) -> Result<Self, EtpError> {
        let source = match auth_source {
            GcpIamAuthSource::MetadataEndpoint { endpoint } => {
                let (host, port, path, use_tls) = parse_http_url(id, endpoint)?;
                let tls_cfg = if use_tls {
                    Some(tls_override.cloned().unwrap_or_default())
                } else {
                    None
                };
                let tls_target = build_tls_target(id, &host, tls_cfg.as_ref())?;
                let blueprint = ColdConnectBlueprint::new(id, &host, port, tls_target, timeout);
                ResolvedGcpSource::Metadata {
                    host_header: format_http_host_header(&host, port, use_tls),
                    path,
                    blueprint,
                }
            }
            GcpIamAuthSource::ServiceAccount {
                client_email,
                private_key_pem,
                token_uri,
            } => {
                let mut reader = BufReader::new(private_key_pem.as_bytes());
                let key_der = rustls_pemfile::private_key(&mut reader)
                    .map_err(|e| {
                        EtpError::Config(format!(
                            "GcpMemorystore ETP '{id}' invalid Service Account PEM: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        EtpError::Config(format!(
                            "GcpMemorystore ETP '{id}' private_key_pem contains no RSA private key"
                        ))
                    })?;
                let key_pair = RsaKeyPair::from_pkcs8(key_der.secret_der()).map_err(|e| {
                    EtpError::Config(format!(
                        "GcpMemorystore ETP '{id}' failed to parse PKCS8 RSA private key: {e}"
                    ))
                })?;
                let (host, port, path, use_tls) = parse_http_url(id, token_uri)?;
                let tls_cfg = if use_tls {
                    Some(tls_override.cloned().unwrap_or_default())
                } else {
                    None
                };
                let tls_target = build_tls_target(id, &host, tls_cfg.as_ref())?;
                let blueprint = ColdConnectBlueprint::new(id, &host, port, tls_target, timeout);
                ResolvedGcpSource::ServiceAccount {
                    client_email: client_email.clone(),
                    token_uri: token_uri.clone(),
                    host_header: format_http_host_header(&host, port, use_tls),
                    path,
                    key_pair: Arc::new(key_pair),
                    rng: SystemRandom::new(),
                    blueprint,
                }
            }
        };

        Ok(Self {
            id: id.to_string(),
            username,
            timeout,
            source,
            cache: Mutex::new(None),
        })
    }

    pub async fn get_token(&self) -> Result<String, EtpError> {
        let now = now_unix_secs();
        let mut guard = self.cache.lock().await;
        if let Some(ref cached) = *guard
            && cached.expires_at_unix > now.saturating_add(30)
        {
            return Ok(cached.token.clone());
        }

        let fresh = self.fetch_fresh_token(now).await?;
        let token = fresh.token.clone();
        *guard = Some(fresh);
        Ok(token)
    }

    async fn fetch_fresh_token(&self, now: u64) -> Result<CachedGcpToken, EtpError> {
        let (stream, req_bytes) = match &self.source {
            ResolvedGcpSource::Metadata {
                host_header,
                path,
                blueprint,
            } => {
                let s = blueprint.connect_stream().await?;
                let req = format!(
                    "GET {path} HTTP/1.1\r\nHost: {host_header}\r\nMetadata-Flavor: Google\r\nConnection: close\r\n\r\n"
                );
                (s, req.into_bytes())
            }
            ResolvedGcpSource::ServiceAccount {
                client_email,
                token_uri,
                host_header,
                path,
                key_pair,
                rng,
                blueprint,
            } => {
                let jwt = sign_rs256_jwt(client_email, token_uri, now, key_pair, rng, &self.id)?;
                let body = format!(
                    "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={jwt}"
                );
                let s = blueprint.connect_stream().await?;
                let req = format!(
                    "POST {path} HTTP/1.1\r\nHost: {host_header}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                (s, req.into_bytes())
            }
        };

        let mut stream = stream;
        let timeout_ms = u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX);
        let body_bytes = tokio::time::timeout(self.timeout, async move {
            stream.write_all(&req_bytes).await?;
            stream.flush().await?;
            read_http_response_body(&mut stream).await
        })
        .await
        .map_err(|_| EtpError::Timeout {
            id: self.id.clone(),
            timeout_ms,
        })??;

        let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).map_err(|e| {
            EtpError::Config(format!(
                "GcpMemorystore '{}' IAM token JSON parse failed: {e}",
                self.id
            ))
        })?;
        let token = parsed
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EtpError::Config(format!(
                    "GcpMemorystore '{}' IAM token response missing 'access_token': {parsed}",
                    self.id
                ))
            })?
            .to_string();
        let expires_in = parsed
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600)
            .max(60);
        Ok(CachedGcpToken {
            token,
            expires_at_unix: now.saturating_add(expires_in),
        })
    }
}

pub struct GcpMemorystoreNodePool {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub db: u16,
    pub blueprint: ColdConnectBlueprint,
    token_provider: Arc<GcpTokenProvider>,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
}

impl fmt::Debug for GcpMemorystoreNodePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpMemorystoreNodePool")
            .field("id", &self.id)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl GcpMemorystoreNodePool {
    pub fn from_config(
        config: &GcpMemorystoreEtpConfig,
        token_provider: Arc<GcpTokenProvider>,
    ) -> Result<Self, EtpError> {
        Self::from_endpoint(
            &config.id,
            &config.host,
            config.port,
            config.db,
            token_provider,
            &config.pool,
            config.tls.as_ref(),
        )
    }

    pub fn from_endpoint(
        id: &str,
        host: &str,
        port: u16,
        db: u16,
        token_provider: Arc<GcpTokenProvider>,
        pool_settings: &EtpPoolSettings,
        tls: Option<&EtpTlsConfig>,
    ) -> Result<Self, EtpError> {
        let tls_target = build_tls_target(id, host, tls)?;
        let connect_timeout = Duration::from_millis(pool_settings.connect_timeout_ms.max(1));
        let idle_timeout_secs = pool_settings.idle_timeout_ms.div_ceil(1000);
        let blueprint = ColdConnectBlueprint::new(id, host, port, tls_target, connect_timeout);
        let arena = EtpPageArena::new(pool_settings.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            id: id.to_string(),
            host: host.to_string(),
            port,
            db,
            blueprint,
            token_provider,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    pub async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        let token = self.token_provider.get_token().await?;
        let mut stream = self.blueprint.connect_stream().await?;
        let mut frame = format!(
            "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{token}\r\n",
            self.token_provider.username.len(),
            self.token_provider.username,
            token.len()
        )
        .into_bytes();
        let mut replies_expected = 1u8;
        if self.db > 0 {
            let db_s = self.db.to_string();
            frame.extend_from_slice(
                format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{db_s}\r\n", db_s.len()).as_bytes(),
            );
            replies_expected = 2;
        }

        let handshake = async {
            stream.write_all(&frame).await?;
            stream.flush().await?;
            for step in 0..replies_expected {
                let reply = read_resp_line(&mut stream).await?;
                if !reply.starts_with("+OK") {
                    return Err(EtpError::Config(format!(
                        "GcpMemorystore '{}' IAM AUTH step {step} failed: {reply}",
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
                Ok(s) => guard.commit_warm(s),
                Err(e) => {
                    drop(guard);
                    if first_err.is_none() {
                        first_err = Some(e);
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

    pub fn try_acquire(self: &Arc<Self>) -> Option<GcpMemorystoreLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(idx) => {
                permit.forget();
                Some(GcpMemorystoreLease::new(Arc::clone(self), idx))
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

    pub async fn acquire(self: &Arc<Self>) -> Result<GcpMemorystoreLease, EtpError> {
        let permit =
            match tokio::time::timeout(self.connect_timeout, self.semaphore.acquire()).await {
                Ok(Ok(p)) => p,
                _ => {
                    return Err(EtpError::PoolExhausted {
                        id: self.id.clone(),
                    });
                }
            };
        if let ReadyAcquire::Hit(idx) = self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            permit.forget();
            return Ok(GcpMemorystoreLease::new(Arc::clone(self), idx));
        }
        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.id.clone(),
            });
        };
        let s = self.connect_authenticated().await?;
        let idx = guard.commit_leased(s);
        permit.forget();
        Ok(GcpMemorystoreLease::new(Arc::clone(self), idx))
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
                let Some(pool) = weak.upgrade() else { break };
                pool.arena.clear_replenish_pending();
                let deficit = pool
                    .arena
                    .baseline_slots
                    .saturating_sub(pool.arena.baseline_active_count());
                for _ in 0..deficit {
                    let Ok(permit) = pool.semaphore.try_acquire() else { break };
                    let Some(guard) = pool.arena.claim_empty_slot(false) else {
                        drop(permit);
                        break;
                    };
                    match pool.connect_authenticated().await {
                        Ok(s) => {
                            guard.commit_warm(s);
                            drop(permit);
                        }
                        Err(e) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(id = %pool.id, error = %e, "GcpMemorystore replenish failed");
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
pub struct GcpClusterNode {
    pub addr: String,
    pub pool: Arc<GcpMemorystoreNodePool>,
}

#[derive(Debug, Default)]
pub struct GcpClusterTopology {
    pub slot_map: SlotMap,
    pub nodes: Vec<GcpClusterNode>,
}

pub struct GcpMemorystoreClusterPool {
    pub config: GcpMemorystoreEtpConfig,
    token_provider: Arc<GcpTokenProvider>,
    topology: ArcSwap<GcpClusterTopology>,
    refresh_lock: Mutex<()>,
    rr_cursor: AtomicUsize,
}

impl fmt::Debug for GcpMemorystoreClusterPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpMemorystoreClusterPool")
            .field("id", &self.config.id)
            .field("node_count", &self.topology.load().nodes.len())
            .finish()
    }
}

impl GcpMemorystoreClusterPool {
    pub fn new(config: GcpMemorystoreEtpConfig, token_provider: Arc<GcpTokenProvider>) -> Self {
        Self {
            config,
            token_provider,
            topology: ArcSwap::from_pointee(GcpClusterTopology::default()),
            refresh_lock: Mutex::new(()),
            rr_cursor: AtomicUsize::new(0),
        }
    }

    pub async fn warm(&self) -> Result<(), EtpError> {
        self.refresh_slots().await?;
        for n in &self.topology.load().nodes {
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

    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<GcpMemorystoreLease, EtpError> {
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

    pub async fn acquire_for_slot(&self, slot: u16) -> Result<GcpMemorystoreLease, EtpError> {
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
                "GcpMemorystore Cluster '{}' has no master nodes for slot {slot}",
                self.config.id
            ))),
        }
    }

    pub async fn acquire_any(&self) -> Result<GcpMemorystoreLease, EtpError> {
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
                "GcpMemorystore Cluster '{}' failed slot discovery",
                self.config.id
            ))),
        }
    }

    pub fn try_acquire_any(&self) -> Option<GcpMemorystoreLease> {
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

    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<GcpMemorystoreLease, EtpError> {
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
        Arc::new(self.build_node_pool(&host, port)?).acquire().await
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
                cloned.push(GcpClusterNode {
                    addr: canonical.to_string(),
                    pool,
                });
                (cloned, idx)
            };
        let target_u16 = u16::try_from(target_idx).map_err(|_| {
            EtpError::Config(format!(
                "GcpMemorystore Cluster '{}' exceeded u16 node count limit",
                self.config.id
            ))
        })?;
        let next_slot_map = current
            .slot_map
            .with_moved_slot(slot, target_u16, next_nodes.len());
        self.topology.store(Arc::new(GcpClusterTopology {
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
                    "GcpMemorystore Cluster '{}' failed to query CLUSTER SLOTS",
                    self.config.id
                ))
            })
        })?;

        let existing = self.topology.load();
        let existing_map: HashMap<&str, &Arc<GcpMemorystoreNodePool>> = existing
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
                        "GcpMemorystore Cluster '{}' exceeded u16 shard count",
                        self.config.id
                    ))
                })?;
                nodes.push(GcpClusterNode {
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
            .store(Arc::new(GcpClusterTopology { slot_map, nodes }));
        Ok(())
    }

    fn build_node_pool(&self, host: &str, port: u16) -> Result<GcpMemorystoreNodePool, EtpError> {
        GcpMemorystoreNodePool::from_endpoint(
            &self.config.id,
            host,
            port,
            0,
            Arc::clone(&self.token_provider),
            &self.config.pool,
            self.config.tls.as_ref(),
        )
    }
}

fn sign_rs256_jwt(
    client_email: &str,
    token_uri: &str,
    now_unix: u64,
    key_pair: &RsaKeyPair,
    rng: &SystemRandom,
    id: &str,
) -> Result<String, EtpError> {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#.as_bytes());
    let claims = json!({
        "iss": client_email,
        "scope": DEFAULT_GCP_SCOPE,
        "aud": token_uri,
        "iat": now_unix,
        "exp": now_unix + 3600,
    })
    .to_string();
    let payload = URL_SAFE_NO_PAD.encode(claims.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let mut sig = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(&RSA_PKCS1_SHA256, rng, signing_input.as_bytes(), &mut sig)
        .map_err(|_| {
            EtpError::Config(format!(
                "GcpMemorystore '{id}' RS256 JWT signature generation failed"
            ))
        })?;
    let sig_b64 = URL_SAFE_NO_PAD.encode(&sig);
    Ok(format!("{signing_input}.{sig_b64}"))
}

fn parse_http_url(id: &str, raw: &str) -> Result<(String, u16, String, bool), EtpError> {
    let trimmed = raw.trim();
    let (use_tls, rest) = if let Some(r) = trimmed.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = trimmed.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(EtpError::Config(format!(
            "GcpMemorystore '{id}' token URL '{trimmed}' must start with http:// or https://"
        )));
    };
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let default_port = if use_tls { 443 } else { 80 };
    let (host, port) = split_host_port(authority, default_port, id)?;
    Ok((host, port, path, use_tls))
}

fn format_http_host_header(host: &str, port: u16, use_tls: bool) -> String {
    let default_port = if use_tls { 443 } else { 80 };
    let formatted_host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    if port == default_port {
        formatted_host
    } else {
        format!("{formatted_host}:{port}")
    }
}

async fn read_http_response_body(stream: &mut EtpStream) -> Result<Vec<u8>, EtpError> {
    let mut raw = Vec::with_capacity(1024);
    let mut tmp = [0u8; 2048];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&tmp[..n]);
        if raw.len() > 256 * 1024 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "GCP IAM HTTP response exceeded 256 KiB",
            )));
        }
    }
    let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Err(EtpError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "GCP IAM HTTP response missing header terminator",
        )));
    };
    let head = String::from_utf8_lossy(&raw[..split]);
    let status_ok = head
        .lines()
        .next()
        .is_some_and(|l| l.contains(" 200 ") || l.contains(" 201 "));
    let body = &raw[split + 4..];
    if !status_ok {
        return Err(EtpError::Config(format!(
            "GCP IAM token endpoint rejected request ({}) body={}",
            head.lines().next().unwrap_or(""),
            String::from_utf8_lossy(body)
        )));
    }
    Ok(body.to_vec())
}

#[inline(always)]
fn now_unix_secs() -> u64 {
    velda_core::clock::coarse_sec()
}

async fn read_resp_line(stream: &mut EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "GcpMemorystore closed connection during IAM AUTH",
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
                "GcpMemorystore RESP line exceeded 16 KiB",
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_http_url_variants() {
        let (h, p, path, tls) =
            parse_http_url("gcp-test", "http://127.0.0.1:8085/computeMetadata/token").unwrap();
        assert_eq!(h, "127.0.0.1");
        assert_eq!(p, 8085);
        assert_eq!(path, "/computeMetadata/token");
        assert!(!tls);

        let (h2, p2, path2, tls2) =
            parse_http_url("gcp-test", "https://oauth2.googleapis.com/token").unwrap();
        assert_eq!(h2, "oauth2.googleapis.com");
        assert_eq!(p2, 443);
        assert_eq!(path2, "/token");
        assert!(tls2);
    }

    #[test]
    fn test_format_http_host_header_port_elision() {
        assert_eq!(
            format_http_host_header("oauth2.googleapis.com", 443, true),
            "oauth2.googleapis.com"
        );
        assert_eq!(
            format_http_host_header("127.0.0.1", 8085, false),
            "127.0.0.1:8085"
        );
    }
}
