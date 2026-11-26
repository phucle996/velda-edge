//! AWS ElastiCache pools (`AwsElastiCacheNodePool` & `AwsElastiCacheClusterPool`) with
//! pure-CPU dynamic AWS SigV4 IAM Presigned Token (`Action=connect`, service=`elasticache`).
//!
//! ```text
//!   connect_authenticated():
//!     1. ColdConnectBlueprint::connect_stream() ──► TLS/TCP stream
//!     2. generate_iam_auth_token(region, replication_group_id, username, access_key, secret)
//!        [0 network hops! Pure-CPU ring::hmac SigV4 presigned token valid 900s]
//!     3. Send AUTH <username> <iam_token> (+ optional SELECT <db>) ──► "+OK"
//! ```

use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use ring::{digest, hmac};
use tokio::sync::{Mutex, Semaphore};

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::aws_elasticache::config::AwsElastiCacheEtpConfig;
use crate::aws_elasticache::lease::AwsElastiCacheLease;
use crate::aws_elasticache::slots::{
    SlotMap, fetch_cluster_slots, format_host_port, split_host_port,
};
use crate::config::EtpPoolSettings;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTlsConfig, build_tls_target};
use crate::error::EtpError;

#[derive(Clone)]
pub struct AwsIamSigner {
    pub region: String,
    pub replication_group_id: String,
    pub username: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl AwsIamSigner {
    /// Generates an AWS ElastiCache IAM presigned token in RAM (`elasticache:connect`).
    pub fn sign_auth_token(&self, unix_secs: u64) -> String {
        let (date_stamp, amz_date) = format_amz_timestamps(unix_secs);
        let scope = format!("{date_stamp}/{}/elasticache/aws4_request", self.region);
        let host = format!("{}/", self.replication_group_id);

        let mut query = format!(
            "Action=connect&User={}&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}%2F{}%2F{}%2Felasticache%2Faws4_request&X-Amz-Date={amz_date}&X-Amz-Expires=900",
            pct_encode(&self.username),
            pct_encode(&self.access_key_id),
            date_stamp,
            pct_encode(&self.region),
        );
        if let Some(ref sts) = self.session_token {
            query.push_str("&X-Amz-Security-Token=");
            query.push_str(&pct_encode(sts));
        }
        query.push_str("&X-Amz-SignedHeaders=host");

        let canonical = format!(
            "GET\n/\n{query}\nhost:{}\n\nhost\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            self.replication_group_id
        );
        let hash = hex_lower(digest::digest(&digest::SHA256, canonical.as_bytes()).as_ref());
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hash}");

        let k_secret = format!("AWS4{}", self.secret_access_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"elasticache");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let sig = hex_lower(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

        format!("{host}?{query}&X-Amz-Signature={sig}")
    }
}

pub struct AwsElastiCacheNodePool {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub db: u16,
    pub blueprint: ColdConnectBlueprint,
    signer: AwsIamSigner,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
}

impl fmt::Debug for AwsElastiCacheNodePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsElastiCacheNodePool")
            .field("id", &self.id)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl AwsElastiCacheNodePool {
    pub fn from_config(config: &AwsElastiCacheEtpConfig) -> Result<Self, EtpError> {
        Self::from_endpoint(
            &config.id,
            &config.host,
            config.port,
            config.db,
            AwsIamSigner {
                region: config.region.clone(),
                replication_group_id: config.replication_group_id.clone(),
                username: config.username.clone(),
                access_key_id: config.access_key_id.clone(),
                secret_access_key: config.secret_access_key.clone(),
                session_token: config.session_token.clone(),
            },
            &config.pool,
            config.tls.as_ref(),
        )
    }

    pub fn from_endpoint(
        id: &str,
        host: &str,
        port: u16,
        db: u16,
        signer: AwsIamSigner,
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
            signer,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    pub async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        let mut stream = self.blueprint.connect_stream().await?;
        let now_unix = velda_core::clock::coarse_sec();
        let token = self.signer.sign_auth_token(now_unix);
        let mut frame = format!(
            "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{token}\r\n",
            self.signer.username.len(),
            self.signer.username,
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
                        "AwsElastiCache '{}' IAM AUTH step {step} failed: {reply}",
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

    pub fn try_acquire(self: &Arc<Self>) -> Option<AwsElastiCacheLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(idx) => {
                permit.forget();
                Some(AwsElastiCacheLease::new(Arc::clone(self), idx))
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

    pub async fn acquire(self: &Arc<Self>) -> Result<AwsElastiCacheLease, EtpError> {
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
            return Ok(AwsElastiCacheLease::new(Arc::clone(self), idx));
        }
        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.id.clone(),
            });
        };
        let s = self.connect_authenticated().await?;
        let idx = guard.commit_leased(s);
        permit.forget();
        Ok(AwsElastiCacheLease::new(Arc::clone(self), idx))
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
                            tracing::warn!(id = %pool.id, error = %e, "AwsElastiCache replenish failed");
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
pub struct AwsClusterNode {
    pub addr: String,
    pub pool: Arc<AwsElastiCacheNodePool>,
}

#[derive(Debug, Default)]
pub struct AwsClusterTopology {
    pub slot_map: SlotMap,
    pub nodes: Vec<AwsClusterNode>,
}

pub struct AwsElastiCacheClusterPool {
    pub config: AwsElastiCacheEtpConfig,
    topology: ArcSwap<AwsClusterTopology>,
    refresh_lock: Mutex<()>,
    rr_cursor: AtomicUsize,
}

impl fmt::Debug for AwsElastiCacheClusterPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsElastiCacheClusterPool")
            .field("id", &self.config.id)
            .field("node_count", &self.topology.load().nodes.len())
            .finish()
    }
}

impl AwsElastiCacheClusterPool {
    pub fn new(config: AwsElastiCacheEtpConfig) -> Self {
        Self {
            config,
            topology: ArcSwap::from_pointee(AwsClusterTopology::default()),
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

    pub async fn acquire_for_node(&self, node_idx: usize) -> Result<AwsElastiCacheLease, EtpError> {
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

    pub async fn acquire_for_slot(&self, slot: u16) -> Result<AwsElastiCacheLease, EtpError> {
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
                "AwsElastiCache Cluster '{}' has no master nodes for slot {slot}",
                self.config.id
            ))),
        }
    }

    pub async fn acquire_any(&self) -> Result<AwsElastiCacheLease, EtpError> {
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
                "AwsElastiCache Cluster '{}' failed slot discovery",
                self.config.id
            ))),
        }
    }

    pub fn try_acquire_any(&self) -> Option<AwsElastiCacheLease> {
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

    pub async fn acquire_for_addr(&self, host_port: &str) -> Result<AwsElastiCacheLease, EtpError> {
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
                cloned.push(AwsClusterNode {
                    addr: canonical.to_string(),
                    pool,
                });
                (cloned, idx)
            };
        let target_u16 = u16::try_from(target_idx).map_err(|_| {
            EtpError::Config(format!(
                "AwsElastiCache Cluster '{}' exceeded u16 node count limit",
                self.config.id
            ))
        })?;
        let next_slot_map = current
            .slot_map
            .with_moved_slot(slot, target_u16, next_nodes.len());
        self.topology.store(Arc::new(AwsClusterTopology {
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
                    "AwsElastiCache Cluster '{}' failed to query CLUSTER SLOTS",
                    self.config.id
                ))
            })
        })?;

        let existing = self.topology.load();
        let existing_map: HashMap<&str, &Arc<AwsElastiCacheNodePool>> = existing
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
                        "AwsElastiCache Cluster '{}' exceeded u16 shard count",
                        self.config.id
                    ))
                })?;
                nodes.push(AwsClusterNode {
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
            .store(Arc::new(AwsClusterTopology { slot_map, nodes }));
        Ok(())
    }

    fn build_node_pool(&self, host: &str, port: u16) -> Result<AwsElastiCacheNodePool, EtpError> {
        AwsElastiCacheNodePool::from_endpoint(
            &self.config.id,
            host,
            port,
            0,
            AwsIamSigner {
                region: self.config.region.clone(),
                replication_group_id: self.config.replication_group_id.clone(),
                username: self.config.username.clone(),
                access_key_id: self.config.access_key_id.clone(),
                secret_access_key: self.config.secret_access_key.clone(),
                session_token: self.config.session_token.clone(),
            },
            &self.config.pool,
            self.config.tls.as_ref(),
        )
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn pct_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn format_amz_timestamps(unix_secs: u64) -> (String, String) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let ss = rem % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    let date = format!("{year:04}{m:02}{d:02}");
    let full = format!("{date}T{hh:02}{mm:02}{ss:02}Z");
    (date, full)
}

async fn read_resp_line(stream: &mut EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "AwsElastiCache closed connection during IAM AUTH",
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
                "AwsElastiCache RESP line exceeded 16 KiB",
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elasticache_iam_presigned_token_format() {
        let signer = AwsIamSigner {
            region: "us-east-1".into(),
            replication_group_id: "velda-rl-rg".into(),
            username: "velda_edge".into(),
            access_key_id: "AKIAEXAMPLE".into(),
            secret_access_key: "SECRETEXAMPLE".into(),
            session_token: None,
        };
        let tok = signer.sign_auth_token(1_700_000_000);
        assert!(tok.starts_with(
            "velda-rl-rg/?Action=connect&User=velda_edge&X-Amz-Algorithm=AWS4-HMAC-SHA256"
        ));
        assert!(tok.contains("X-Amz-Expires=900"));
        assert!(tok.contains("X-Amz-Signature="));
    }
}
