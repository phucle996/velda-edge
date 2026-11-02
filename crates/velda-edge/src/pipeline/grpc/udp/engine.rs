//! Layer 7 gRPC over UDP: sharded QUIC state machine container and engine lifecycle.
//!
//! Partitioned by remote peer address to eliminate lock contention on multi-core topologies.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};

use rustc_hash::FxHashMap;
use tokio::sync::Mutex;
use velda_grpc::GrpcConfig;
use velda_grpc::udp::quinn_proto;
use velda_grpc::udp::server::GrpcUdpEngine;
use velda_tls::TlsServerEngine;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Sharded container of gRPC UDP engines partitioned by remote peer address to eliminate
/// lock contention on multi-core hardware topologies.
#[derive(Clone)]
pub struct GrpcUdpEngineShards {
    shards: Arc<Vec<Arc<Mutex<GrpcUdpEngine>>>>,
    /// Per-process randomly keyed SipHash state. Source IP/port are attacker-chosen, so an
    /// unkeyed `ip ^ port` would let a client pile every flow onto one shard mutex. The key is
    /// fixed for the process lifetime (clones share it), so a peer always maps to the same shard.
    hasher: RandomState,
}

impl GrpcUdpEngineShards {
    /// Creates a sharded pool scaled dynamically to available hardware parallelism.
    pub fn new(quic_cfg: Arc<quinn_proto::ServerConfig>, grpc_config: GrpcConfig) -> Self {
        let count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 64)
            .next_power_of_two();

        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            let engine = GrpcUdpEngine::new(quic_cfg.clone(), grpc_config);
            shards.push(Arc::new(Mutex::new(engine)));
        }
        Self {
            shards: Arc::new(shards),
            hasher: RandomState::new(),
        }
    }

    /// Selects an engine shard by a keyed hash of the full remote peer address (IP + port).
    ///
    /// Deterministic within the process (a peer's QUIC state always lives in one shard) yet
    /// unpredictable to clients, so shard placement cannot be targeted.
    #[inline]
    pub fn get_shard_for_peer(&self, peer: SocketAddr) -> Arc<Mutex<GrpcUdpEngine>> {
        let idx = (self.hasher.hash_one(peer) as usize) & (self.shards.len() - 1);
        self.shards[idx].clone()
    }

    /// Returns the primary shard (shard 0) for lifecycle checks.
    #[inline]
    pub fn primary_shard(&self) -> Arc<Mutex<GrpcUdpEngine>> {
        self.shards[0].clone()
    }
}

/// Global registry holding long-lived gRPC UDP sharded engines indexed by listener identifier.
static GRPC_UDP_ENGINES: OnceLock<RwLock<FxHashMap<String, GrpcUdpEngineShards>>> = OnceLock::new();

fn engines_table() -> &'static RwLock<FxHashMap<String, GrpcUdpEngineShards>> {
    GRPC_UDP_ENGINES.get_or_init(|| RwLock::new(FxHashMap::default()))
}

/// Initializes or updates the gRPC UDP QUIC engine for the given listener.
pub fn init_grpc_udp_engine(
    listener_id: &str,
    tls_server: &TlsServerEngine,
) -> Result<(), EdgeError> {
    let quic_cfg = tls_server.build_quic_config().map_err(EdgeError::Tls)?;
    let grpc_config = GrpcConfig::auto();

    let table = engines_table();
    let mut write = table.write().unwrap();
    if !write.contains_key(listener_id) {
        let pool = GrpcUdpEngineShards::new(Arc::new(quic_cfg), grpc_config);
        write.insert(listener_id.to_string(), pool);
    }
    Ok(())
}

/// Checks whether a gRPC UDP engine is initialized for the given listener.
pub fn has_grpc_udp_engine(listener_id: &str) -> bool {
    let table = engines_table();
    table
        .read()
        .map(|l| l.contains_key(listener_id))
        .unwrap_or(false)
}

/// Clears all active gRPC UDP engines.
pub fn clear_grpc_udp_engines() {
    let table = engines_table();
    if let Ok(mut lock) = table.write() {
        lock.clear();
    }
}

/// Retrieves the specific sharded engine for a given remote peer.
pub fn get_or_init_grpc_udp_engine_for_peer(
    listener_id: &str,
    peer: SocketAddr,
    runtime: &SharedRuntime,
) -> Option<Arc<Mutex<GrpcUdpEngine>>> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool.get_shard_for_peer(peer));
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_grpc_udp_engine(listener_id, tls_server);

    let table = engines_table();
    table
        .read()
        .ok()?
        .get(listener_id)
        .map(|p| p.get_shard_for_peer(peer))
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    use std::net::SocketAddr;

    /// Structured, attacker-style peers (one IP, sequential ports) must spread across shards
    /// the same way `get_shard_for_peer` selects them, and stay stable for a given key.
    #[test]
    fn keyed_peer_hash_spreads_sequential_ports() {
        let hasher = RandomState::new();
        let shards = 8usize;
        let mut buckets = [0usize; 8];
        for port in 10_000u16..18_000 {
            let peer: SocketAddr = SocketAddr::from(([203, 0, 113, 7], port));
            let idx = (hasher.hash_one(peer) as usize) & (shards - 1);
            assert_eq!(idx, (hasher.hash_one(peer) as usize) & (shards - 1));
            buckets[idx] += 1;
        }
        // 8000 flows / 8 shards = 1000 expected; allow +-25%.
        assert!(
            buckets.iter().all(|&c| (750..=1250).contains(&c)),
            "{buckets:?}"
        );
    }
}
