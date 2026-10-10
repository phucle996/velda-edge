//! Layer 7 gRPC over UDP: sharded QUIC state machine container and engine lifecycle.
//!
//! Partitioned by remote peer address to eliminate lock contention on multi-core topologies.

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
        }
    }

    /// Selects an engine shard deterministically by hashing remote peer IP and port.
    #[inline]
    pub fn get_shard_for_peer(&self, peer: SocketAddr) -> Arc<Mutex<GrpcUdpEngine>> {
        let port = peer.port() as usize;
        let ip_hash = match peer.ip() {
            std::net::IpAddr::V4(v4) => u32::from_ne_bytes(v4.octets()) as usize,
            std::net::IpAddr::V6(v6) => {
                let o = v6.octets();
                u32::from_ne_bytes([o[12], o[13], o[14], o[15]]) as usize
            }
        };
        let idx = (port ^ ip_hash) % self.shards.len();
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
