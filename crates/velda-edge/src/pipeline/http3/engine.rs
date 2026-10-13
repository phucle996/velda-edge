//! Persistent HTTP/3 QUIC State Machine Engine Container.
//!
//! Architectural Invariant:
//! Active HTTP/3 QUIC state machine engines persist across configuration reloads.
//! When routes, upstreams, or plugins reload, active client QUIC connections
//! are NOT disconnected; new incoming requests on existing connections seamlessly
//! evaluate against the latest swapped runtime snapshot.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};

use arc_swap::ArcSwapOption;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use velda_http3::Http3Engine;
use velda_tls::TlsServerEngine;
use velda_transport::UdpSocket;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// A single sharded QUIC engine state machine with lock-free socket association
/// and a dedicated millisecond-precision background ticker for loss recovery and ACK timer flushes.
pub struct H3Shard {
    pub engine: Mutex<Http3Engine>,
    socket: ArcSwapOption<UdpSocket>,
    ticker_started: std::sync::atomic::AtomicBool,
}

impl H3Shard {
    /// Creates a new shard. The background ticker is lazily spawned on first socket registration.
    pub fn new(
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Arc<Self> {
        let engine = Http3Engine::new_with_config(quic_cfg, h3_config);
        Arc::new(Self {
            engine: Mutex::new(engine),
            socket: ArcSwapOption::empty(),
            ticker_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Registers the active UDP socket handle on the shard and ensures the 1ms ticker is active.
    #[inline]
    pub fn register_socket(self: &Arc<Self>, socket: &Arc<UdpSocket>) {
        if self.socket.load().is_none() {
            self.socket.store(Some(socket.clone()));
        }

        if !self
            .ticker_started
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            let shard_weak = Arc::downgrade(self);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let Some(shard) = shard_weak.upgrade() else {
                        break;
                    };

                    let now = std::time::Instant::now();
                    let outgoing = {
                        let mut engine = shard.engine.lock();
                        if engine.connection_count() == 0 {
                            continue;
                        }
                        engine.handle_timeouts(now)
                    };

                    if !outgoing.is_empty()
                        && let Some(socket) = shard.socket.load_full()
                    {
                        for pkt in outgoing {
                            if let Some(res) = socket.try_send_to(&pkt.payload, pkt.peer) {
                                if let Err(e) = res {
                                    tracing::warn!(peer = %pkt.peer, error = %e, "Failed to send timeout packet via try_send_to");
                                }
                            } else if let Err(e) = socket.send_to(&pkt.payload, pkt.peer).await {
                                tracing::warn!(peer = %pkt.peer, error = %e, "Failed to send timeout packet via send_to");
                            }
                        }
                    }
                }
            });
        }
    }
}

/// Sharded container of HTTP/3 engines partitioned by remote peer address to eliminate
/// lock contention on multi-core hardware topologies.
#[derive(Clone)]
pub struct H3EngineShards {
    shards: Arc<Vec<Arc<H3Shard>>>,
}

impl H3EngineShards {
    /// Creates a sharded pool scaled dynamically to available hardware parallelism.
    pub fn new(
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let count = (cores * 8).clamp(32, 128).next_power_of_two();

        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(H3Shard::new(quic_cfg.clone(), h3_config));
        }
        Self {
            shards: Arc::new(shards),
        }
    }

    /// Selects an engine shard deterministically by hashing remote peer IP and port.
    #[inline]
    pub fn get_shard_for_peer(&self, peer: SocketAddr) -> Arc<H3Shard> {
        let port = peer.port() as usize;
        let ip_hash = match peer.ip() {
            std::net::IpAddr::V4(v4) => u32::from_ne_bytes(v4.octets()) as usize,
            std::net::IpAddr::V6(v6) => {
                let o = v6.octets();
                u32::from_ne_bytes([o[12], o[13], o[14], o[15]]) as usize
            }
        };
        // Fast multiply-shift mixing for uniform bucket distribution
        let hash = (port.wrapping_mul(0x9E3779B9) ^ ip_hash).wrapping_mul(0x85EBCA6B);
        let idx = hash & (self.shards.len() - 1);
        self.shards[idx].clone()
    }

    /// Returns the primary shard (shard 0) for lifecycle checks and API stability.
    #[inline]
    pub fn primary_shard(&self) -> Arc<H3Shard> {
        self.shards[0].clone()
    }
}

/// Global registry holding long-lived HTTP/3 QUIC sharded engines indexed by listener identifier.
static H3_ENGINES: OnceLock<RwLock<FxHashMap<String, H3EngineShards>>> = OnceLock::new();

fn engines_table() -> &'static RwLock<FxHashMap<String, H3EngineShards>> {
    H3_ENGINES.get_or_init(|| RwLock::new(FxHashMap::default()))
}

/// Initializes or updates the HTTP/3 QUIC engine for the given listener.
///
/// If an engine already exists for this listener, its active connection state is PRESERVED,
/// ensuring zero-downtime and zero connection drop during configuration reloads!
pub fn init_h3_engine(listener_id: &str, tls_server: &TlsServerEngine) -> Result<(), EdgeError> {
    let mut quic_cfg = tls_server.build_quic_config().map_err(EdgeError::Tls)?;
    let h3_config = velda_http3::Http3Config::auto();
    quic_cfg.transport = Arc::new(h3_config.build_transport_config());

    let table = engines_table();
    let mut write = table.write().unwrap();
    if !write.contains_key(listener_id) {
        let pool = H3EngineShards::new(Arc::new(quic_cfg), h3_config);
        write.insert(listener_id.to_string(), pool);
    }
    Ok(())
}

/// Checks whether an HTTP/3 engine is initialized for the given listener.
pub fn has_h3_engine(listener_id: &str) -> bool {
    let table = engines_table();
    table
        .read()
        .map(|l| l.contains_key(listener_id))
        .unwrap_or(false)
}

/// Clears all active HTTP/3 engines (useful for test isolation).
pub fn clear_h3_engines() {
    let table = engines_table();
    if let Ok(mut lock) = table.write() {
        lock.clear();
    }
}

/// Retrieves all sharded engines for the given listener, initializing if needed.
pub fn get_or_init_h3_shards(listener_id: &str, runtime: &SharedRuntime) -> Option<H3EngineShards> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool);
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table.read().ok()?.get(listener_id).cloned()
}

/// Retrieves an existing engine or lazily initializes one from the active runtime snapshot.
pub fn get_or_init_h3_engine(listener_id: &str, runtime: &SharedRuntime) -> Option<Arc<H3Shard>> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool.primary_shard());
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table
        .read()
        .ok()?
        .get(listener_id)
        .map(|p| p.primary_shard())
}

/// Retrieves the specific sharded engine for a given remote peer to eliminate lock contention.
pub fn get_or_init_h3_engine_for_peer(
    listener_id: &str,
    peer: SocketAddr,
    runtime: &SharedRuntime,
) -> Option<Arc<H3Shard>> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool.get_shard_for_peer(peer));
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table
        .read()
        .ok()?
        .get(listener_id)
        .map(|p| p.get_shard_for_peer(peer))
}
