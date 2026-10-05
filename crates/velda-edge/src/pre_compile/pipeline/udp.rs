//! Layer 4 (L4) raw UDP datagram proxying.
//!
//! Supports:
//! - **Unidirectional proxying (1 chiều)**: Direct fire-and-forget datagram forwarding.
//! - **Bidirectional proxying (2 chiều)**: Stateful flow/session tracking via [`UdpSessionTable`]
//!   with ephemeral upstream sockets, automatic response routing back to client, and idle eviction.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use rustc_hash::{FxBuildHasher, FxHashMap};
use velda_transport::{Datagram, UdpSocket};

use crate::runtime::Runtime;

use std::hash::{BuildHasher, Hash};

/// Unique identifier for an active client flow in the L4 UDP session table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UdpSessionKey {
    /// Ingress listener identifier accepting the client flow.
    pub listener_id: Arc<str>,
    /// Client remote socket address.
    pub client_addr: SocketAddr,
}

/// Result of acquiring or registering a session slot in [`UdpSessionTable`].
pub enum SessionAcquisition {
    /// An existing active session channel is already running.
    Existing(tokio::sync::mpsc::Sender<Vec<u8>>),
    /// A new session was atomically registered; the caller must drive the receive loop.
    Created {
        /// Sender handle stored in the session table.
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
        /// Receiver channel to consume incoming client datagrams.
        rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    },
}

pub type UdpSessionShard = RwLock<FxHashMap<UdpSessionKey, tokio::sync::mpsc::Sender<Vec<u8>>>>;

/// In-memory sharded session table tracking active bidirectional L4 UDP flows with zero cross-client contention.
pub struct UdpSessionTable {
    shards: Box<[UdpSessionShard]>,
    hash_builder: FxBuildHasher,
    mask: usize,
}

impl Default for UdpSessionTable {
    fn default() -> Self {
        Self::new()
    }
}

impl UdpSessionTable {
    /// Creates a new sharded UDP session table scaled to hardware topology.
    pub fn new() -> Self {
        let count = velda_core::global_hardware_topology()
            .worker_threads
            .max(4)
            .clamp(4, 64)
            .next_power_of_two();
        let shards = (0..count)
            .map(|_| RwLock::new(FxHashMap::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            hash_builder: FxBuildHasher,
            mask: count - 1,
        }
    }

    #[inline]
    fn shard_for(&self, key: &UdpSessionKey) -> &UdpSessionShard {
        let idx = (self.hash_builder.hash_one(key) as usize) & self.mask;
        &self.shards[idx]
    }

    /// Atomically retrieves an existing session or registers a new session slot.
    ///
    /// Eliminates TOCTOU race conditions where concurrent packets from the same client flow
    /// might attempt to spawn multiple duplicate ephemeral upstream sockets.
    pub fn get_or_create(&self, key: &UdpSessionKey) -> SessionAcquisition {
        let shard = self.shard_for(key);

        // Fast path: existing active session
        if let Ok(guard) = shard.read()
            && let Some(sender) = guard.get(key)
            && !sender.is_closed()
        {
            return SessionAcquisition::Existing(sender.clone());
        }

        // Write path: double-check and register fresh channel atomically
        if let Ok(mut guard) = shard.write() {
            if let Some(sender) = guard.get(key)
                && !sender.is_closed()
            {
                return SessionAcquisition::Existing(sender.clone());
            }
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            guard.insert(key.clone(), tx.clone());
            SessionAcquisition::Created { sender: tx, rx }
        } else {
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            SessionAcquisition::Created { sender: tx, rx }
        }
    }

    /// Retrieves an active sender channel for the given session key.
    pub fn get_sender(&self, key: &UdpSessionKey) -> Option<tokio::sync::mpsc::Sender<Vec<u8>>> {
        self.shard_for(key).read().ok()?.get(key).cloned()
    }

    /// Inserts an active session into the appropriate shard.
    pub fn insert(&self, key: UdpSessionKey, sender: tokio::sync::mpsc::Sender<Vec<u8>>) {
        if let Ok(mut lock) = self.shard_for(&key).write() {
            lock.insert(key, sender);
        }
    }

    /// Removes a session from the table upon completion or idle timeout.
    pub fn remove(&self, key: &UdpSessionKey) {
        if let Ok(mut lock) = self.shard_for(key).write() {
            lock.remove(key);
        }
    }

    /// Returns the number of currently active UDP sessions across all shards.
    pub fn active_session_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().map(|l| l.len()).unwrap_or(0))
            .sum()
    }

    /// Clears all sessions across all shards.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            if let Ok(mut lock) = shard.write() {
                lock.clear();
            }
        }
    }
}

static GLOBAL_UDP_SESSIONS: OnceLock<UdpSessionTable> = OnceLock::new();

/// Returns a reference to the global UDP session table.
pub fn get_udp_session_table() -> &'static UdpSessionTable {
    GLOBAL_UDP_SESSIONS.get_or_init(UdpSessionTable::new)
}

/// Handles raw UDP L4 datagrams with support for both unidirectional and bidirectional flows.
pub async fn handle_l4_udp(
    listener_id: Arc<str>,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    rt: &Runtime,
) {
    let Some(route) = rt.router.route_udp(&listener_id) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %datagram.peer(),
            "No L4 UDP route configured for listener; dropping datagram"
        );
        return;
    };

    let Some(up) = rt.upstreams.udp.get(&route.upstream_name) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %datagram.peer(),
            "No backend upstream available in UDP upstream table; dropping datagram"
        );
        return;
    };

    // 1. Unidirectional Mode (1 chiều): every datagram is balanced independently.
    if route.is_unidirectional() {
        let Some(target_addr) = up.select_target() else {
            tracing::error!(
                listener = %listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                peer = %datagram.peer(),
                "No backend endpoints available for L4 UDP upstream; dropping datagram"
            );
            return;
        };
        tracing::debug!(
            listener = %listener_id,
            target = %target_addr,
            peer = %datagram.peer(),
            "Forwarding UDP datagram unidirectionally"
        );
        if let Err(e) = socket.send_to(datagram.data(), target_addr).await {
            tracing::warn!(
                error = %e,
                listener = %listener_id,
                target = %target_addr,
                peer = %datagram.peer(),
                "Failed to forward UDP datagram (unidirectional)"
            );
        }
        return;
    }

    // 2. Bidirectional Mode (2 chiều): Stateful Session Tracking
    let idle_timeout = route.udp_idle_timeout.unwrap_or(Duration::from_secs(30));

    let key = UdpSessionKey {
        listener_id: Arc::clone(&listener_id),
        client_addr: datagram.peer(),
    };

    let session_table = get_udp_session_table();

    let client_addr = datagram.peer();

    // Atomically acquire existing session or register fresh session. The backend is chosen
    // only for a new session, so existing flows neither pay for nor perturb the balancer.
    // Use `datagram.into_data()` to move the existing payload buffer into the channel without
    // any duplicate heap allocations or memcpy.
    let (mut rx, initial_data) = match session_table.get_or_create(&key) {
        SessionAcquisition::Existing(sender) => {
            if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) =
                sender.try_send(datagram.into_data())
            {
                tracing::warn!(
                    listener = %listener_id,
                    peer = %client_addr,
                    "UDP session buffer full; dropping datagram"
                );
            }
            return;
        }
        SessionAcquisition::Created { sender: _, rx } => (rx, datagram.into_data()),
    };

    let Some(target_addr) = up.select_target() else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %client_addr,
            "No backend endpoints available for L4 UDP upstream; dropping datagram"
        );
        session_table.remove(&key);
        return;
    };

    let downstream_socket = socket.clone();
    let session_key = key.clone();

    tracing::debug!(
        listener = %listener_id,
        target = %target_addr,
        client = %client_addr,
        "Established new bidirectional L4 UDP session"
    );

    tokio::spawn(async move {
        let bind_addr: SocketAddr = if target_addr.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let upstream_socket = match tokio::net::UdpSocket::bind(bind_addr).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    listener = %session_key.listener_id,
                    target = %target_addr,
                    "Failed to bind ephemeral upstream UDP socket for L4 session"
                );
                get_udp_session_table().remove(&session_key);
                return;
            }
        };

        if let Err(e) = upstream_socket.connect(target_addr).await {
            tracing::error!(
                error = %e,
                listener = %session_key.listener_id,
                target = %target_addr,
                "Failed to connect ephemeral UDP socket to upstream target"
            );
            get_udp_session_table().remove(&session_key);
            return;
        }

        // Send the initial datagram to upstream
        if let Err(e) = upstream_socket.send(&initial_data).await {
            tracing::warn!(
                error = %e,
                listener = %session_key.listener_id,
                target = %target_addr,
                "Failed to send initial UDP datagram to upstream"
            );
            get_udp_session_table().remove(&session_key);
            return;
        }

        let mut buf = [0u8; 65535];

        loop {
            tokio::select! {
                // Client -> Gateway -> Upstream Backend
                maybe_data = rx.recv() => {
                    match maybe_data {
                        Some(data) => {
                            if let Err(e) = upstream_socket.send(&data).await {
                                tracing::warn!(
                                    error = %e,
                                    target = %target_addr,
                                    "Failed forwarding client UDP datagram to upstream"
                                );
                                break;
                            }
                        }
                        None => break,
                    }
                }
                // Upstream Backend -> Gateway -> Client
                res = upstream_socket.recv(&mut buf) => {
                    match res {
                        Ok(n) => {
                            if let Err(e) = downstream_socket.send_to(&buf[..n], client_addr).await {
                                tracing::warn!(
                                    error = %e,
                                    client = %client_addr,
                                    "Failed forwarding UDP reply back to client"
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Error receiving UDP response from upstream");
                            break;
                        }
                    }
                }
                // Idle timeout
                _ = tokio::time::sleep(idle_timeout) => {
                    tracing::debug!(
                        client = %client_addr,
                        target = %target_addr,
                        "UDP L4 session idle timeout elapsed; closing session"
                    );
                    break;
                }
            }
        }

        get_udp_session_table().remove(&session_key);
    });
}
