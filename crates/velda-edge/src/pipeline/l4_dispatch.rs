//! Layer 4 (L4) traffic dispatching for raw TCP and UDP connections.
//!
//! Supports:
//! - **TCP stream proxying**: Zero-copy bidirectional byte pumping via `connect_and_forward`.
//! - **UDP unidirectional proxying (1 chiều)**: Direct fire-and-forget datagram forwarding.
//! - **UDP bidirectional proxying (2 chiều)**: Stateful flow/session tracking via [`UdpSessionTable`]
//!   with ephemeral upstream sockets, automatic response routing back to client, and idle eviction.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use velda_core::TransportProtocol;
use velda_transport::{Connection, Datagram, UdpSocket};

use crate::runtime::SharedRuntime;

/// Unique identifier for an active client flow in the L4 UDP session table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UdpSessionKey {
    /// Ingress listener identifier accepting the client flow.
    pub listener_id: String,
    /// Client remote socket address.
    pub client_addr: SocketAddr,
}

/// In-memory session table tracking active bidirectional L4 UDP flows.
#[derive(Debug, Default)]
pub struct UdpSessionTable {
    sessions: RwLock<HashMap<UdpSessionKey, tokio::sync::mpsc::Sender<Vec<u8>>>>,
}

impl UdpSessionTable {
    /// Creates a new empty UDP session table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieves an active sender channel for the given session key.
    pub fn get_sender(&self, key: &UdpSessionKey) -> Option<tokio::sync::mpsc::Sender<Vec<u8>>> {
        self.sessions.read().ok()?.get(key).cloned()
    }

    /// Inserts a new active session into the table.
    pub fn insert(&self, key: UdpSessionKey, sender: tokio::sync::mpsc::Sender<Vec<u8>>) {
        if let Ok(mut lock) = self.sessions.write() {
            lock.insert(key, sender);
        }
    }

    /// Removes a session from the table upon completion or idle timeout.
    pub fn remove(&self, key: &UdpSessionKey) {
        if let Ok(mut lock) = self.sessions.write() {
            lock.remove(key);
        }
    }

    /// Returns the number of currently active UDP sessions.
    pub fn active_session_count(&self) -> usize {
        self.sessions.read().map(|l| l.len()).unwrap_or(0)
    }

    /// Clears all sessions from the table.
    pub fn clear(&self) {
        if let Ok(mut lock) = self.sessions.write() {
            lock.clear();
        }
    }
}

static GLOBAL_UDP_SESSIONS: OnceLock<UdpSessionTable> = OnceLock::new();

/// Returns a reference to the global UDP session table.
pub fn get_udp_session_table() -> &'static UdpSessionTable {
    GLOBAL_UDP_SESSIONS.get_or_init(UdpSessionTable::new)
}

/// Dispatches raw L4 TCP connection to routing and direct upstream byte-level proxying.
pub async fn dispatch_l4(conn: Connection, runtime: &SharedRuntime) {
    let peer = conn.peer();
    let Some(listener_id) = conn.listener_id().map(|s| s.to_string()) else {
        tracing::warn!(peer = %peer, "Received L4 connection without listener_id; dropping");
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_l4(&listener_id, TransportProtocol::Tcp) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "No L4 TCP route configured for listener; dropping connection"
        );
        return;
    };

    let Some(target_addr) = route.select_target() else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %peer,
            "No backend endpoints available for L4 upstream; dropping connection"
        );
        return;
    };

    tracing::debug!(
        listener = %listener_id,
        route = %route.id,
        upstream = %route.upstream_name,
        target = %target_addr,
        peer = %peer,
        "Proxying L4 TCP stream to upstream backend"
    );

    tokio::spawn(async move {
        match velda_transport::tcp::forward::connect_and_forward(conn, target_addr).await {
            Ok(stats) => {
                tracing::debug!(
                    target = %target_addr,
                    peer = %peer,
                    client_to_server = stats.client_to_server_bytes,
                    server_to_client = stats.server_to_client_bytes,
                    "L4 TCP stream forwarding completed"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    target = %target_addr,
                    peer = %peer,
                    "L4 TCP stream forwarding terminated with error"
                );
            }
        }
    });
}

/// Dispatches raw UDP L4 datagrams with support for both:
/// - Unidirectional forwarding (1 chiều): fire-and-forget.
/// - Bidirectional forwarding (2 chiều): stateful flow session proxying with reply routing.
pub async fn dispatch_udp_l4(
    listener_id: String,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    runtime: &SharedRuntime,
) {
    let rt = runtime.load();
    let Some(route) = rt.router.route_l4(&listener_id, TransportProtocol::Udp) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %datagram.peer(),
            "No L4 UDP route configured for listener; dropping datagram"
        );
        return;
    };

    let Some(target_addr) = route.select_target() else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %datagram.peer(),
            "No backend endpoints available for L4 UDP upstream; dropping datagram"
        );
        return;
    };

    // 1. Unidirectional Mode (1 chiều)
    if route.is_unidirectional() {
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
        listener_id: listener_id.clone(),
        client_addr: datagram.peer(),
    };

    let session_table = get_udp_session_table();

    // Route through existing active session if present
    if let Some(sender) = session_table.get_sender(&key) {
        match sender.try_send(datagram.data().to_vec()) {
            Ok(()) => return,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    listener = %listener_id,
                    peer = %datagram.peer(),
                    "UDP session buffer full; dropping datagram"
                );
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                session_table.remove(&key);
                // Session closing, fall through to establish fresh session
            }
        }
    }

    // Establish a new bidirectional UDP session
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(128);
    session_table.insert(key.clone(), tx);

    let client_addr = datagram.peer();
    let initial_data = datagram.data().to_vec();
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
