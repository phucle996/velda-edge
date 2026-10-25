//! Layer 4 (L4) raw UDP datagram proxying.
//!
//! Supports:
//! - **Unidirectional proxying (1 chiều)**: Direct fire-and-forget datagram forwarding.
//! - **Bidirectional proxying (2 chiều)**: Stateful flow/session tracking via [`UdpSessionTable`]
//!   with ephemeral upstream sockets, automatic response routing back to client, and two-tier idle eviction.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use rustc_hash::FxHashMap;
use velda_transport::{Datagram, UdpSocket};

use crate::runtime::Runtime;
use crate::runtime::pipeline::L4UdpConfig;

thread_local! {
    /// 64KB thread-local scratch buffer per CPU core for kernel GRO demuxing.
    ///
    /// Allocated on heap once per worker thread, keeping it permanently warm in L1/L2 cache.
    /// Eliminates buffer allocations inside individual session tasks (0 bytes buffer per session).
    static GRO_SCRATCH: std::cell::RefCell<Box<[u8; 65536]>> =
        std::cell::RefCell::new(vec![0u8; 65536].into_boxed_slice().try_into().unwrap());
}

/// Non-blocking datagram receipt with Linux UDP_GRO demuxing.
///
/// Returns `Ok(Some((bytes_read, gso_size)))` when data is available.
/// If kernel coalesced multiple packets via `UDP_GRO`, `gso_size` contains the original packet length.
fn try_recv_gro(
    socket: &tokio::net::UdpSocket,
    buf: &mut [u8],
) -> std::io::Result<Option<(usize, Option<u16>)>> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let fd = socket.as_raw_fd();

        socket
            .try_io(tokio::io::Interest::READABLE, || {
                let mut iov = libc::iovec {
                    iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                    iov_len: buf.len(),
                };
                let mut cmsg_buf = [0u8; 128];
                let mut msg = unsafe { std::mem::zeroed::<libc::msghdr>() };
                msg.msg_iov = &mut iov;
                msg.msg_iovlen = 1;
                msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
                msg.msg_controllen = cmsg_buf.len() as _;

                let ret = unsafe { libc::recvmsg(fd, &mut msg, libc::MSG_DONTWAIT) };
                if ret < 0 {
                    return Err(std::io::Error::last_os_error());
                }

                // Drop truncated datagrams exceeding buffer size (64KB)
                if msg.msg_flags & libc::MSG_TRUNC != 0 {
                    return Ok((0, None));
                }

                let bytes_read = ret as usize;
                let mut gso_size: Option<u16> = None;

                const UDP_GRO: libc::c_int = 104;
                let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
                while !cmsg.is_null() {
                    unsafe {
                        if (*cmsg).cmsg_level == libc::IPPROTO_UDP && (*cmsg).cmsg_type == UDP_GRO {
                            let data_ptr = libc::CMSG_DATA(cmsg) as *const u16;
                            let val = std::ptr::read_unaligned(data_ptr);
                            if val > 0 {
                                gso_size = Some(val);
                            }
                            break;
                        }
                        cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                    }
                }

                Ok((bytes_read, gso_size))
            })
            .map(Some)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    Ok(None)
                } else {
                    Err(e)
                }
            })
    }
    #[cfg(not(target_os = "linux"))]
    {
        match socket.try_recv(buf) {
            Ok(n) => Ok(Some((n, None))),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Sends a batch of datagrams to an upstream connected UDP socket using `sendmmsg` (Linux).
///
/// Batches up to 32 datagrams in a single system call to cut syscall overhead under bursts,
/// while strictly preserving individual datagram framing on the wire.
async fn send_batch_to_socket(socket: &tokio::net::UdpSocket, batch: &[bytes::Bytes]) {
    if batch.is_empty() {
        return;
    }
    if batch.len() == 1 {
        let _ = socket.send(&batch[0]).await;
        return;
    }

    #[cfg(target_os = "linux")]
    let sent = {
        use std::os::fd::AsRawFd;
        let fd = socket.as_raw_fd();
        let batch_len = batch.len().min(32);

        let mut iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; 32];
        let mut msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; 32]>() };

        for i in 0..batch_len {
            iovecs[i].iov_base = batch[i].as_ptr() as *mut libc::c_void;
            iovecs[i].iov_len = batch[i].len();
            msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
            msgs[i].msg_hdr.msg_iovlen = 1;
        }

        let ret =
            unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_len as u32, libc::MSG_DONTWAIT) };
        if ret > 0 { ret as usize } else { 0 }
    };

    #[cfg(target_os = "linux")]
    {
        let batch_len = batch.len().min(32);
        for data in &batch[sent..batch_len] {
            if let Err(e) = socket.send(data).await {
                tracing::warn!(error = %e, "Failed forwarding UDP datagram to upstream");
                break;
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        for data in batch {
            if let Err(e) = socket.send(data).await {
                tracing::warn!(error = %e, "Failed forwarding UDP datagram to upstream");
                break;
            }
        }
    }
}

/// Unique identifier for an active client flow in the L4 UDP session table.
#[derive(Debug, Clone)]
pub struct UdpSessionKey {
    /// Ingress listener identifier accepting the client flow.
    pub listener_id: Arc<str>,
    /// Client remote socket address.
    pub client_addr: SocketAddr,
    /// Opt-in payload token (e.g. Game ticket / Match ID / QUIC connection ID).
    pub token: Option<u64>,
}

impl UdpSessionKey {
    /// Creates a pure L4 session key identified strictly by listener and client address.
    #[inline]
    pub fn peer(listener_id: Arc<str>, client_addr: SocketAddr) -> Self {
        Self {
            listener_id,
            client_addr,
            token: None,
        }
    }

    /// Creates an opt-in session key identified by listener and payload session token.
    #[inline]
    pub fn token(listener_id: Arc<str>, client_addr: SocketAddr, token: u64) -> Self {
        Self {
            listener_id,
            client_addr,
            token: Some(token),
        }
    }
}

impl PartialEq for UdpSessionKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        (Arc::ptr_eq(&self.listener_id, &other.listener_id)
            || self.listener_id == other.listener_id)
            && match (self.token, other.token) {
                (Some(t1), Some(t2)) => t1 == t2,
                (None, None) => self.client_addr == other.client_addr,
                _ => false,
            }
    }
}

impl Eq for UdpSessionKey {}

impl Hash for UdpSessionKey {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.listener_id.hash(state);
        match self.token {
            Some(token) => {
                1u8.hash(state);
                token.hash(state);
            }
            None => {
                0u8.hash(state);
                self.client_addr.hash(state);
            }
        }
    }
}

/// Active session entry storing the upstream delivery channel and dynamic peer address.
#[derive(Clone)]
pub struct UdpSessionEntry {
    /// Channel sender transmitting incoming datagrams to the session worker task.
    pub sender: tokio::sync::mpsc::Sender<bytes::Bytes>,
    /// Dynamic client peer address (supports seamless NAT rebinding and network migration).
    pub peer: Arc<parking_lot::RwLock<SocketAddr>>,
}

impl UdpSessionEntry {
    /// Creates a new session entry with the initial client peer address.
    #[inline]
    pub fn new(sender: tokio::sync::mpsc::Sender<bytes::Bytes>, peer_addr: SocketAddr) -> Self {
        Self {
            sender,
            peer: Arc::new(parking_lot::RwLock::new(peer_addr)),
        }
    }

    /// Updates the client peer address if it has changed (e.g. NAT rebinding).
    #[inline]
    pub fn update_peer_if_changed(&self, new_addr: SocketAddr) {
        if *self.peer.read() != new_addr {
            let mut lock = self.peer.write();
            if *lock != new_addr {
                *lock = new_addr;
            }
        }
    }
}

/// Result of acquiring or registering a session slot in [`UdpSessionTable`].
pub enum SessionAcquisition {
    /// An existing active session channel is already running.
    Existing(tokio::sync::mpsc::Sender<bytes::Bytes>),
    /// A new session was atomically registered; the caller must drive the receive loop.
    Created {
        /// Sender handle stored in the session table.
        sender: tokio::sync::mpsc::Sender<bytes::Bytes>,
        /// Receiver channel to consume incoming client datagrams.
        rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    },
    /// The session table has reached its maximum configured capacity.
    CapacityExceeded,
}

pub type UdpSessionShard = RwLock<FxHashMap<UdpSessionKey, UdpSessionEntry>>;

/// In-memory sharded session table tracking active bidirectional L4 UDP flows with zero cross-client contention.
pub struct UdpSessionTable {
    shards: Box<[UdpSessionShard]>,
    hash_builder: RandomState,
    mask: usize,
    active_sessions: AtomicUsize,
    max_sessions: usize,
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
        let max_sessions = velda_core::global_hardware_topology()
            .worker_threads
            .max(4)
            .saturating_mul(16_384)
            .clamp(16_384, 131_072);

        Self::with_capacity(count, max_sessions)
    }

    /// Creates a new sharded UDP session table with explicit shard count and capacity limit.
    pub fn with_capacity(shards_count: usize, max_sessions: usize) -> Self {
        let count = shards_count.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|_| RwLock::new(FxHashMap::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            shards,
            hash_builder: RandomState::new(),
            mask: count - 1,
            active_sessions: AtomicUsize::new(0),
            max_sessions,
        }
    }

    #[inline]
    fn shard_for(&self, key: &UdpSessionKey) -> &UdpSessionShard {
        let idx = (self.hash_builder.hash_one(key) as usize) & self.mask;
        &self.shards[idx]
    }

    /// Returns the maximum allowable active sessions across this table.
    #[inline]
    pub fn max_sessions(&self) -> usize {
        self.max_sessions
    }

    /// Atomically retrieves an existing session or registers a new session slot within `max_sessions`.
    ///
    /// Eliminates TOCTOU race conditions and rejects new sessions when the effective capacity is reached.
    pub fn get_or_create(&self, key: &UdpSessionKey, max_sessions: usize) -> SessionAcquisition {
        let shard = self.shard_for(key);
        let cap = max_sessions.min(self.max_sessions);

        // 1. Read-lock fast path: return active session without allocating
        if let Ok(guard) = shard.read() {
            if let Some(entry) = guard.get(key).filter(|e| !e.sender.is_closed()) {
                entry.update_peer_if_changed(key.client_addr);
                return SessionAcquisition::Existing(entry.sender.clone());
            }
            if !guard.contains_key(key) && self.active_sessions.load(Ordering::Acquire) >= cap {
                return SessionAcquisition::CapacityExceeded;
            }
        }

        // 2. Write-lock slow path: register or refresh channel slot atomically
        let Ok(mut guard) = shard.write() else {
            return SessionAcquisition::CapacityExceeded;
        };

        if let Some(entry) = guard.get(key).filter(|e| !e.sender.is_closed()) {
            entry.update_peer_if_changed(key.client_addr);
            return SessionAcquisition::Existing(entry.sender.clone());
        }

        let is_new = !guard.contains_key(key);
        if is_new {
            if self.active_sessions.load(Ordering::Acquire) >= cap {
                return SessionAcquisition::CapacityExceeded;
            }
            self.active_sessions.fetch_add(1, Ordering::Release);
        }

        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let entry = UdpSessionEntry::new(tx.clone(), key.client_addr);
        guard.insert(key.clone(), entry);
        SessionAcquisition::Created { sender: tx, rx }
    }

    /// Fast-path packet forwarding under the shard read lock without cloning `Sender` or `Arc<RwLock<SocketAddr>>`.
    ///
    /// Returns:
    /// - `Ok(true)`: Datagram delivered to the active session channel and peer updated if changed.
    /// - `Ok(false)`: Session exists and is alive, but its 128-slot ring buffer is full (drop packet).
    /// - `Err(data)`: Session does not exist or its channel is closed; returns ownership of `data` for new session creation.
    #[inline]
    pub fn try_forward(
        &self,
        key: &UdpSessionKey,
        client_addr: SocketAddr,
        data: bytes::Bytes,
    ) -> Result<bool, bytes::Bytes> {
        let Ok(guard) = self.shard_for(key).read() else {
            return Err(data);
        };
        let Some(entry) = guard.get(key) else {
            return Err(data);
        };
        match entry.sender.try_send(data) {
            Ok(()) => {
                entry.update_peer_if_changed(client_addr);
                Ok(true)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(recovered)) => Err(recovered),
        }
    }

    /// Retrieves an active sender channel for the given session key.
    pub fn get_sender(
        &self,
        key: &UdpSessionKey,
    ) -> Option<tokio::sync::mpsc::Sender<bytes::Bytes>> {
        let entry = self.shard_for(key).read().ok()?.get(key).cloned()?;
        if entry.sender.is_closed() {
            None
        } else {
            Some(entry.sender)
        }
    }

    /// Retrieves the active session entry for the given session key.
    pub fn get_session(&self, key: &UdpSessionKey) -> Option<UdpSessionEntry> {
        let entry = self.shard_for(key).read().ok()?.get(key).cloned()?;
        if entry.sender.is_closed() {
            None
        } else {
            Some(entry)
        }
    }

    /// Retrieves the shared dynamic peer address handle for the given session key.
    pub fn get_peer(&self, key: &UdpSessionKey) -> Option<Arc<parking_lot::RwLock<SocketAddr>>> {
        let entry = self.shard_for(key).read().ok()?.get(key).cloned()?;
        Some(entry.peer)
    }

    /// Seamlessly updates the client peer socket address for an active session (e.g. NAT rebinding).
    pub fn update_peer(&self, key: &UdpSessionKey, new_addr: SocketAddr) -> bool {
        if let Some(entry) = self.get_session(key) {
            entry.update_peer_if_changed(new_addr);
            true
        } else {
            false
        }
    }

    /// Inserts an active session into the appropriate shard (defaults peer address to key.client_addr).
    pub fn insert(&self, key: UdpSessionKey, sender: tokio::sync::mpsc::Sender<bytes::Bytes>) {
        let peer_addr = key.client_addr;
        let entry = UdpSessionEntry::new(sender, peer_addr);
        self.insert_entry(key, entry);
    }

    /// Inserts an active session entry into the appropriate shard.
    pub fn insert_entry(&self, key: UdpSessionKey, entry: UdpSessionEntry) {
        if let Ok(mut lock) = self.shard_for(&key).write()
            && lock.insert(key, entry).is_none()
        {
            self.active_sessions.fetch_add(1, Ordering::Release);
        }
    }

    /// Removes a session from the table upon completion or idle timeout.
    pub fn remove(&self, key: &UdpSessionKey) {
        let shard = self.shard_for(key);
        if let Ok(mut lock) = shard.write()
            && lock.remove(key).is_some()
        {
            self.active_sessions.fetch_sub(1, Ordering::Release);
        }
    }

    /// Returns the number of currently active UDP sessions across all shards.
    #[inline]
    pub fn active_session_count(&self) -> usize {
        self.active_sessions.load(Ordering::Acquire)
    }

    /// Sets the maximum allowable active sessions.
    pub fn set_max_sessions(&mut self, max: usize) {
        self.max_sessions = max;
    }

    /// Clears all sessions across all shards.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            if let Ok(mut lock) = shard.write() {
                lock.clear();
            }
        }
        self.active_sessions.store(0, Ordering::Release);
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
    config: L4UdpConfig,
    rt: &Runtime,
) {
    // 1. Ingress Validation & Anti-Reflection Filter: Drop port 0, unspecified, multicast, broadcast,
    // or self-directed loopback datagrams to prevent UDP ping-pong reflection amplification loops.
    let client_addr = datagram.peer();
    let client_ip = client_addr.ip();
    let client_port = client_addr.port();

    if client_port == 0
        || client_ip.is_unspecified()
        || client_ip.is_multicast()
        || (match client_ip {
            std::net::IpAddr::V4(v4) => v4.is_broadcast(),
            std::net::IpAddr::V6(_) => false,
        })
        || client_addr == datagram.local_addr()
    {
        tracing::trace!(peer = %client_addr, "Dropping invalid/bogon/loopback UDP datagram");
        return;
    }

    // 2. Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::trace!(
            listener = %listener_id,
            peer = %client_addr,
            overload = ?overload_lvl,
            "Shedding L4 UDP datagram due to memory saturation"
        );
        return;
    }

    let initial_data = datagram.into_bytes();

    // 3. Route Resolution (O(1) in-memory lookup)
    let Some(route) = rt.router.route_udp(&listener_id) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %client_addr,
            "No L4 UDP route configured for listener; dropping datagram"
        );
        return;
    };

    // 4. Session Key Construction (Opt-in Token Affinity vs Pure L4 Client Address)
    // Move `listener_id` directly into `key` to avoid an atomic refcount increment on every packet.
    let token = route.session_affinity.extract_token(&initial_data);
    let key = match token {
        Some(tok) => UdpSessionKey::token(listener_id, client_addr, tok),
        None => UdpSessionKey::peer(listener_id, client_addr),
    };
    let session_table = get_udp_session_table();

    // 5. Fast Path: Check existing bidirectional session immediately under shard read guard.
    // Zero Arc clones on the hot path; updates downstream peer only after successful enqueue.
    let initial_data = match session_table.try_forward(&key, client_addr, initial_data) {
        Ok(true) => return,
        Ok(false) => {
            tracing::trace!(
                listener = %key.listener_id,
                peer = %client_addr,
                "UDP session buffer full; dropping datagram"
            );
            return;
        }
        Err(recovered) => recovered,
    };

    // 6. Upstream Backend Resolution
    let Some(up) = rt
        .upstreams
        .raw
        .get(&route.upstream_name)
        .and_then(|u| u.as_udp())
    else {
        tracing::error!(
            listener = %key.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %client_addr,
            "No backend upstream available in Raw UDP upstream table; dropping datagram"
        );
        return;
    };

    // 7. Upstream Physical Endpoint Selection with LB Affinity Context
    let selection_ctx = match token {
        Some(tok) => velda_lb::SelectionContext::with_hash(tok),
        None => velda_lb::SelectionContext::with_client_ip(client_addr),
    };

    let Some(target_addr) = up.select_target_with_context(&selection_ctx) else {
        tracing::error!(
            listener = %key.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %client_addr,
            "No backend endpoints available for L4 UDP upstream; dropping datagram"
        );
        return;
    };

    // 8. Unidirectional Mode (1 chiều): fire-and-forget datagram forwarding
    if route.is_unidirectional() {
        tracing::debug!(
            listener = %key.listener_id,
            target = %target_addr,
            peer = %client_addr,
            "Forwarding UDP datagram unidirectionally"
        );
        if let Err(e) = socket.send_to(&initial_data, target_addr).await {
            tracing::trace!(
                error = %e,
                listener = %key.listener_id,
                target = %target_addr,
                peer = %client_addr,
                "Failed to forward UDP datagram (unidirectional)"
            );
        }
        return;
    }

    // 9. Bidirectional Flow: Allocate or retrieve conntrack session slot
    let (mut rx, initial_bytes) = match session_table.get_or_create(&key, config.max_sessions) {
        SessionAcquisition::Existing(sender) => {
            // Concurrent race: another worker created the session in parallel
            let _ = sender.try_send(initial_data);
            return;
        }
        SessionAcquisition::Created { sender: _, rx } => (rx, initial_data),
        SessionAcquisition::CapacityExceeded => {
            tracing::trace!(
                listener = %key.listener_id,
                peer = %client_addr,
                max = config.max_sessions,
                "L4 UDP session table capacity exceeded; dropping datagram"
            );
            return;
        }
    };

    let session_peer = session_table
        .get_peer(&key)
        .unwrap_or_else(|| Arc::new(parking_lot::RwLock::new(client_addr)));
    let downstream_socket = socket.clone();
    let session_key = key;

    tracing::debug!(
        listener = %session_key.listener_id,
        target = %target_addr,
        client = %client_addr,
        token = ?token,
        "Established new bidirectional L4 UDP session"
    );

    let up_clone = Arc::clone(up);
    let unassured_timeout = config.unassured_timeout;
    let idle_timeout = route.udp_idle_timeout.unwrap_or(Duration::from_secs(30));

    tokio::spawn(async move {
        let upstream_socket = match up_clone.connect_socket(target_addr) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    listener = %session_key.listener_id,
                    target = %target_addr,
                    "Failed to connect accelerated upstream UDP socket for L4 session"
                );
                get_udp_session_table().remove(&session_key);
                return;
            }
        };

        // Send the initial datagram to upstream
        if let Err(e) = upstream_socket.send(&initial_bytes).await {
            tracing::warn!(
                error = %e,
                listener = %session_key.listener_id,
                target = %target_addr,
                "Failed to send initial UDP datagram to upstream"
            );
            get_udp_session_table().remove(&session_key);
            return;
        }

        let has_activity = AtomicBool::new(false);
        let mut assured = false;
        let mut unassured_elapsed = Duration::ZERO;
        let mut idle_elapsed = Duration::ZERO;

        let mut watchdog_interval = tokio::time::interval(Duration::from_millis(500));
        watchdog_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Client -> Gateway -> Upstream Backend (Drains batch of up to 32 datagrams via sendmmsg)
                maybe_data = rx.recv() => {
                    match maybe_data {
                        Some(first_data) => {
                            let mut batch: [bytes::Bytes; 32] = Default::default();
                            batch[0] = first_data;
                            let mut batch_count = 1;
                            while batch_count < 32 {
                                match rx.try_recv() {
                                    Ok(d) => {
                                        batch[batch_count] = d;
                                        batch_count += 1;
                                    }
                                    Err(_) => break,
                                }
                            }

                            send_batch_to_socket(&upstream_socket, &batch[..batch_count]).await;
                            has_activity.store(true, Ordering::Relaxed);
                        }
                        None => break,
                    }
                }
                // Upstream Backend -> Gateway -> Client (Zero task buffer; uses Thread-Local 64KB GRO slab)
                ready_res = upstream_socket.readable() => {
                    if ready_res.is_err() {
                        break;
                    }

                    let mut drained_any = false;
                    let mut fallback_packet: Option<(bytes::Bytes, Option<u16>)> = None;
                    let current_peer = *session_peer.read();

                    GRO_SCRATCH.with(|cell| {
                        let mut buf = cell.borrow_mut();
                        while let Ok(Some((n, gso_size))) = try_recv_gro(&upstream_socket, &mut buf[..]) {
                            if n == 0 {
                                continue;
                            }
                            drained_any = true;
                            // Fast path: non-blocking GSO / batch send directly to downstream socket
                            match downstream_socket.try_send_gso_aware(&buf[..n], gso_size, current_peer) {
                                Some(Ok(_)) => {}
                                Some(Err(e)) => {
                                    tracing::trace!(
                                        error = %e,
                                        client = %current_peer,
                                        "Failed forwarding UDP reply back to client"
                                    );
                                    break;
                                }
                                None => {
                                    // Send buffer temporarily full: copy packet and exit TLS borrow to await backpressure
                                    fallback_packet = Some((bytes::Bytes::copy_from_slice(&buf[..n]), gso_size));
                                    break;
                                }
                            }
                        }
                    });

                    if let Some((pkt, gso_size)) = fallback_packet
                        && let Err(e) = downstream_socket
                            .send_gso_aware(&pkt, gso_size, current_peer)
                            .await
                    {
                        tracing::trace!(
                            error = %e,
                            client = %current_peer,
                            "Failed forwarding UDP reply back to client with backpressure"
                        );
                        break;
                    }

                    if drained_any {
                        assured = true;
                        has_activity.store(true, Ordering::Relaxed);
                    }
                }
                // Periodic Watchdog: Two-tier conntrack timeout using low-cost AtomicBool
                _ = watchdog_interval.tick() => {
                    let active = has_activity.swap(false, Ordering::Relaxed);
                    if assured && active {
                        idle_elapsed = Duration::ZERO;
                        continue;
                    }

                    let (elapsed, limit) = match assured {
                        true => {
                            idle_elapsed += Duration::from_millis(500);
                            (idle_elapsed, idle_timeout)
                        }
                        false => {
                            unassured_elapsed += Duration::from_millis(500);
                            (unassured_elapsed, unassured_timeout)
                        }
                    };

                    if elapsed >= limit {
                        let current_peer = *session_peer.read();
                        tracing::debug!(
                            client = %current_peer,
                            target = %target_addr,
                            assured,
                            "UDP L4 session timeout elapsed ({limit:?}); closing session"
                        );
                        break;
                    }
                }
            }
        }

        get_udp_session_table().remove(&session_key);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udp_session_table_capacity_enforcement() {
        let table = UdpSessionTable::with_capacity(4, 2);
        let key1 = UdpSessionKey::peer(Arc::from("l1"), "127.0.0.1:10001".parse().unwrap());
        let key2 = UdpSessionKey::peer(Arc::from("l1"), "127.0.0.1:10002".parse().unwrap());
        let key3 = UdpSessionKey::peer(Arc::from("l1"), "127.0.0.1:10003".parse().unwrap());

        let cap = table.max_sessions();
        let (_rx1, _tx1) = match table.get_or_create(&key1, cap) {
            SessionAcquisition::Created { sender, rx } => (rx, sender),
            _ => panic!("Expected Created for key1"),
        };
        assert_eq!(table.active_session_count(), 1);

        let (_rx2, _tx2) = match table.get_or_create(&key2, cap) {
            SessionAcquisition::Created { sender, rx } => (rx, sender),
            _ => panic!("Expected Created for key2"),
        };
        assert_eq!(table.active_session_count(), 2);

        // Third session must hit capacity limit
        match table.get_or_create(&key3, cap) {
            SessionAcquisition::CapacityExceeded => {}
            _ => panic!("Expected CapacityExceeded for key3 when max_sessions=2"),
        }
        assert_eq!(table.active_session_count(), 2);

        // Existing active session should still be retrievable
        match table.get_or_create(&key1, cap) {
            SessionAcquisition::Existing(_) => {}
            _ => panic!("Expected Existing for key1"),
        }

        // After removal of key1, slot is available
        table.remove(&key1);
        assert_eq!(table.active_session_count(), 1);

        let (_rx3, _tx3) = match table.get_or_create(&key3, cap) {
            SessionAcquisition::Created { sender, rx } => (rx, sender),
            _ => panic!("Expected Created for key3 after removing key1"),
        };
        assert_eq!(table.active_session_count(), 2);

        // Test stale session replacement: drop rx3
        drop(_rx3);
        // key3 is now closed, calling get_or_create(&key3) should replace in-place
        let (_rx3_new, _tx3_new) = match table.get_or_create(&key3, cap) {
            SessionAcquisition::Created { sender, rx } => (rx, sender),
            _ => panic!("Expected Created (in-place replacement) for stale key3"),
        };
        assert_eq!(table.active_session_count(), 2);
    }

    #[test]
    fn test_udp_session_table_clear() {
        let table = UdpSessionTable::with_capacity(4, 10);
        let key = UdpSessionKey::peer(Arc::from("l1"), "127.0.0.1:10001".parse().unwrap());
        let cap = table.max_sessions();
        let _ = table.get_or_create(&key, cap);
        assert_eq!(table.active_session_count(), 1);

        table.clear();
        assert_eq!(table.active_session_count(), 0);
    }

    #[test]
    fn test_udp_session_table_max_sessions_override() {
        // Table capacity is 100, but per-listener config limit is 1
        let table = UdpSessionTable::with_capacity(4, 100);
        let key1 = UdpSessionKey::peer(Arc::from("l1"), "127.0.0.1:20001".parse().unwrap());
        let key2 = UdpSessionKey::peer(Arc::from("l1"), "127.0.0.1:20002".parse().unwrap());

        match table.get_or_create(&key1, 1) {
            SessionAcquisition::Created { .. } => {}
            _ => panic!("Expected key1 to be created"),
        }
        assert_eq!(table.active_session_count(), 1);

        // key2 should hit per-listener limit of 1
        match table.get_or_create(&key2, 1) {
            SessionAcquisition::CapacityExceeded => {}
            _ => panic!("Expected key2 to exceed per-listener capacity"),
        }
    }

    #[test]
    fn test_udp_session_affinity_pure_l4_default() {
        let listener: Arc<str> = Arc::from("game-l4");
        let client_a: SocketAddr = "10.0.0.1:50000".parse().unwrap();
        let client_b: SocketAddr = "10.0.0.2:50000".parse().unwrap();

        let key_a = UdpSessionKey::peer(Arc::clone(&listener), client_a);
        let key_b = UdpSessionKey::peer(Arc::clone(&listener), client_b);

        // Pure L4 keys with different client addresses must not be equal
        assert_ne!(key_a, key_b);

        let table = UdpSessionTable::with_capacity(4, 100);
        let _ = table.get_or_create(&key_a, 100);
        let _ = table.get_or_create(&key_b, 100);

        assert_eq!(table.active_session_count(), 2);
    }

    #[test]
    fn test_udp_session_affinity_token_extraction() {
        use velda_router::UdpSessionAffinity;

        // Default pure L4 extracts nothing
        let default_affinity = UdpSessionAffinity::ClientAddr;
        let payload = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09];
        assert_eq!(default_affinity.extract_token(&payload), None);

        // Token affinity at offset 0, len 4 (u32 LE)
        let token_affinity = UdpSessionAffinity::PayloadToken { offset: 0, len: 4 };
        let tok = token_affinity.extract_token(&payload).unwrap();
        assert_eq!(tok, u32::from_le_bytes([0x01, 0x02, 0x03, 0x04]) as u64);

        // Token affinity at offset 2, len 8 (u64 LE)
        let token_affinity_64 = UdpSessionAffinity::PayloadToken { offset: 2, len: 8 };
        let payload_64 = [0xAA, 0xBB, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let tok_64 = token_affinity_64.extract_token(&payload_64).unwrap();
        assert_eq!(
            tok_64,
            u64::from_le_bytes([0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88])
        );

        // Insufficient payload length returns None
        let short_payload = [0x01, 0x02];
        assert_eq!(token_affinity_64.extract_token(&short_payload), None);
    }

    #[test]
    fn test_udp_nat_rebinding_migration() {
        let listener: Arc<str> = Arc::from("game-matchmaker");
        let initial_client: SocketAddr = "192.168.1.50:41234".parse().unwrap();
        let migrated_client: SocketAddr = "100.64.0.1:58999".parse().unwrap(); // NAT rebind
        let match_ticket: u64 = 0xCAFEBABE_DEADBEEF;

        let key_initial = UdpSessionKey::token(Arc::clone(&listener), initial_client, match_ticket);
        let key_migrated =
            UdpSessionKey::token(Arc::clone(&listener), migrated_client, match_ticket);

        // With token affinity, both keys represent the identical game session despite address drift!
        assert_eq!(key_initial, key_migrated);

        let table = UdpSessionTable::with_capacity(4, 100);

        // 1. Initial player connects
        let rx = match table.get_or_create(&key_initial, 100) {
            SessionAcquisition::Created { rx, .. } => rx,
            _ => panic!("Expected session creation"),
        };
        assert_eq!(table.active_session_count(), 1);

        let peer_handle = table.get_peer(&key_initial).unwrap();
        assert_eq!(*peer_handle.read(), initial_client);

        // 2. Client encounters NAT port rebind or WiFi -> 5G network migration
        // Gateway receives packet from new migrated_client carrying the same match_ticket
        let session = table
            .get_session(&key_migrated)
            .expect("Session must exist");
        session.update_peer_if_changed(migrated_client);

        // 3. Dynamic peer address is updated seamlessly without tearing down the stream or channel
        assert_eq!(*peer_handle.read(), migrated_client);
        assert_eq!(table.active_session_count(), 1);

        // Retain rx to verify channel was undisturbed
        drop(rx);
    }
}
