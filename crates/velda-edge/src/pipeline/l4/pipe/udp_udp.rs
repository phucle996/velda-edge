//! # Canonical Pipeline Classification: Standard Pipe (Kernel Network Stack: UDP Datagrams)
//!
//! ## Architectural Mechanism & Operating Principles
//!
//! Unlike **Acceleration Pipes** (`*_udp_rdma`) that bypass the OS kernel via hardware RDMA Verbs,
//! this is a **Standard Pipe** routing UDP datagrams through the Linux kernel network stack using
//! pre-compiled socket acceleration flags (`SO_REUSEPORT`, UDP GRO `UDP_GRO`, GSO `UDP_SEGMENT`,
//! and `sendmmsg(2)` batching).
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to a raw upstream backend configured with `protocol.transport = "udp"`.
//! 2. **Unidirectional Forwarding (1-way)**: Fire-and-forget datagram forwarding directly via `socket.send_to(target_addr)`
//!    without allocating session state.
//!    - Ideal for telemetry, syslog, metrics, and one-way media streams.
//! 3. **Bidirectional Forwarding (2-way)**: Stateful flow/session tracking via [`UdpSessionTable`]
//!    with ephemeral upstream sockets connected to the selected target, automatic response routing
//!    back to client, and two-tier idle eviction (`unassured` vs `assured`).
//!    - Zero memory allocations for buffers inside session tasks (uses thread-local 64KB GRO scratch buffer).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow (`AGENTS.md` Sec 2.1–2.3).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use velda_router::UdpRoute;
use velda_transport::UdpSocket;

use super::super::udp::{SessionAcquisition, UdpSessionKey, get_udp_session_table};
use crate::runtime::pipeline::L4UdpConfig;
use crate::upstream::raw::UdpUpstream;

/// Thread-local scratch buffer size for UDP Generic Receive Offload (64KB).
pub const GRO_SCRATCH_SIZE: usize = 65536;
/// Maximum datagram batch size sent in a single sendmmsg syscall.
pub const MAX_SENDMMSG_BATCH: usize = 32;
/// Default session idle timeout when not specified on the UDP route.
pub const DEFAULT_UDP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Periodic watchdog evaluation interval for UDP session tracking.
pub const WATCHDOG_INTERVAL: Duration = Duration::from_millis(500);
/// Time step added to session idle elapsed counters on each watchdog tick.
pub const WATCHDOG_TICK_STEP: Duration = Duration::from_millis(500);

thread_local! {
    /// 64KB thread-local scratch buffer per CPU core for kernel GRO demuxing.
    ///
    /// Allocated on heap once per worker thread, keeping it permanently warm in L1/L2 cache.
    /// Eliminates buffer allocations inside individual session tasks (0 bytes buffer per session).
    static GRO_SCRATCH: std::cell::RefCell<Box<[u8; GRO_SCRATCH_SIZE]>> =
        std::cell::RefCell::new(vec![0u8; GRO_SCRATCH_SIZE].into_boxed_slice().try_into().unwrap());
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

/// Serves an L4 UDP datagram flow forwarded to a standard kernel UDP upstream backend.
pub async fn serve(
    downstream_socket: Arc<UdpSocket>,
    upstream: &Arc<UdpUpstream>,
    key: UdpSessionKey,
    initial_bytes: bytes::Bytes,
    route: &UdpRoute,
    config: &L4UdpConfig,
) {
    let client_addr = key.client_addr;
    let token = key.token;
    let is_unidirectional = route.is_unidirectional();
    let udp_idle_timeout = route.udp_idle_timeout;
    let route_id = route.id;
    let upstream_name = &route.upstream_name;
    // Step 1: Resolve physical backend target endpoint using load balancer context.
    // Uses token hash affinity if available; falls back to client IP affinity.
    let selection_ctx = match token {
        Some(tok) => velda_lb::SelectionContext::with_hash(tok),
        None => velda_lb::SelectionContext::with_client_ip(client_addr),
    };

    let Some(target_addr) = upstream.select_target_with_context(&selection_ctx) else {
        tracing::error!(
            listener = %key.listener_id,
            route = %route_id,
            upstream = %upstream_name,
            peer = %client_addr,
            "No backend endpoints available for L4 UDP upstream; dropping datagram"
        );
        return;
    };

    // Step 2: Unidirectional Mode: Fire-and-forget datagram forwarding.
    // Dispatches directly via downstream socket to avoid allocating session tracking state.
    if is_unidirectional {
        tracing::debug!(
            listener = %key.listener_id,
            target = %target_addr,
            peer = %client_addr,
            "Forwarding UDP datagram unidirectionally"
        );
        if let Err(e) = downstream_socket.send_to(&initial_bytes, target_addr).await {
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

    // Step 3: Bidirectional Flow: Allocate or retrieve conntrack session slot.
    // Concurrency race handling: if another worker initialized the session in parallel,
    // forward initial bytes to the existing channel directly.
    let session_table = get_udp_session_table();
    let (mut rx, initial_bytes) = match session_table.get_or_create(&key, config.max_sessions) {
        SessionAcquisition::Existing(sender) => {
            let _ = sender.try_send(initial_bytes);
            return;
        }
        SessionAcquisition::Created { sender: _, rx } => (rx, initial_bytes),
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

    // Step 4: Extract shared client peer address handle to support dynamic NAT rebinding.
    let session_peer = session_table
        .get_peer(&key)
        .unwrap_or_else(|| Arc::new(parking_lot::RwLock::new(client_addr)));
    let session_key = key;

    tracing::debug!(
        listener = %session_key.listener_id,
        target = %target_addr,
        client = %client_addr,
        token = ?token,
        "Established new bidirectional L4 UDP session"
    );

    let up_clone = Arc::clone(upstream);
    let unassured_timeout = config.unassured_timeout;
    let idle_timeout = udp_idle_timeout.unwrap_or(DEFAULT_UDP_IDLE_TIMEOUT);

    // Step 5: Spawn background session worker driving bidirectional packet pumping.
    tokio::spawn(async move {
        // Step 5a: Open accelerated kernel UDP socket connected to upstream target.
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

        // Step 5b: Send initial datagram to prime the upstream connection.
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

        let mut watchdog_interval = tokio::time::interval(WATCHDOG_INTERVAL);
        watchdog_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Step 5c: Bidirectional event loop multiplexing client datagrams, upstream replies, and watchdog.
        loop {
            tokio::select! {
                // Leg 1: Client -> Gateway -> Upstream Backend (Drains batches of up to 32 datagrams via sendmmsg)
                maybe_data = rx.recv() => {
                    match maybe_data {
                        Some(first_data) => {
                            let mut batch: [bytes::Bytes; MAX_SENDMMSG_BATCH] = Default::default();
                            batch[0] = first_data;
                            let mut batch_count = 1;
                            while batch_count < MAX_SENDMMSG_BATCH {
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
                // Leg 2: Upstream Backend -> Gateway -> Client (Zero task buffer; uses Thread-Local 64KB GRO slab)
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
                // Leg 3: Periodic Watchdog: Two-tier conntrack timeout using low-cost AtomicBool
                _ = watchdog_interval.tick() => {
                    let active = has_activity.swap(false, Ordering::Relaxed);
                    if assured && active {
                        idle_elapsed = Duration::ZERO;
                        continue;
                    }

                    let (elapsed, limit) = match assured {
                        true => {
                            idle_elapsed += WATCHDOG_TICK_STEP;
                            (idle_elapsed, idle_timeout)
                        }
                        false => {
                            unassured_elapsed += WATCHDOG_TICK_STEP;
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

        // Step 6: Evict session key from table on task exit.
        get_udp_session_table().remove(&session_key);
    });
}
