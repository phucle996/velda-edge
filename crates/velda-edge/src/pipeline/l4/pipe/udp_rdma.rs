//! # Canonical Pipeline Classification: Acceleration Pipe (Hardware Kernel-Bypass)
//!
//! ## Architectural Mechanism & Operating Principles
//!
//! Unlike **Standard Pipes** (`*_udp_udp`) that route outbound traffic through the Linux kernel
//! UDP socket stack, this is an **Acceleration Pipe** utilizing `velda-rdma` to bypass the OS kernel
//! entirely on the upstream transport leg. Outbound datagram payloads are dispatched via RDMA
//! Work Requests directly between host Memory Region buffers and backend network adapters using RoCEv2
//! or InfiniBand Reliable Connected (RC) Queue Pairs.
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to a raw upstream backend configured with `protocol.transport = "rdma"`.
//! 2. **Hardware & Driver Availability**: Host possesses an active RDMA NIC (`libibverbs.so.1`) or runs in verified software mock mode.
//! 3. **Core Lane Shard Leased**: Pre-warmed `RdmaLease` leased directly from the worker core lane shard
//!    without cross-thread lock contention.
//! 4. **Transport Bridge**: Adapts discrete UDP datagrams to discrete RDMA message transfers via `RdmaChannel`
//!    Work Requests (`send` / `recv`).
//!    - **Unidirectional Forwarding (1-way)**: Fire-and-forget datagram dispatch over an acquired RDMA channel.
//!    - **Bidirectional Forwarding (2-way)**: Stateful conntrack session tracking via [`UdpSessionTable`] bridging
//!      downstream UDP datagrams into RDMA Send Work Requests and inbound RDMA completions back as client UDP datagrams.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow (`AGENTS.md` Sec 2.1–2.3).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use velda_router::UdpRoute;
use velda_sock::UdpSocket;

use super::super::udp::{SessionAcquisition, UdpSessionKey, get_udp_session_table};
use crate::runtime::pipeline::L4UdpConfig;
use crate::upstream::rdma::RdmaUpstream;

/// Timeout for RDMA receive polling during bidirectional UDP session pump.
pub const RDMA_RECV_POLL_TIMEOUT: Duration = Duration::from_millis(100);
/// Periodic watchdog evaluation interval for UDP session tracking.
pub const WATCHDOG_INTERVAL: Duration = Duration::from_millis(500);
/// Time step added to session idle elapsed counters on each watchdog tick.
pub const WATCHDOG_TICK_STEP: Duration = Duration::from_millis(500);
/// Maximum datagrams drained opportunistically from channel per loop iteration.
pub const MAX_OPPORTUNISTIC_DRAIN: usize = 31;
/// Default session idle timeout when not specified on the UDP route.
pub const DEFAULT_UDP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Serves an L4 UDP datagram flow bridged to a hardware RDMA kernel-bypass upstream backend.
pub async fn serve(
    downstream_socket: Arc<UdpSocket>,
    upstream: &Arc<RdmaUpstream>,
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
    let upstream_id = route.upstream_id;
    tracing::debug!(
        listener = %key.listener_id,
        route = %route_id,
        upstream = %upstream_id,
        peer = %client_addr,
        token = ?token,
        is_unidirectional,
        "Proxying L4 UDP datagram to RDMA kernel-bypass upstream"
    );

    // Step 1: Unidirectional Mode: Fire-and-forget datagram forwarding over RDMA.
    // Acquires a pooled RDMA lease, posts a single Send Work Request, and immediately returns
    // the channel to the core lane pool upon drop.
    if is_unidirectional {
        let lease = match upstream.acquire().await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    upstream = %upstream_id,
                    peer = %client_addr,
                    "Failed to acquire RDMA channel for unidirectional UDP forwarding"
                );
                return;
            }
        };

        // Zero-copy path: lease pre-registered slot from RDMA arena to avoid intermediate memcpy.
        // Standard packets (<= 2KB) lease Standard2K; large GRO packets lease Large64K with single SGE.
        let send_res = if let Ok(mut slab) = upstream.lease_slab_for_size(initial_bytes.len()) {
            slab[..initial_bytes.len()].copy_from_slice(&initial_bytes);
            lease.sender.send_lease(&slab, initial_bytes.len()).await
        } else {
            lease.sender.send(&initial_bytes).await
        };

        if let Err(e) = send_res {
            lease.mark_broken();
            tracing::warn!(
                error = %e,
                upstream = %upstream_id,
                peer = %client_addr,
                "Failed to send unidirectional UDP datagram over RDMA channel"
            );
        }
        return;
    }

    // Step 2: Bidirectional Flow: Allocate or retrieve conntrack session slot.
    // Concurrency race handling: if another worker initialized the session in parallel,
    // forward initial bytes to the existing channel directly without re-spawning a task.
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

    // Step 3: Extract shared client peer address handle to support dynamic NAT rebinding.
    let session_peer = session_table
        .get_peer(&key)
        .unwrap_or_else(|| Arc::new(parking_lot::RwLock::new(client_addr)));
    let session_key = key;

    let up_clone = Arc::clone(upstream);
    let unassured_timeout = config.unassured_timeout;
    let idle_timeout = udp_idle_timeout.unwrap_or(DEFAULT_UDP_IDLE_TIMEOUT);

    // Step 4: Spawn background session worker driving bidirectional UDP <-> RDMA bridging.
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        let (request_id, _) = velda_core::next_request_id();
        let emit_log =
            |peer: std::net::SocketAddr,
             upstream_addr: Option<std::net::SocketAddr>,
             rx: u64,
             tx: u64,
             completion: velda_observability::LogCompletionReason| {
                let (upstream_ip, upstream_port) = upstream_addr
                    .map(|a| {
                        (
                            velda_observability::encode_upstream_ip_u32(a.ip()),
                            a.port(),
                        )
                    })
                    .unwrap_or((0, 0));
                velda_observability::global_log_store().emit(
                    &velda_observability::AccessLogRecord {
                        request_id: request_id.entropy(),
                        timestamp_sec: request_id.timestamp_secs() as u32,
                        duration_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                        bytes_rx: rx.min(u32::MAX as u64) as u32,
                        bytes_tx: tx.min(u32::MAX as u64) as u32,
                        upstream_ip,
                        peer_port: peer.port(),
                        upstream_port,
                        status_code: 0,
                        route_id: route_id.0 as u16,
                        ua_major_version: 0,
                        rpc_msgs_or_flags: 0,
                        pipe: velda_core::pipe::PipeKind::UdpRdma,
                        completion: completion as u8,
                        method: velda_observability::LogHttpMethod::Unknown as u8,
                        grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                        ua_family: velda_observability::UaFamily::None as u8,
                        ua_os: velda_observability::UaOs::Unknown as u8,
                        url_len: 0,
                        url_bytes: [0; 188],
                    },
                );
            };
        // Step 4a: Lease a pre-warmed RDMA channel from the local core lane shard.
        // Conforms to AGENTS.md Sec 2.9: Frontline worker does not block on warmup.
        let lease = match up_clone.acquire().await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    listener = %session_key.listener_id,
                    upstream = %upstream_id,
                    "Failed to acquire RDMA lease for bidirectional L4 UDP session"
                );
                get_udp_session_table().remove(&session_key);
                emit_log(
                    client_addr,
                    None,
                    0,
                    0,
                    velda_observability::LogCompletionReason::UpstreamError,
                );
                return;
            }
        };
        let upstream_addr = lease.endpoint();

        // Decouple RDMA send and receive halves ("minh bạch và tách biệt 2 luồng")
        let (rdma_sender, rdma_receiver) = lease.split();

        // Step 4b: Send initial datagram over the RDMA channel to prime the session.
        let send_res = if let Ok(mut slab) = up_clone.lease_slab_for_size(initial_bytes.len()) {
            slab[..initial_bytes.len()].copy_from_slice(&initial_bytes);
            rdma_sender.send_lease(&slab, initial_bytes.len()).await
        } else {
            rdma_sender.send(&initial_bytes).await
        };
        if let Err(e) = send_res {
            tracing::warn!(
                error = %e,
                listener = %session_key.listener_id,
                upstream = %upstream_id,
                "Failed to send initial UDP datagram over RDMA channel"
            );
            lease.mark_broken();
            get_udp_session_table().remove(&session_key);
            emit_log(
                client_addr,
                Some(upstream_addr),
                0,
                0,
                velda_observability::LogCompletionReason::UpstreamError,
            );
            return;
        }

        let mut bytes_rx = initial_bytes.len() as u64;
        let mut bytes_tx = 0u64;
        let mut completion = velda_observability::LogCompletionReason::Complete;
        let has_activity = AtomicBool::new(false);
        let mut assured = false;
        let mut unassured_elapsed = Duration::ZERO;
        let mut idle_elapsed = Duration::ZERO;

        let mut watchdog_interval = tokio::time::interval(WATCHDOG_INTERVAL);
        watchdog_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Step 4c: Bidirectional event loop bridging client datagrams, RDMA completions, and watchdog.
        loop {
            tokio::select! {
                // Leg 1: Client -> Gateway -> RDMA Upstream
                // Drains incoming client datagrams and posts RDMA Send Work Requests into the Queue Pair.
                maybe_data = rx.recv() => {
                    match maybe_data {
                        Some(first_data) => {
                            let send_res = if let Ok(mut slab) = up_clone.lease_slab_for_size(first_data.len()) {
                                slab[..first_data.len()].copy_from_slice(&first_data);
                                rdma_sender.send_lease(&slab, first_data.len()).await
                            } else {
                                rdma_sender.send(&first_data).await
                            };
                            if let Err(e) = send_res {
                                tracing::warn!(
                                    error = %e,
                                    listener = %session_key.listener_id,
                                    upstream = %upstream_id,
                                    "Failed posting RDMA Send Work Request for client datagram"
                                );
                                completion = velda_observability::LogCompletionReason::UpstreamError;
                                lease.mark_broken();
                                break;
                            }
                            bytes_rx += first_data.len() as u64;

                            // Opportunistically drain additional queued datagrams to amortize task wakeups
                            let mut drained = 0;
                            while drained < MAX_OPPORTUNISTIC_DRAIN {
                                match rx.try_recv() {
                                    Ok(data) => {
                                        let send_res = if let Ok(mut slab) = up_clone.lease_slab_for_size(data.len()) {
                                            slab[..data.len()].copy_from_slice(&data);
                                            rdma_sender.send_lease(&slab, data.len()).await
                                        } else {
                                            rdma_sender.send(&data).await
                                        };
                                        if let Err(e) = send_res {
                                            tracing::warn!(
                                                error = %e,
                                                listener = %session_key.listener_id,
                                                upstream = %upstream_id,
                                                "Failed posting batched RDMA Send Work Request"
                                            );
                                            completion = velda_observability::LogCompletionReason::UpstreamError;
                                            lease.mark_broken();
                                            break;
                                        }
                                        bytes_rx += data.len() as u64;
                                        drained += 1;
                                    }
                                    Err(_) => break,
                                }
                            }

                            has_activity.store(true, Ordering::Relaxed);
                        }
                        None => break,
                    }
                }
                // Leg 2: RDMA Upstream -> Gateway -> Downstream Client
                // Awaits RDMA Receive completion with a poll timeout and transmits back via downstream UDP socket.
                rdma_res = rdma_receiver.recv(RDMA_RECV_POLL_TIMEOUT) => {
                    match rdma_res {
                        Ok(resp_payload) => {
                            let current_peer = *session_peer.read();
                            if let Err(e) = downstream_socket.send_to(&resp_payload, current_peer).await {
                                tracing::trace!(
                                    error = %e,
                                    client = %current_peer,
                                    "Failed forwarding RDMA reply back to UDP client"
                                );
                                break;
                            }
                            bytes_tx += resp_payload.len() as u64;
                            assured = true;
                            has_activity.store(true, Ordering::Relaxed);
                        }
                        Err(velda_rdma::RdmaError::Timeout(_)) => {
                            // Non-fatal receive timeout: allows watchdog tick and cooperative task interleaving
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                listener = %session_key.listener_id,
                                upstream = %upstream_id,
                                "RDMA channel receive failure during L4 UDP session"
                            );
                            completion = velda_observability::LogCompletionReason::UpstreamError;
                            lease.mark_broken();
                            break;
                        }
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
                            upstream = %upstream_id,
                            assured,
                            "L4 UDP-to-RDMA session timeout elapsed ({limit:?}); closing session"
                        );
                        break;
                    }
                }
            }
        }

        // Step 5: Evict session key from table on task exit.
        // Mark channel broken to prevent cross-session datagram bleeding into pooled connections.
        let final_peer = *session_peer.read();
        lease.mark_broken();
        get_udp_session_table().remove(&session_key);
        emit_log(
            final_peer,
            Some(upstream_addr),
            bytes_rx,
            bytes_tx,
            completion,
        );
    });
}
