//! # Canonical Pipeline Classification: Acceleration Pipe (Hardware Kernel-Bypass)
//!
//! ## Architectural Mechanism & Operating Principles
//!
//! Unlike **Standard Pipes** (`*_tcp_tcp`) that route outbound traffic through the Linux kernel
//! network stack, this is an **Acceleration Pipe** utilizing `velda-rdma` to bypass the OS kernel entirely
//! on the upstream transport leg. Outbound byte streams are transferred via DMA directly between host
//! RAM and backend network adapters using RoCEv2 or InfiniBand Reliable Connected (RC) Queue Pairs.
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to a raw upstream backend configured with `protocol.transport = "rdma"`.
//! 2. **Hardware & Driver Availability**: Host possesses an active RDMA NIC (`libibverbs.so.1`) or runs in verified software mock mode.
//! 3. **Core Lane Shard Leased**: Pre-warmed `RdmaStream` leased directly from the worker core lane shard
//!    without cross-thread lock contention.
//! 4. **Transport Bridge**: Adapts continuous TCP stream sockets to discrete RDMA Memory Region buffers
//!    via the transparent `RdmaStream` adapter (`AsyncRead + AsyncWrite`).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow (`AGENTS.md` Sec 2.1–2.3).

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use velda_core::{RouteId, UpstreamId};
use velda_rdma::{RdmaError, RdmaSlabClass};
use velda_sock::Connection;

use crate::runtime::pipeline::L4TcpConfig;
use crate::upstream::rdma::RdmaUpstream;

/// Serves a raw L4 TCP stream forwarded to an upstream RDMA kernel-bypass backend.
pub async fn serve(
    conn: Connection,
    upstream: &Arc<RdmaUpstream>,
    _config: &L4TcpConfig,
    route_id: RouteId,
    upstream_id: UpstreamId,
    peer: SocketAddr,
    listener_id: &str,
) {
    tracing::debug!(
        listener = %listener_id,
        route = %route_id,
        upstream = %upstream_id,
        peer = %peer,
        "Proxying L4 TCP stream to RDMA kernel-bypass upstream via zero-copy arena"
    );

    let started = std::time::Instant::now();
    let (request_id, _) = velda_core::next_request_id();
    let emit_log = |upstream_addr: Option<SocketAddr>,
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
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
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
            pipe: velda_core::pipe::PipeKind::TcpRdma,
            completion: completion as u8,
            method: velda_observability::LogHttpMethod::Unknown as u8,
            grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
            ua_family: velda_observability::UaFamily::None as u8,
            ua_os: velda_observability::UaOs::Unknown as u8,
            url_len: 0,
            url_bytes: [0; 188],
        });
    };

    // Step 1: Lease a pre-warmed RDMA channel from the core-local lane shard.
    // Conforms to AGENTS.md Sec 2.9: Frontline worker does not perform handshakes or block on warmup.
    let lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream_id,
                peer = %peer,
                "Failed to acquire RDMA lease for L4 raw forwarding"
            );
            emit_log(
                None,
                0,
                0,
                velda_observability::LogCompletionReason::UpstreamError,
            );
            return;
        }
    };
    let upstream_addr = lease.endpoint();

    // Step 2: Unpack downstream TCP client socket stream and split into directional halves.
    let (mut client_reader, mut client_writer) = tokio::io::split(conn.stream);
    let idle_timeout = upstream.inner.timeouts().idle;

    // Transparently decouple RDMA into independent Send and Receive flows ("minh bạch và tách biệt 2 luồng")
    let (rdma_sender, rdma_receiver) = lease.split();

    // Step 3: Directional leg: Client TCP -> RDMA Upstream
    // Leases contiguous 64KB spans from the pre-registered arena, reading directly into pinned memory
    // and posting single-SGE Send WRs (`num_sge = 1`) with zero intermediate user-space copies.
    let client_to_rdma = async {
        let mut total_bytes = 0u64;
        loop {
            let mut slab = match upstream.lease_slab(RdmaSlabClass::Large64K) {
                Ok(s) => s,
                Err(_) => match upstream.lease_slab(RdmaSlabClass::Standard2K) {
                    Ok(s) => s,
                    Err(e) => return Err(std::io::Error::other(e.to_string())),
                },
            };

            let n = client_reader.read(&mut slab).await?;
            if n == 0 {
                // Client reached EOF; send explicit shutdown frame to peer
                let _ = rdma_sender.shutdown().await;
                break;
            }

            rdma_sender
                .send_lease(&slab, n)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            total_bytes += n as u64;
        }
        Ok::<u64, std::io::Error>(total_bytes)
    };

    // Step 4: Directional leg: RDMA Upstream -> Client TCP
    // Awaits RDMA completions and streams responses directly into the downstream TCP socket.
    let rdma_to_client = async {
        let mut total_bytes = 0u64;
        loop {
            let payload = match rdma_receiver.recv(idle_timeout).await {
                Ok(bytes) => bytes,
                Err(RdmaError::Disconnected(_)) => break,
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            };

            if payload.is_empty() {
                break;
            }

            client_writer.write_all(&payload).await?;
            total_bytes += payload.len() as u64;
        }
        client_writer.flush().await?;
        Ok::<u64, std::io::Error>(total_bytes)
    };

    // Step 5: Drive full-duplex forwarding with graceful half-close coordination.
    tokio::pin!(client_to_rdma);
    tokio::pin!(rdma_to_client);

    let mut rx_bytes = 0u64;
    let mut tx_bytes = 0u64;
    let mut completion = velda_observability::LogCompletionReason::Complete;

    tokio::select! {
        c2s_res = &mut client_to_rdma => {
            match c2s_res {
                Ok(c2s) => {
                    // Client finished writing; drain remaining upstream responses up to idle timeout
                    match tokio::time::timeout(idle_timeout, &mut rdma_to_client).await {
                        Ok(Ok(s2c)) => {
                            rx_bytes = c2s;
                            tx_bytes = s2c;
                            tracing::debug!(
                                upstream = %upstream_id,
                                peer = %peer,
                                client_to_server = c2s,
                                server_to_client = s2c,
                                "L4 TCP to RDMA stream forwarding completed"
                            );
                        }
                        Ok(Err(e)) => {
                            rx_bytes = c2s;
                            completion = velda_observability::LogCompletionReason::UpstreamError;
                            lease.mark_broken();
                            tracing::warn!(
                                error = %e,
                                upstream = %upstream_id,
                                peer = %peer,
                                "RDMA receive failed after client EOF"
                            );
                        }
                        Err(_) => {
                            rx_bytes = c2s;
                            completion = velda_observability::LogCompletionReason::Timeout;
                            lease.mark_broken();
                            tracing::debug!(
                                upstream = %upstream_id,
                                peer = %peer,
                                client_to_server = c2s,
                                "Idle timeout awaiting final RDMA response; stream closed"
                            );
                        }
                    }
                }
                Err(e) => {
                    completion = velda_observability::LogCompletionReason::ClientCancel;
                    lease.mark_broken();
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream_id,
                        peer = %peer,
                        "L4 TCP client read terminated with error"
                    );
                }
            }
        }
        s2c_res = &mut rdma_to_client => {
            match s2c_res {
                Ok(s2c) => {
                    tx_bytes = s2c;
                    tracing::debug!(
                        upstream = %upstream_id,
                        peer = %peer,
                        server_to_client = s2c,
                        "RDMA upstream terminated forwarding"
                    );
                }
                Err(e) => {
                    completion = velda_observability::LogCompletionReason::UpstreamError;
                    lease.mark_broken();
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream_id,
                        peer = %peer,
                        "RDMA to TCP stream forwarding terminated with error"
                    );
                }
            }
        }
    }

    emit_log(Some(upstream_addr), rx_bytes, tx_bytes, completion);

    // Raw L4 TCP streams lack application framing delimiters.
    // Mark channel broken to prevent cross-session byte bleeding into pooled connections (CWE-444/CWE-456).
    lease.mark_broken();
}
