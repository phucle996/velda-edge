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

use velda_core::RouteId;
use velda_rdma::{RdmaError, RdmaSlabClass};
use velda_transport::Connection;

use crate::runtime::pipeline::L4TcpConfig;
use crate::upstream::rdma::RdmaUpstream;

/// Serves a raw L4 TCP stream forwarded to an upstream RDMA kernel-bypass backend.
pub async fn serve(
    conn: Connection,
    upstream: &Arc<RdmaUpstream>,
    _config: &L4TcpConfig,
    route_id: RouteId,
    upstream_name: &str,
    peer: SocketAddr,
    listener_id: &str,
) {
    tracing::debug!(
        listener = %listener_id,
        route = %route_id,
        upstream = %upstream_name,
        peer = %peer,
        "Proxying L4 TCP stream to RDMA kernel-bypass upstream via zero-copy arena"
    );

    // Step 1: Lease a pre-warmed RDMA channel from the core-local lane shard.
    // Conforms to AGENTS.md Sec 2.9: Frontline worker does not perform handshakes or block on warmup.
    let lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream_name,
                peer = %peer,
                "Failed to acquire RDMA lease for L4 raw forwarding"
            );
            return;
        }
    };

    // Step 2: Unpack downstream TCP client socket stream and split into directional halves.
    let (mut client_reader, mut client_writer) = tokio::io::split(conn.stream);
    let idle_timeout = upstream.inner.timeouts().idle;

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
                // Client reached EOF
                break;
            }

            lease
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
            let payload = match lease.recv(idle_timeout).await {
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

    tokio::select! {
        c2s_res = &mut client_to_rdma => {
            match c2s_res {
                Ok(c2s) => {
                    // Client finished writing; drain remaining upstream responses up to idle timeout
                    match tokio::time::timeout(idle_timeout, &mut rdma_to_client).await {
                        Ok(Ok(s2c)) => {
                            tracing::debug!(
                                upstream = %upstream_name,
                                peer = %peer,
                                client_to_server = c2s,
                                server_to_client = s2c,
                                "L4 TCP to RDMA stream forwarding completed"
                            );
                        }
                        Ok(Err(e)) => {
                            lease.mark_broken();
                            tracing::warn!(
                                error = %e,
                                upstream = %upstream_name,
                                peer = %peer,
                                "RDMA receive failed after client EOF"
                            );
                        }
                        Err(_) => {
                            tracing::debug!(
                                upstream = %upstream_name,
                                peer = %peer,
                                client_to_server = c2s,
                                "Idle timeout awaiting final RDMA response; stream closed"
                            );
                        }
                    }
                }
                Err(e) => {
                    lease.mark_broken();
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream_name,
                        peer = %peer,
                        "L4 TCP client read terminated with error"
                    );
                }
            }
        }
        s2c_res = &mut rdma_to_client => {
            match s2c_res {
                Ok(s2c) => {
                    tracing::debug!(
                        upstream = %upstream_name,
                        peer = %peer,
                        server_to_client = s2c,
                        "RDMA upstream terminated forwarding"
                    );
                }
                Err(e) => {
                    lease.mark_broken();
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream_name,
                        peer = %peer,
                        "RDMA to TCP stream forwarding terminated with error"
                    );
                }
            }
        }
    }
}
