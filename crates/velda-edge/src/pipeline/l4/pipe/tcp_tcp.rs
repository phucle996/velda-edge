//! # Canonical Pipeline Classification: Standard Pipe (Kernel Network Stack)
//!
//! ## Architectural Mechanism & Operating Principles
//!
//! Unlike **Acceleration Pipes** (`*_tcp_rdma`) that bypass the OS kernel via hardware RDMA Verbs,
//! this is a **Standard Pipe** routing outbound byte traffic through the Linux kernel TCP network stack
//! using pre-compiled socket acceleration flags (`TcpAccelerationPath`: `TCP_NODELAY`, `TCP_FASTOPEN_CONNECT`,
//! `TCP_NOTSENT_LOWAT`, `SO_BUSY_POLL`, `TCP_QUICKACK`, and BBR congestion control).
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to a raw upstream backend configured with `protocol.transport = "tcp"`.
//! 2. **Transport & Framing**: Pure Layer 4 TCP byte stream proxying with zero TLS termination and zero application parsing.
//! 3. **Zero-Copy Acceleration**: On Linux platforms, `forward_connection` utilizes the `splice(2)` syscall
//!    to move byte streams directly between kernel socket pipe buffers without copying data into user-space RAM.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow (`AGENTS.md` Sec 2.1–2.3).

use std::net::SocketAddr;
use std::sync::Arc;

use velda_core::{RouteId, UpstreamId};
use velda_sock::Connection;

use crate::runtime::pipeline::L4TcpConfig;
use crate::upstream::raw::TcpUpstream;

/// Serves a raw L4 TCP stream forwarded to an upstream TCP backend.
pub async fn serve(
    conn: Connection,
    upstream: &Arc<TcpUpstream>,
    config: &L4TcpConfig,
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
        "Proxying L4 TCP stream to upstream backend via forward_connection (Zero TLS)"
    );

    // Step 1: Compute effective idle timeout and buffer sizing.
    // Falls back to the upstream pool's configured idle timeout if not overridden on the listener.
    let idle_timeout = config.idle_timeout.or(Some(upstream.timeouts().idle));
    let buffer_size = config.copy_buffer_size;

    let started = std::time::Instant::now();
    let (request_id, _) = velda_core::next_request_id();

    // Step 2: Dispatch connection from upstream pool and bridge byte streams.
    // Utilizes Linux splice(2) kernel zero-copy where available, falling back to chunked user-space copy.
    let result = upstream
        .dispatch_stream(|stream| async move {
            let upstream_addr = stream.peer_addr().ok();
            let res = velda_sock::forward_connection(conn, stream, buffer_size, idle_timeout).await;
            Ok::<_, std::convert::Infallible>((res, upstream_addr))
        })
        .await;

    // Step 3: Emit exactly one connection-level access log record (L4 has no per-request framing).
    // `status_code`/`method` stay 0/Unknown: raw TCP carries no HTTP semantics.
    let (rx, tx, upstream_ip, upstream_port, completion, forward_err) = match result {
        Ok((Ok(stats), upstream_addr)) => {
            let (up_ip, up_port) = upstream_addr
                .map(|a| {
                    (
                        velda_observability::encode_upstream_ip_u32(a.ip()),
                        a.port(),
                    )
                })
                .unwrap_or((0, 0));
            (
                stats.client_to_server_bytes,
                stats.server_to_client_bytes,
                up_ip,
                up_port,
                velda_observability::LogCompletionReason::Complete,
                None,
            )
        }
        Ok((Err(e), upstream_addr)) => {
            let (up_ip, up_port) = upstream_addr
                .map(|a| {
                    (
                        velda_observability::encode_upstream_ip_u32(a.ip()),
                        a.port(),
                    )
                })
                .unwrap_or((0, 0));
            (
                0,
                0,
                up_ip,
                up_port,
                velda_observability::LogCompletionReason::UpstreamError,
                Some(e.to_string()),
            )
        }
        Err(e) => (
            0,
            0,
            0,
            0,
            velda_observability::LogCompletionReason::UpstreamError,
            Some(e.to_string()),
        ),
    };
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
        pipe: velda_core::pipe::PipeKind::TcpTcp,
        completion: completion as u8,
        method: velda_observability::LogHttpMethod::Unknown as u8,
        grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
        ua_family: velda_observability::UaFamily::None as u8,
        ua_os: velda_observability::UaOs::Unknown as u8,
        url_len: 0,
        url_bytes: [0; 188],
    });

    match forward_err {
        None => {
            // Step 4: Record transferred byte telemetry upon graceful stream closure.
            tracing::debug!(
                upstream = %upstream_id,
                peer = %peer,
                client_to_server = rx,
                server_to_client = tx,
                "L4 TCP stream forwarding completed"
            );
        }
        Some(e) => {
            // Step 5: Log abnormal connection termination (e.g. client reset, network drop, timeout).
            tracing::warn!(
                error = %e,
                upstream = %upstream_id,
                peer = %peer,
                "L4 TCP stream forwarding terminated with error"
            );
        }
    }
}
