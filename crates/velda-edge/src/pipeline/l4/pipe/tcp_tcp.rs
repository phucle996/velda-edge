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

    // Step 2: Dispatch connection from upstream pool and bridge byte streams.
    // Utilizes Linux splice(2) kernel zero-copy where available, falling back to chunked user-space copy.
    match upstream
        .dispatch_stream(|stream| {
            velda_sock::forward_connection(conn, stream, buffer_size, idle_timeout)
        })
        .await
    {
        Ok(stats) => {
            // Step 3: Record transferred byte telemetry upon graceful stream closure.
            tracing::debug!(
                upstream = %upstream_id,
                peer = %peer,
                client_to_server = stats.client_to_server_bytes,
                server_to_client = stats.server_to_client_bytes,
                "L4 TCP stream forwarding completed"
            );
        }
        Err(e) => {
            // Step 4: Log abnormal connection termination (e.g. client reset, network drop, timeout).
            tracing::warn!(
                error = %e,
                upstream = %upstream_id,
                peer = %peer,
                "L4 TCP stream forwarding terminated with error"
            );
        }
    }
}
