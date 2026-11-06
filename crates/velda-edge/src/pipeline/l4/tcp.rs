//! Layer 4 (L4) raw TCP connection forwarding.
//!
//! Direct byte-level proxying between downstream client and upstream backend
//! via `velda_transport::forward_connection`. Pure raw TCP with zero TLS termination.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use velda_transport::Connection;

use crate::runtime::Runtime;
use crate::runtime::pipeline::L4TcpConfig;
use crate::upstream::RawUpstream;

static ACTIVE_L4_TCP_CONNS: AtomicUsize = AtomicUsize::new(0);

/// Returns the currently active L4 TCP connection count.
#[inline]
pub fn active_l4_tcp_conns() -> usize {
    ACTIVE_L4_TCP_CONNS.load(Ordering::Acquire)
}

/// Zero-cost RAII guard tracking active L4 TCP stream concurrency.
struct L4TcpConnGuard;

impl Drop for L4TcpConnGuard {
    fn drop(&mut self) {
        ACTIVE_L4_TCP_CONNS.fetch_sub(1, Ordering::Release);
    }
}

/// Handles a raw L4 TCP connection: evaluates route, acquires backend stream, and pumps bytes.
///
/// `config` is pre-compiled at bootstrap in [`crate::runtime::pipeline::PipelineTable`], eliminating
/// runtime listener searches on the connection accept hot path.
pub async fn handle_l4_tcp(
    conn: Connection,
    listener_id: Arc<str>,
    config: L4TcpConfig,
    rt: &Runtime,
) {
    let peer = conn.peer();

    // 1. Ingress Admission: Reject if connection capacity reached or memory is saturating
    let current_conns = ACTIVE_L4_TCP_CONNS.load(Ordering::Acquire);
    if current_conns >= config.max_connections || rt.overload.level().is_shedding() {
        tracing::trace!(
            listener = %listener_id,
            peer = %peer,
            current = current_conns,
            max = config.max_connections,
            "Shedding L4 TCP connection: capacity limit or memory saturation"
        );
        return;
    }

    ACTIVE_L4_TCP_CONNS.fetch_add(1, Ordering::Release);
    let _conn_guard = L4TcpConnGuard;

    let Some(route) = rt.router.route_tcp(&listener_id) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "No L4 TCP route configured for listener; dropping connection"
        );
        return;
    };

    let Some(raw_upstream) = rt.upstreams.raw.get(&route.upstream_name) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %peer,
            "No backend upstream available in Raw upstream table; dropping connection"
        );
        return;
    };

    match raw_upstream.as_ref() {
        // [Standard Pipe: Linux Kernel Network Stack via splice / TcpAccelerationPath]
        // Activation Condition: route upstream configured with `transport = "tcp"`.
        RawUpstream::Tcp(upstream) => {
            tracing::debug!(
                listener = %listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                peer = %peer,
                "Proxying L4 TCP stream to upstream backend via forward_connection (Zero TLS)"
            );

            let idle_timeout = config.idle_timeout.or(Some(upstream.timeouts().idle));
            let buffer_size = config.copy_buffer_size;

            match upstream
                .dispatch_stream(|stream| {
                    velda_transport::forward_connection(conn, stream, buffer_size, idle_timeout)
                })
                .await
            {
                Ok(stats) => {
                    tracing::debug!(
                        upstream = %route.upstream_name,
                        peer = %peer,
                        client_to_server = stats.client_to_server_bytes,
                        server_to_client = stats.server_to_client_bytes,
                        "L4 TCP stream forwarding completed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        upstream = %route.upstream_name,
                        peer = %peer,
                        "L4 TCP stream forwarding terminated with error"
                    );
                }
            }
        }
        // [Acceleration Pipe: Hardware Kernel-Bypass via velda-rdma Queue Pairs]
        // Activation Conditions:
        // 1. Upstream configuration declared with `transport = "rdma"`.
        // 2. Host possesses an active RDMA NIC (`libibverbs.so.1`) or runs in verified software mock mode.
        // 3. Pre-warmed RdmaStream leased from core lane in < 10ns without hot-path lock contention.
        RawUpstream::Rdma(upstream) => {
            tracing::debug!(
                listener = %listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                peer = %peer,
                "Proxying L4 TCP stream to RDMA kernel-bypass upstream"
            );

            let mut rdma_stream = match upstream.acquire_stream().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        upstream = %route.upstream_name,
                        peer = %peer,
                        "Failed to acquire RDMA stream for L4 raw forwarding"
                    );
                    return;
                }
            };

            let mut client_stream = conn.stream;
            match tokio::io::copy_bidirectional(&mut client_stream, &mut rdma_stream).await {
                Ok((c2s, s2c)) => {
                    tracing::debug!(
                        upstream = %route.upstream_name,
                        peer = %peer,
                        client_to_server = c2s,
                        server_to_client = s2c,
                        "L4 TCP to RDMA stream forwarding completed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        upstream = %route.upstream_name,
                        peer = %peer,
                        "L4 TCP to RDMA stream forwarding terminated with error"
                    );
                }
            }
        }
        RawUpstream::Udp(_) => {
            tracing::error!(
                listener = %listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                "Cannot forward TCP connection to UDP raw upstream"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l4_tcp_conn_guard_lifecycle() {
        let before = active_l4_tcp_conns();
        ACTIVE_L4_TCP_CONNS.fetch_add(1, Ordering::Release);
        let guard = L4TcpConnGuard;
        assert_eq!(active_l4_tcp_conns(), before + 1);
        drop(guard);
        assert_eq!(active_l4_tcp_conns(), before);
    }
}
