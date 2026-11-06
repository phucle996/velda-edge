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
        RawUpstream::Tcp(upstream) => {
            super::pipe::tcp_tcp::serve(
                conn,
                upstream,
                &config,
                route.id,
                &route.upstream_name,
                peer,
                &listener_id,
            )
            .await;
        }
        RawUpstream::Rdma(upstream) => {
            super::pipe::tcp_rdma::serve(
                conn,
                upstream,
                &config,
                route.id,
                &route.upstream_name,
                peer,
                &listener_id,
            )
            .await;
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
