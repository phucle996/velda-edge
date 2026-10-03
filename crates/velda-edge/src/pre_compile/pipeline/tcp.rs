//! Layer 4 (L4) raw TCP connection forwarding.
//!
//! Direct byte-level proxying between downstream client and upstream backend
//! via `velda_transport::forward_connection`. Pure raw TCP with zero TLS termination.

use velda_transport::Connection;

use crate::runtime::SharedRuntime;

/// Handles a raw L4 TCP connection: evaluates route, acquires backend stream, and pumps bytes.
pub async fn handle_l4_tcp(conn: Connection, runtime: &SharedRuntime) {
    let peer = conn.peer();
    let Some(listener_id) = conn.listener_id().map(|s| s.to_string()) else {
        tracing::warn!(peer = %peer, "Received L4 connection without listener_id; dropping");
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_tcp(&listener_id) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "No L4 TCP route configured for listener; dropping connection"
        );
        return;
    };

    let Some(upstream) = rt.upstreams.tcp.get(&route.upstream_name) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            peer = %peer,
            "No backend upstream available in TCP upstream table; dropping connection"
        );
        return;
    };

    tracing::debug!(
        listener = %listener_id,
        route = %route.id,
        upstream = %route.upstream_name,
        peer = %peer,
        "Proxying L4 TCP stream to upstream backend via forward_connection (Zero TLS)"
    );

    match upstream
        .dispatch_stream(|stream| velda_transport::forward_connection(conn, stream))
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
