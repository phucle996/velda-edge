//! Traffic dispatcher bridging `velda-transport` handoffs to protocol composition and handlers.

use velda_composer::{ComposedStream, TlsMetadata};
use velda_http::HttpVersion;
use velda_tls::TlsServerEngine;
use velda_transport::{Connection, TcpL7Handoff};

use crate::pipeline::http::handle_http_stream;
use crate::runtime::SharedRuntime;

/// Dispatches raw L4 connection to routing or direct upstream proxying.
pub async fn dispatch_l4(_conn: Connection, _runtime: &SharedRuntime) {
    // In full vertical slice, dispatches to L4 router / upstream
}

/// Dispatches an accepted TCP L7 handoff through protocol composition, downstream TLS termination,
/// and L7 HTTP stream decoding.
pub async fn dispatch_tcp_l7(handoff: TcpL7Handoff, runtime: &SharedRuntime) {
    let rt = runtime.load();
    match rt.composer.compose_tcp_handoff(handoff) {
        Ok(ComposedStream::Cleartext {
            connection,
            context,
        }) => {
            tracing::debug!(
                listener = %context.listener_id,
                protocol = %context.protocol,
                peer = %context.peer,
                "Composed TCP cleartext L7 stream"
            );
            let version = HttpVersion::from_proto_str(context.protocol.as_str())
                .unwrap_or(HttpVersion::Http1);
            tokio::spawn(handle_http_stream(connection, version, context));
        }
        Ok(ComposedStream::TlsRequired {
            connection,
            context,
        }) => {
            tracing::debug!(
                listener = %context.listener_id,
                protocol = %context.protocol,
                peer = %context.peer,
                "Composed TCP TLS-required L7 stream; initiating downstream TLS termination"
            );

            let Some(tls_server) = rt.tls_server.as_ref() else {
                tracing::error!(
                    listener = %context.listener_id,
                    peer = %context.peer,
                    "TLS required for listener, but no TLS server engine is compiled in runtime snapshot; dropping connection"
                );
                return;
            };

            match tls_server.accept(connection).await {
                Ok(tls_stream) => {
                    let handshake_info = TlsServerEngine::extract_handshake_info(&tls_stream);
                    tracing::debug!(
                        listener = %context.listener_id,
                        peer = %context.peer,
                        sni = ?handshake_info.sni,
                        alpn = ?handshake_info.alpn,
                        "Downstream TLS handshake succeeded"
                    );
                    let enriched_context = context.with_tls_metadata(TlsMetadata::new(
                        handshake_info.sni,
                        handshake_info.alpn,
                    ));
                    tracing::debug!(
                        listener = %enriched_context.listener_id,
                        protocol = %enriched_context.protocol,
                        peer = %enriched_context.peer,
                        "Resolved concrete application protocol after TLS"
                    );
                    let version = HttpVersion::from_proto_str(enriched_context.protocol.as_str())
                        .unwrap_or(HttpVersion::Http1);
                    tokio::spawn(handle_http_stream(tls_stream, version, enriched_context));
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        listener = %context.listener_id,
                        peer = %context.peer,
                        "Downstream TLS handshake failed"
                    );
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "TCP L7 composition failed");
        }
    }
}

/// Dispatches raw UDP L4 datagrams to direct proxying or routing.
pub async fn dispatch_udp_l4(
    _listener_id: String,
    _socket: std::sync::Arc<velda_transport::UdpSocket>,
    _datagram: velda_transport::Datagram,
    _runtime: &SharedRuntime,
) {
    // In full vertical slice, dispatches to L4 UDP router
}

/// Dispatches an accepted UDP L7 handoff through protocol composition to the HTTP/3 state machine.
pub async fn dispatch_udp_l7(handoff: velda_transport::UdpL7Handoff, runtime: &SharedRuntime) {
    let rt = runtime.load();
    let composed = match rt.composer.compose_udp_handoff(handoff) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "UDP L7 composition failed");
            return;
        }
    };

    let Some(ref h3_engine_lock) = rt.h3_engine else {
        tracing::warn!(
            listener = %composed.context().listener_id,
            peer = %composed.context().peer,
            "Received UDP L7 handoff for HTTP/3, but no HTTP/3 engine is compiled"
        );
        return;
    };

    let (datagram, socket) = composed.into_parts();
    let now = std::time::Instant::now();
    let mut engine = h3_engine_lock.lock().await;
    let (outgoing, requests) = engine.handle_datagram(
        now,
        datagram.peer(),
        Some(datagram.local_addr().ip()),
        datagram.data(),
    );

    // 1. Send all outgoing handshake / ACK datagrams
    for pkt in outgoing {
        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
    }

    // 2. Process complete HTTP/3 requests
    for req_event in requests {
        tracing::debug!(
            method = %req_event.request.method,
            path = %req_event.request.path(),
            peer = %datagram.peer(),
            "Decoded HTTP/3 request"
        );

        let response = velda_core::L7Response::from_bytes(
            http::StatusCode::OK,
            b"{\"gateway\":\"velda-edge\",\"protocol\":\"http3\",\"status\":\"active\"}".to_vec(),
        );

        if let Ok(resp_pkts) =
            engine.send_response(now, req_event.handle, req_event.stream_id, &response)
        {
            for pkt in resp_pkts {
                let _ = socket.send_to(&pkt.payload, pkt.peer).await;
            }
        }
    }
}
