//! Layer 7 (L7) traffic dispatching via pre-compiled pipeline table.
//!
//! Dispatches TCP and UDP handoffs to protocol-specific stream workers
//! using a flat `match` on [`TcpPipeline`] / [`UdpPipeline`] enum variants
//! pre-compiled at bootstrap. Zero if-else branching on the hot path.

use velda_composer::{ComposedStream, TlsMetadata};
use velda_tls::TlsServerEngine;
use velda_transport::{TcpL7Handoff, UdpL7Handoff};

use crate::pipeline::l7::grpc::handle_grpc_stream;
use crate::pipeline::l7::http1::handle_http1_stream;
use crate::pipeline::l7::http2::handle_http2_stream;
use crate::pipeline::l7::http3::{handle_grpc_udp_handoff, handle_http3_handoff};
use crate::runtime::SharedRuntime;
use crate::runtime::pipeline::{TcpPipeline, UdpPipeline};

/// Dispatches an accepted TCP L7 handoff through the pre-compiled pipeline table.
///
/// Protocol resolution is O(1) via `PipelineTable::tcp_pipeline()` lookup.
/// TLS termination is performed inline for TLS-required pipelines. ALPN is
/// validated but never mutates the declared protocol.
pub async fn dispatch_tcp_l7(handoff: TcpL7Handoff, runtime: &SharedRuntime) {
    let rt = runtime.load();
    let listener_id = handoff.listener_id().to_owned();

    let Some(pipeline) = rt.pipelines.tcp_pipeline(&listener_id) else {
        tracing::error!(
            listener = %listener_id,
            "No compiled TCP pipeline for listener; dropping connection"
        );
        return;
    };

    let composed = match rt.composer.compose_tcp_handoff(handoff) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "TCP L7 composition failed");
            return;
        }
    };

    match composed {
        ComposedStream::Cleartext {
            connection,
            context,
        } => {
            tracing::debug!(
                listener = %context.listener_id,
                protocol = %context.protocol,
                peer = %context.peer,
                pipeline = ?pipeline,
                "Dispatching cleartext TCP stream via compiled pipeline"
            );
            match pipeline {
                TcpPipeline::CleartextHttp1 => {
                    tokio::spawn(handle_http1_stream(
                        connection,
                        context,
                        std::sync::Arc::clone(runtime),
                    ));
                }
                TcpPipeline::CleartextHttp2 => {
                    tokio::spawn(handle_http2_stream(
                        connection,
                        context,
                        std::sync::Arc::clone(runtime),
                    ));
                }
                TcpPipeline::CleartextGrpc => {
                    tokio::spawn(handle_grpc_stream(
                        connection,
                        context,
                        std::sync::Arc::clone(runtime),
                    ));
                }
                other => {
                    tracing::error!(
                        listener = %context.listener_id,
                        pipeline = ?other,
                        "TLS pipeline variant on cleartext stream; dropping connection"
                    );
                }
            }
        }
        ComposedStream::TlsRequired {
            connection,
            context,
        } => {
            tracing::debug!(
                listener = %context.listener_id,
                protocol = %context.protocol,
                peer = %context.peer,
                pipeline = ?pipeline,
                "Initiating downstream TLS termination for compiled pipeline"
            );

            let Some(tls_server) = rt.tls_server.as_ref() else {
                tracing::error!(
                    listener = %context.listener_id,
                    peer = %context.peer,
                    "TLS required for listener, but no TLS server engine is compiled; dropping connection"
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
                    // ALPN validates client compatibility but does NOT change the protocol
                    let enriched_context = context.with_tls_metadata(TlsMetadata::new(
                        handshake_info.sni,
                        handshake_info.alpn,
                    ));

                    match pipeline {
                        TcpPipeline::TlsHttp1 => {
                            tokio::spawn(handle_http1_stream(
                                tls_stream,
                                enriched_context,
                                std::sync::Arc::clone(runtime),
                            ));
                        }
                        TcpPipeline::TlsHttp2 => {
                            tokio::spawn(handle_http2_stream(
                                tls_stream,
                                enriched_context,
                                std::sync::Arc::clone(runtime),
                            ));
                        }
                        TcpPipeline::TlsGrpc => {
                            tokio::spawn(handle_grpc_stream(
                                tls_stream,
                                enriched_context,
                                std::sync::Arc::clone(runtime),
                            ));
                        }
                        other => {
                            tracing::error!(
                                listener = %enriched_context.listener_id,
                                pipeline = ?other,
                                "Cleartext pipeline variant on TLS stream; dropping connection"
                            );
                        }
                    }
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
    }
}

/// Dispatches an accepted UDP L7 handoff through the pre-compiled pipeline table.
pub async fn dispatch_udp_l7(handoff: UdpL7Handoff, runtime: &SharedRuntime) {
    let rt = runtime.load();
    let composed = match rt.composer.compose_udp_handoff(handoff) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "UDP L7 composition failed");
            return;
        }
    };

    let listener_id = composed.context().listener_id.clone();
    let Some(udp_pipeline) = rt.pipelines.udp_pipeline(&listener_id) else {
        tracing::error!(
            listener = %listener_id,
            peer = %composed.context().peer,
            "No compiled UDP pipeline for listener; dropping datagram"
        );
        return;
    };

    match udp_pipeline {
        UdpPipeline::Http3 => {
            handle_http3_handoff(composed, runtime).await;
        }
        UdpPipeline::Grpc => {
            handle_grpc_udp_handoff(composed, runtime).await;
        }
    }
}
