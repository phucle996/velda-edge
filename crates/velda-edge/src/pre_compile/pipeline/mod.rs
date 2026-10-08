//! Flat protocol pipelines: each module represents an isolated, self-contained protocol pipeline.

pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod tcp;
pub mod udp;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use velda_transport::{Connection, Datagram, UdpSocket};

use crate::runtime::SharedRuntime;
use crate::runtime::pipeline::{TcpPipeline, UdpPipeline};

/// Downstream connection metadata capturing network endpoints and listener identity.
#[derive(Debug, Clone)]
pub struct DownstreamMeta {
    pub listener_id: Arc<str>,
    pub peer: SocketAddr,
    pub local_addr: SocketAddr,
    pub is_tls: bool,
}

/// Boxed thread-safe future alias for pre-bound pipeline runners.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Dedicated, specialized pipeline runner pre-bound to a specific TCP listener.
pub type DedicatedTcpRunner = Arc<dyn Fn(Connection) -> BoxFuture<'static, ()> + Send + Sync>;

/// Dedicated, specialized pipeline runner pre-bound to a specific UDP listener.
pub type DedicatedUdpRunner =
    Arc<dyn Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> BoxFuture<'static, ()> + Send + Sync>;

/// Pre-resolves and compiles a dedicated TCP pipeline runner for the specified listener.
///
/// Executed strictly on the cold configuration path (during listener bind / reconcile),
/// completely eliminating runtime HashMap lookups, Arc cloning, and enum pattern matching
/// from the connection accept loop.
pub fn build_tcp_pipeline_runner(listener_id: &str, runtime: SharedRuntime) -> DedicatedTcpRunner {
    let lid: Arc<str> = Arc::from(listener_id);
    let pipeline = {
        let rt = runtime.load();
        rt.pipelines.tcp_pipeline(listener_id).cloned()
    };

    match pipeline {
        Some(TcpPipeline::Http1 {
            config,
            tls_enabled,
            streaming,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    http1::handle_http1_stream(connection, lid, config, tls_enabled, streaming, rt)
                        .await;
                })
            })
        }
        Some(TcpPipeline::Http2 {
            config,
            tls_enabled,
            streaming: _,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    http2::handle_http2_stream(connection, lid, config, tls_enabled, rt).await;
                })
            })
        }
        Some(TcpPipeline::HttpAuto {
            tls_enabled,
            streaming,
            http1_config,
            http2_config,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    handle_http_auto_stream(
                        connection,
                        lid,
                        http1_config,
                        http2_config,
                        tls_enabled,
                        streaming,
                        rt,
                    )
                    .await;
                })
            })
        }
        Some(TcpPipeline::Grpc {
            config,
            tls_enabled,
            streaming: _,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    grpc::handle_grpc_tcp(connection, lid, config, tls_enabled, rt).await;
                })
            })
        }
        None => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.load_full();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    tcp::handle_l4_tcp(connection, lid, &rt).await;
                })
            })
        }
    }
}

/// Pre-resolves and compiles a dedicated UDP pipeline runner for the specified listener.
///
/// Executed strictly on the cold configuration path (during socket bind / reconcile),
/// completely eliminating runtime HashMap lookups, Arc cloning, and enum pattern matching
/// from line-rate UDP datagram ingestion.
pub fn build_udp_pipeline_runner(listener_id: &str, runtime: SharedRuntime) -> DedicatedUdpRunner {
    let lid: Arc<str> = Arc::from(listener_id);
    let pipeline = {
        let rt = runtime.load();
        rt.pipelines.udp_pipeline(listener_id).cloned()
    };

    match pipeline {
        Some(UdpPipeline::Http3 { config, .. }) => {
            let lid = Arc::clone(&lid);
            Arc::new(
                move |_id: Arc<str>, socket: Arc<UdpSocket>, datagram: Datagram| {
                    let rt = runtime.clone();
                    let lid = Arc::clone(&lid);
                    Box::pin(async move {
                        http3::handle_http3_udp(lid, socket, datagram, config, &rt).await;
                    })
                },
            )
        }
        Some(UdpPipeline::Grpc { config, .. }) => {
            let lid = Arc::clone(&lid);
            Arc::new(
                move |_id: Arc<str>, socket: Arc<UdpSocket>, datagram: Datagram| {
                    let rt = runtime.clone();
                    let lid = Arc::clone(&lid);
                    Box::pin(async move {
                        grpc::handle_grpc_udp(lid, socket, datagram, config, &rt).await;
                    })
                },
            )
        }
        None => {
            let lid = Arc::clone(&lid);
            Arc::new(
                move |_id: Arc<str>, socket: Arc<UdpSocket>, datagram: Datagram| {
                    let rt = runtime.clone();
                    let lid = Arc::clone(&lid);
                    Box::pin(async move {
                        let rt_guard = rt.load();
                        udp::handle_l4_udp(lid, socket, datagram, &rt_guard).await;
                    })
                },
            )
        }
    }
}

/// Unified HTTP stream worker for listeners declaring the `http` family.
///
/// Under TLS, negotiates ALPN strictly within the HTTP family ("h2" -> HTTP/2, "http/1.1" -> HTTP/1.1).
/// Under cleartext, falls back directly to the HTTP/1.1 stream loop.
async fn handle_http_auto_stream(
    connection: Connection,
    listener_id: Arc<str>,
    http1_config: velda_http1::Http1Config,
    http2_config: velda_http2::Http2Config,
    tls_enabled: bool,
    streaming: velda_core::StreamingMode,
    runtime: SharedRuntime,
) {
    if !tls_enabled {
        http1::handle_http1_stream(
            connection,
            listener_id,
            http1_config,
            false,
            streaming,
            runtime,
        )
        .await;
        return;
    }

    let rt = runtime.load();
    let peer = connection.peer();
    let local_addr = connection.local_addr();

    let Some(tls_server) = rt.tls_server.as_ref() else {
        tracing::error!(
            listener = %listener_id,
            peer = %peer,
            "TLS required for HTTP family listener, but no TLS server engine is compiled; dropping connection"
        );
        return;
    };

    match tls_server.accept_with_timeout(connection).await {
        Ok(tls_stream) => {
            let handshake_info = velda_tls::TlsServerEngine::extract_handshake_info(&tls_stream);
            tracing::debug!(
                listener = %listener_id,
                peer = %peer,
                sni = ?handshake_info.sni,
                alpn = ?handshake_info.alpn,
                "Downstream TLS handshake succeeded for HTTP family"
            );

            let meta = DownstreamMeta {
                listener_id,
                peer,
                local_addr,
                is_tls: true,
            };

            // ALPN demuxing strictly within the HTTP protocol family
            match handshake_info.alpn.as_deref() {
                Some("h2") => {
                    http2::run_http2_loop(tls_stream, meta, http2_config, runtime).await;
                }
                Some("http/1.1") | None => {
                    http1::run_http1_loop(tls_stream, meta, streaming, http1_config, runtime).await;
                }
                Some(other_alpn) => {
                    tracing::warn!(
                        listener = %meta.listener_id,
                        peer = %peer,
                        alpn = other_alpn,
                        "Dropping connection: ALPN negotiated outside the HTTP protocol family"
                    );
                }
            }
        }
        Err(e) => {
            tracing::debug!(
                listener = %listener_id,
                peer = %peer,
                error = %e,
                "Downstream TLS handshake failed for HTTP family"
            );
        }
    }
}
