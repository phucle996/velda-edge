//! Flat protocol pipelines: each module represents an isolated, self-contained protocol pipeline.

pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod tcp;
pub mod udp;

pub use grpc::{
    clear_grpc_udp_engines, handle_grpc_tcp, handle_grpc_udp, has_grpc_udp_engine,
    init_grpc_udp_engine,
};
pub use http1::handle_http1_stream;
pub use http2::handle_http2_stream;
pub use http3::{clear_h3_engines, handle_http3_handoff, has_h3_engine, init_h3_engine};
pub use tcp::handle_l4_tcp;
pub use udp::{UdpSessionKey, UdpSessionTable, get_udp_session_table, handle_l4_udp};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use velda_transport::{Connection, Datagram, UdpSocket};

use crate::runtime::SharedRuntime;
use crate::runtime::pipeline::{TcpPipeline, UdpPipeline};

/// Boxed thread-safe future alias for pre-bound pipeline runners.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Dedicated, specialized pipeline runner pre-bound to a specific TCP listener.
pub type DedicatedTcpRunner = Arc<dyn Fn(Connection) -> BoxFuture<'static, ()> + Send + Sync>;

/// Dedicated, specialized pipeline runner pre-bound to a specific UDP listener.
pub type DedicatedUdpRunner =
    Arc<dyn Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> BoxFuture<'static, ()> + Send + Sync>;

/// Dispatches an accepted TCP connection to its compiled pipeline:
/// L7 (HTTP/1, HTTP/2, gRPC over TCP) when the listener has one, otherwise raw L4 forwarding.
pub async fn handle_tcp(connection: Connection, runtime: &SharedRuntime) {
    let Some(listener_id) = connection.listener_id.clone() else {
        tracing::warn!(peer = %connection.peer(), "Accepted TCP connection without listener_id; dropping");
        return;
    };

    // Fast-path: use zero-atomic RCU read guard to check if an L7 pipeline is compiled.
    let pipeline = {
        let rt = runtime.load();
        rt.pipelines.tcp_pipeline(&listener_id).cloned()
    };

    // Pipeline configs are `Copy`, so this is a plain memcpy, not a heap clone.
    let Some(pipeline) = pipeline else {
        // Raw L4 forwarding keeps an owned snapshot for the connection's lifetime.
        let rt = runtime.load_full();
        handle_l4_tcp(connection, listener_id, &rt).await;
        return;
    };

    match pipeline {
        TcpPipeline::Http1 {
            config,
            tls_enabled,
            streaming,
        } => {
            handle_http1_stream(
                connection,
                listener_id,
                config,
                tls_enabled,
                streaming,
                runtime.clone(),
            )
            .await;
        }
        TcpPipeline::Http2 {
            config,
            tls_enabled,
            streaming: _,
        } => {
            handle_http2_stream(
                connection,
                listener_id,
                config,
                tls_enabled,
                runtime.clone(),
            )
            .await;
        }
        TcpPipeline::Grpc {
            config,
            tls_enabled,
            streaming: _,
        } => {
            handle_grpc_tcp(
                connection,
                listener_id,
                config,
                tls_enabled,
                runtime.clone(),
            )
            .await;
        }
    }
}

/// Dispatches a received UDP datagram to its compiled pipeline:
/// L7 (HTTP/3, gRPC over UDP) when the listener has one, otherwise raw L4 forwarding.
pub async fn handle_udp(
    listener_id: Arc<str>,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    runtime: &SharedRuntime,
) {
    // Fast-path: use zero-atomic RCU read guard to check if an L7 pipeline is compiled.
    let pipeline = {
        let rt = runtime.load();
        rt.pipelines.udp_pipeline(&listener_id).cloned()
    };

    let Some(pipeline) = pipeline else {
        let rt = runtime.load();
        handle_l4_udp(listener_id, socket, datagram, &rt).await;
        return;
    };

    match pipeline {
        UdpPipeline::Http3 { config, .. } => {
            handle_http3_handoff(datagram, socket, listener_id, config, runtime).await;
        }
        UdpPipeline::Grpc { config, .. } => {
            handle_grpc_udp(datagram, socket, listener_id, config, runtime).await;
        }
    }
}

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
                    handle_http1_stream(connection, lid, config, tls_enabled, streaming, rt).await;
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
                    handle_http2_stream(connection, lid, config, tls_enabled, rt).await;
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
                    handle_grpc_tcp(connection, lid, config, tls_enabled, rt).await;
                })
            })
        }
        None => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.load_full();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    handle_l4_tcp(connection, lid, &rt).await;
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
                        handle_http3_handoff(datagram, socket, lid, config, &rt).await;
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
                        handle_grpc_udp(datagram, socket, lid, config, &rt).await;
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
                        handle_l4_udp(lid, socket, datagram, &rt_guard).await;
                    })
                },
            )
        }
    }
}
