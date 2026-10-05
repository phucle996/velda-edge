//! Flat protocol pipelines: each module represents an isolated, self-contained protocol pipeline.

pub mod context;
pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod tcp;
pub mod udp;

pub use context::{IngressContext, TlsMetadata};
pub use grpc::handle_grpc_stream;
pub use http1::handle_http1_stream;
pub use http2::handle_http2_stream;
pub use http3::{
    clear_h3_engines, handle_grpc_udp_handoff, handle_http3_handoff, has_h3_engine, init_h3_engine,
};
pub use tcp::handle_l4_tcp;
pub use udp::{UdpSessionKey, UdpSessionTable, get_udp_session_table, handle_l4_udp};

use std::sync::Arc;

use velda_transport::{Connection, Datagram, UdpSocket};

use crate::runtime::SharedRuntime;
use crate::runtime::pipeline::{TcpPipeline, UdpPipeline};

/// Dispatches an accepted TCP connection to its compiled pipeline:
/// L7 (HTTP/1, HTTP/2, gRPC) when the listener has one, otherwise raw L4 forwarding.
pub async fn handle_tcp(connection: Connection, runtime: &SharedRuntime) {
    // One snapshot for the whole dispatch decision (pipeline lookup and L4 routing agree).
    let rt = runtime.load_full();
    let Some(listener_id) = connection.listener_id.clone() else {
        tracing::warn!(peer = %connection.peer(), "Accepted TCP connection without listener_id; dropping");
        return;
    };

    // Pipeline configs are `Copy`, so this is a plain memcpy, not a heap clone.
    let Some(pipeline) = rt.pipelines.tcp_pipeline(&listener_id).cloned() else {
        // Raw L4 forwarding keeps the snapshot for the connection's lifetime.
        handle_l4_tcp(connection, listener_id, &rt).await;
        return;
    };
    // L7 handlers load their own snapshots; do not pin this one for the connection lifetime.
    drop(rt);

    let context = IngressContext::new_tcp(
        connection.id(),
        listener_id,
        connection.peer(),
        connection.local_addr(),
        pipeline.tls_enabled(),
        pipeline.streaming(),
    );

    match pipeline {
        TcpPipeline::Http1 { config, .. } => {
            handle_http1_stream(connection, context, config, runtime.clone()).await;
        }
        TcpPipeline::Http2 { config, .. } => {
            handle_http2_stream(connection, context, config, runtime.clone()).await;
        }
        TcpPipeline::Grpc { config, .. } => {
            handle_grpc_stream(connection, context, config, runtime.clone()).await;
        }
    }
}

/// Dispatches a received UDP datagram to its compiled pipeline:
/// L7 (HTTP/3, gRPC over QUIC) when the listener has one, otherwise raw L4 forwarding.
pub async fn handle_udp(
    listener_id: Arc<str>,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    runtime: &SharedRuntime,
) {
    let rt = runtime.load_full();

    let Some(pipeline) = rt.pipelines.udp_pipeline(&listener_id).cloned() else {
        handle_l4_udp(listener_id, socket, datagram, &rt).await;
        return;
    };
    drop(rt);

    let context = IngressContext::new_udp(
        listener_id,
        datagram.peer(),
        datagram.local_addr(),
        pipeline.tls_enabled(),
        pipeline.streaming(),
    );

    match pipeline {
        UdpPipeline::Http3 { config, .. } => {
            handle_http3_handoff(datagram, socket, context, config, runtime).await;
        }
        UdpPipeline::Grpc { config, .. } => {
            handle_grpc_udp_handoff(datagram, socket, context, config, runtime).await;
        }
    }
}
