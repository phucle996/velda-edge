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

use velda_transport::{TcpL7Handoff, UdpL7Handoff};

use crate::runtime::SharedRuntime;
use crate::runtime::pipeline::{TcpPipeline, UdpPipeline};

/// Routes an accepted TCP L7 handoff to its protocol-specific stream worker.
pub async fn handle_tcp_l7(handoff: TcpL7Handoff, runtime: &SharedRuntime) {
    let rt = runtime.load();
    let listener_id = handoff.listener_id();

    let Some(pipeline) = rt.pipelines.tcp_pipeline(listener_id).cloned() else {
        tracing::error!(
            listener = %listener_id,
            "No compiled TCP pipeline for listener; dropping connection"
        );
        return;
    };

    let conn_id = handoff.id();
    let peer = handoff.peer();
    let local_addr = handoff.local_addr();
    let (connection, owned_listener_id) = handoff.into_parts();

    let context = IngressContext::new_tcp(
        conn_id,
        owned_listener_id,
        peer,
        local_addr,
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

/// Routes an accepted UDP L7 handoff to its protocol-specific handler.
pub async fn handle_udp_l7(handoff: UdpL7Handoff, runtime: &SharedRuntime) {
    let rt = runtime.load();
    let listener_id = handoff.listener_id();

    let Some(pipeline) = rt.pipelines.udp_pipeline(listener_id).cloned() else {
        tracing::error!(
            listener = %listener_id,
            peer = %handoff.peer(),
            "No compiled UDP pipeline for listener; dropping datagram"
        );
        return;
    };

    let peer = handoff.peer();
    let local_addr = handoff.local_addr();
    let (datagram, socket, owned_listener_id) = handoff.into_parts();
    let context = IngressContext::new_udp(
        owned_listener_id,
        peer,
        local_addr,
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
