//! Flat protocol pipelines: each module represents an isolated, self-contained protocol pipeline.

pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod tcp;
pub mod udp;

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
            handle_grpc_stream(
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
/// L7 (HTTP/3, gRPC over QUIC) when the listener has one, otherwise raw L4 forwarding.
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
            handle_grpc_udp_handoff(datagram, socket, listener_id, config, runtime).await;
        }
    }
}
