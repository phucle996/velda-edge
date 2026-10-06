//! Upstream gRPC client connector over UDP / QUIC transport.
//!
//! Provides [`GrpcUdpClient`], an asynchronous, persistent QUIC client connection
//! capable of multiplexing concurrent bidirectional gRPC streams over a single
//! QUIC connection without Head-of-Line blocking.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::{ClientConfig, Endpoint, EndpointConfig, Event};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use velda_core::{L7Request, L7Response};

use super::driver::{ClientCommand, run_client_driver};
use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// An active, persistent multiplexed gRPC upstream client connection over UDP / QUIC.
#[derive(Clone)]
pub struct GrpcUdpClient {
    command_tx: mpsc::Sender<ClientCommand>,
    target: SocketAddr,
}

impl GrpcUdpClient {
    /// Dispatches an owned [`L7Request`] over a new multiplexed bidirectional stream on this connection.
    pub async fn send_request(&self, req: L7Request) -> Result<L7Response, GrpcError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::SendRequest {
                request: req,
                reply_tx,
            })
            .await
            .map_err(|_| GrpcError::Protocol("connection closed".into()))?;

        reply_rx
            .await
            .map_err(|_| GrpcError::Protocol("connection closed".into()))?
    }

    /// Dispatches a borrowed [`L7Request`] by cloning it.
    #[inline]
    pub async fn send_request_ref(&self, req: &L7Request) -> Result<L7Response, GrpcError> {
        self.send_request(req.clone()).await
    }

    /// Returns the target physical socket address of this client connection.
    #[inline]
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    /// Returns `true` if the background driver loop has terminated or connection closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.command_tx.is_closed()
    }

    /// Gracefully closes the underlying QUIC connection.
    pub async fn close(&self) {
        let _ = self.command_tx.send(ClientCommand::Close).await;
    }
}

/// Builds default QUIC client configuration for gRPC over UDP upstream.
pub fn default_client_config() -> Result<ClientConfig, GrpcError> {
    let rustls_client = velda_tls::client::build_insecure_tls13_client_config(vec![
        b"grpc-exp".to_vec(),
        b"h3".to_vec(),
    ]);
    velda_tls::client::build_quic_client_config(rustls_client)
        .map_err(|e| GrpcError::Internal(format!("QUIC crypto config error: {e}")))
}

/// Establishes a new persistent, multiplexed gRPC client connection over QUIC / UDP to the target backend endpoint.
///
/// Drives the initial QUIC handshake to completion and spawns the background connection driver.
pub async fn connect(
    target: SocketAddr,
    server_name: &str,
    config: &GrpcConfig,
) -> Result<GrpcUdpClient, GrpcError> {
    // 1. Bind local UDP egress socket
    let bind_addr: SocketAddr = if target.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let socket = Arc::new(UdpSocket::bind(bind_addr).await.map_err(GrpcError::Io)?);

    // 2. Build QUIC client endpoint and config
    let endpoint_config = Arc::new(EndpointConfig::default());
    let mut endpoint = Endpoint::new(endpoint_config, None, false, None);

    let client_cfg = default_client_config()?;
    let now = Instant::now();
    let (handle, mut conn) = endpoint
        .connect(now, client_cfg, target, server_name)
        .map_err(|e| {
            GrpcError::Internal(format!("Failed to initiate QUIC connect to {target}: {e}"))
        })?;

    let mut transmit_buf = Vec::with_capacity(65535);
    let mut socket_recv_buf = vec![0u8; 65535];

    // 3. Drive QUIC handshake loop to completion
    let handshake_start = Instant::now();
    let handshake_timeout = Duration::from_millis(config.max_call_duration_ms.max(5000));

    while conn.is_handshaking() {
        if handshake_start.elapsed() > handshake_timeout {
            return Err(GrpcError::Timeout);
        }

        // Drain pending transmits
        transmit_buf.clear();
        while let Some(transmit) = conn.poll_transmit(Instant::now(), 1, &mut transmit_buf) {
            if transmit.size > 0 {
                let _ = socket
                    .send_to(&transmit_buf[..transmit.size], transmit.destination)
                    .await;
            }
            transmit_buf.clear();
        }

        tokio::select! {
            res = socket.recv_from(&mut socket_recv_buf) => {
                let (len, from_addr) = res.map_err(GrpcError::Io)?;
                let now = Instant::now();
                let payload = BytesMut::from(&socket_recv_buf[..len]);
                let mut resp_buf = Vec::new();
                if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ce)) =
                    endpoint.handle(now, from_addr, None, None, payload, &mut resp_buf)
                {
                    conn.handle_event(ce);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {
                let now = Instant::now();
                if let Some(timeout) = conn.poll_timeout()
                    && timeout <= now
                {
                    conn.handle_timeout(now);
                }
            }
        }

        // Poll connection events during handshake to catch failures early
        while let Some(event) = conn.poll() {
            if let Event::ConnectionLost { reason } = event {
                return Err(GrpcError::Protocol(format!(
                    "gRPC QUIC connection to {target} failed during handshake: {reason}"
                )));
            }
        }
    }

    if conn.is_drained() {
        return Err(GrpcError::Protocol("QUIC connection drained".into()));
    }

    // 4. Drain final handshake transmits
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(Instant::now(), 1, &mut transmit_buf) {
        if transmit.size > 0 {
            let _ = socket
                .send_to(&transmit_buf[..transmit.size], transmit.destination)
                .await;
        }
        transmit_buf.clear();
    }

    // 5. Spawn background connection driver task
    let (command_tx, command_rx) = mpsc::channel(256);
    tokio::spawn(run_client_driver(
        socket,
        endpoint,
        conn,
        handle,
        command_rx,
        config.max_message_size,
    ));

    Ok(GrpcUdpClient { command_tx, target })
}
