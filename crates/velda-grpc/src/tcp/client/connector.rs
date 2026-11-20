//! Upstream gRPC client connector for `velda-upstream` and connection pool.
//!
//! Owns connection establishment, HTTP/2 client framing, and both Unary
//! and Streaming request dispatch to physical backend endpoints.

use bytes::Bytes;
use h2::SendStream;
use h2::client::{ResponseFuture, SendRequest};
use std::time::Duration;
use velda_core::{L7Request, L7Response};
use velda_tls::TlsClientEngine;

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Active gRPC client connector to an upstream backend endpoint.
#[derive(Clone)]
pub struct GrpcUpstreamConnector {
    send_request: SendRequest<Bytes>,
    target: Option<(u128, u16)>,
}

impl GrpcUpstreamConnector {
    /// Returns the physical backend `(ip: u128, port_be: u16)` if established to a socket target.
    #[inline(always)]
    pub fn target(&self) -> Option<(u128, u16)> {
        self.target
    }

    /// Attaches the physical backend `(ip: u128, port_be: u16)` to a stream-handshaked connector.
    #[inline(always)]
    pub fn with_target(mut self, target_ip: u128, target_port_be: u16) -> Self {
        self.target = Some((target_ip, target_port_be));
        self
    }
    /// Polls or awaits readiness of the underlying HTTP/2 connection to accept a new request stream.
    pub async fn ready(&mut self) -> Result<(), GrpcError> {
        let ready_send = self
            .send_request
            .clone()
            .ready()
            .await
            .map_err(GrpcError::H2)?;
        self.send_request = ready_send;
        Ok(())
    }

    /// Establishes a new HTTP/2 connection to the upstream target backend endpoint
    /// applying limits from [`GrpcConfig`], optional socket acceleration path, optional TLS,
    /// and connection timeout.
    ///
    /// `tls` is `Some((engine, sni))` for TLS upstreams; the SNI is the upstream's pre-compiled
    /// static property. gRPC is always HTTP/2 as declared by the upstream: ALPN is only validated
    /// (a negotiated value other than `h2` is rejected), never used to choose the protocol.
    pub async fn connect(
        target_ip: u128,
        target_port_be: u16,
        tls: Option<(&TlsClientEngine, &str)>,
        config: &GrpcConfig,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self, GrpcError> {
        let connect_timeout = timeout.unwrap_or(std::time::Duration::from_secs(10));
        let stream =
            velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, connect_timeout)
                .await
                .map_err(GrpcError::Io)?;

        let mut builder = h2::client::Builder::default();
        builder.max_header_list_size(config.max_header_size as u32);

        let send_request = if let Some((engine, sni)) = tls {
            // Enforce connect timeout on upstream gRPC TLS handshake
            let tls_fut = engine.connect(sni, stream);
            let tls_stream = if let Some(to) = timeout {
                tokio::time::timeout(to, tls_fut)
                    .await
                    .map_err(|_| {
                        GrpcError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Upstream gRPC TLS handshake timed out",
                        ))
                    })?
                    .map_err(|e| GrpcError::Tls(e.to_string()))?
            } else {
                tls_fut.await.map_err(|e| GrpcError::Tls(e.to_string()))?
            };

            // gRPC over TCP/TLS requires HTTP/2 transport framing: ALPN "h2" is strictly enforced
            let alpn = tls_stream.get_ref().1.alpn_protocol();
            if alpn != Some(b"h2") {
                return Err(GrpcError::Tls(format!(
                    "upstream negotiated ALPN {:?}, strictly expected \"h2\"",
                    alpn.map(String::from_utf8_lossy)
                )));
            }

            // Enforce timeout on gRPC HTTP/2 SETTINGS frame exchange over TLS
            let h2_fut = builder.handshake::<_, Bytes>(tls_stream);
            let (send_request, connection) = if let Some(to) = timeout {
                tokio::time::timeout(to, h2_fut)
                    .await
                    .map_err(|_| {
                        GrpcError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Upstream gRPC H2 handshake timed out",
                        ))
                    })?
                    .map_err(GrpcError::H2)?
            } else {
                h2_fut.await.map_err(GrpcError::H2)?
            };

            // Drive background H2 connection management
            tokio::spawn(async move {
                let _ = connection.await;
            });
            send_request
        } else {
            // Enforce timeout on cleartext gRPC HTTP/2 handshake
            let h2_fut = builder.handshake::<_, Bytes>(stream);
            let (send_request, connection) = if let Some(to) = timeout {
                tokio::time::timeout(to, h2_fut)
                    .await
                    .map_err(|_| {
                        GrpcError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Upstream gRPC cleartext H2 handshake timed out",
                        ))
                    })?
                    .map_err(GrpcError::H2)?
            } else {
                h2_fut.await.map_err(GrpcError::H2)?
            };

            // Drive background H2 connection management
            tokio::spawn(async move {
                let _ = connection.await;
            });
            send_request
        };

        Ok(Self {
            send_request,
            target: Some((target_ip, target_port_be)),
        })
    }

    /// Performs the gRPC HTTP/2 client handshake over an arbitrary asynchronous I/O stream (e.g. RDMA stream).
    pub async fn handshake_stream<S>(
        stream: S,
        config: &GrpcConfig,
        timeout: Option<Duration>,
    ) -> Result<Self, GrpcError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let mut builder = h2::client::Builder::default();
        builder.max_header_list_size(config.max_header_size as u32);
        builder.enable_push(false);

        let h2_fut = builder.handshake::<_, Bytes>(stream);
        let (send_request, connection) = if let Some(to) = timeout {
            tokio::time::timeout(to, h2_fut)
                .await
                .map_err(|_| {
                    GrpcError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Upstream gRPC stream handshake timed out",
                    ))
                })?
                .map_err(GrpcError::H2)?
        } else {
            h2_fut.await.map_err(GrpcError::H2)?
        };

        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self {
            send_request,
            target: None,
        })
    }

    /// Submits a raw streaming gRPC request to the upstream backend.
    ///
    /// Returns the upstream `ResponseFuture` and an active `SendStream` for streaming data chunks.
    pub fn open_stream(
        &mut self,
        request: http::Request<()>,
        end_of_stream: bool,
    ) -> Result<(ResponseFuture, SendStream<Bytes>), GrpcError> {
        self.send_request
            .send_request(request, end_of_stream)
            .map_err(GrpcError::H2)
    }

    /// Dispatches an upstream [`super::request::GrpcClientRequest`] and returns a [`super::response::GrpcClientResponse`].
    pub async fn invoke_client_request(
        &mut self,
        req: &super::request::GrpcClientRequest,
        config: &GrpcConfig,
    ) -> Result<super::response::GrpcClientResponse, GrpcError> {
        let (http_req, end_of_stream) = req.encode_http_request()?;
        let (response_future, mut send_stream) = self.open_stream(http_req, end_of_stream)?;

        if !req.body.is_empty() {
            send_stream
                .send_data(req.body.to_bytes(), true)
                .map_err(GrpcError::H2)?;
        }

        super::response::GrpcClientResponse::decode_from_future(
            response_future,
            config.max_message_size,
        )
        .await
    }

    /// Invokes a Unary gRPC request and returns the upstream `L7Response`.
    ///
    /// Seamlessly forwards request payload, waits for response headers, reads response LPM frame,
    /// and collects trailers (`grpc-status`). Enforces `max_message_size` from [`GrpcConfig`]
    /// on the upstream response body.
    pub async fn invoke_unary(
        &mut self,
        req: &L7Request,
        config: &GrpcConfig,
    ) -> Result<L7Response, GrpcError> {
        let client_req = super::request::GrpcClientRequest::from_l7(req);
        let resp = self.invoke_client_request(&client_req, config).await?;
        Ok(resp.into_l7())
    }
}
