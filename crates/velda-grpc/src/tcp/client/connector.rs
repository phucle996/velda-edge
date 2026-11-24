//! Upstream gRPC client connector powered by native `velda-http2` ([`Http2Client`]).
//!
//! Owns connection establishment, HTTP/2 client framing, and both Unary
//! and Streaming request dispatch to physical backend endpoints.

use std::time::Duration;

use http::{Method, Uri};
use velda_core::{HeaderWorkspace, L7Request, L7Response};
use velda_http2::client::connection::{Http2Client, Http2ClientSendStream, Http2ResponseFuture};
use velda_http2::error::Http2Error;
use velda_tls::TlsClientEngine;

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Active gRPC client connector to an upstream backend endpoint.
#[derive(Clone)]
pub struct GrpcUpstreamConnector {
    client: Http2Client,
    target: Option<(u128, u16)>,
}

impl GrpcUpstreamConnector {
    /// Returns the physical backend `(ip: u128, port_be: u16)` if established to a socket target.
    #[inline(always)]
    pub fn target(&self) -> Option<(u128, u16)> {
        self.target
    }

    /// Returns true if the underlying HTTP/2 connection is open and not draining (`GOAWAY`).
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.client.is_healthy()
    }

    /// Attaches the physical backend `(ip: u128, port_be: u16)` to a stream-handshaked connector.
    #[inline(always)]
    pub fn with_target(mut self, target_ip: u128, target_port_be: u16) -> Self {
        self.target = Some((target_ip, target_port_be));
        self
    }

    /// Verifies that the underlying HTTP/2 connection is healthy and ready to accept streams.
    #[inline]
    pub async fn ready(&mut self) -> Result<(), GrpcError> {
        if self.client.is_healthy() {
            Ok(())
        } else {
            Err(GrpcError::Http2(Http2Error::ConnectionClosed))
        }
    }

    /// Establishes a new HTTP/2 connection to the upstream target backend endpoint.
    ///
    /// `tls` is `Some((engine, sni))` for TLS upstreams; gRPC strictly enforces ALPN `"h2"`.
    pub async fn connect(
        target_ip: u128,
        target_port_be: u16,
        tls: Option<(&TlsClientEngine, &str)>,
        config: &GrpcConfig,
        timeout: Option<Duration>,
    ) -> Result<Self, GrpcError> {
        let connect_timeout = timeout.unwrap_or(Duration::from_secs(10));
        let stream =
            velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, connect_timeout)
                .await
                .map_err(GrpcError::Io)?;
        let h2_cfg = config.to_http2_config();

        let client = if let Some((engine, sni)) = tls {
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

            let alpn = tls_stream.get_ref().1.alpn_protocol();
            if alpn != Some(b"h2") {
                return Err(GrpcError::Tls(format!(
                    "upstream negotiated ALPN {:?}, strictly expected \"h2\"",
                    alpn.map(String::from_utf8_lossy)
                )));
            }

            let h2_fut = Http2Client::handshake(tls_stream, &h2_cfg);
            if let Some(to) = timeout {
                tokio::time::timeout(to, h2_fut).await.map_err(|_| {
                    GrpcError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Upstream gRPC H2 handshake timed out",
                    ))
                })??
            } else {
                h2_fut.await?
            }
        } else {
            let h2_fut = Http2Client::handshake(stream, &h2_cfg);
            if let Some(to) = timeout {
                tokio::time::timeout(to, h2_fut).await.map_err(|_| {
                    GrpcError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Upstream gRPC cleartext H2 handshake timed out",
                    ))
                })??
            } else {
                h2_fut.await?
            }
        };

        Ok(Self {
            client,
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
        let h2_cfg = config.to_http2_config();
        let h2_fut = Http2Client::handshake(stream, &h2_cfg);
        let client = if let Some(to) = timeout {
            tokio::time::timeout(to, h2_fut).await.map_err(|_| {
                GrpcError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Upstream gRPC stream handshake timed out",
                ))
            })??
        } else {
            h2_fut.await?
        };

        Ok(Self {
            client,
            target: None,
        })
    }

    /// Submits a raw streaming gRPC request directly from [`HeaderWorkspace`] to the upstream backend.
    pub async fn open_stream(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderWorkspace,
        end_of_stream: bool,
    ) -> Result<(Http2ResponseFuture, Http2ClientSendStream), GrpcError> {
        self.client
            .send_request(method, uri, headers, end_of_stream)
            .await
            .map_err(GrpcError::Http2)
    }

    /// Dispatches an upstream [`super::request::GrpcClientRequest`] and returns a [`super::response::GrpcClientResponse`].
    pub async fn invoke_client_request(
        &mut self,
        req: &super::request::GrpcClientRequest,
        config: &GrpcConfig,
    ) -> Result<super::response::GrpcClientResponse, GrpcError> {
        let (method, uri, headers, end_of_stream) = req.encode_headers();
        let (response_future, mut send_stream) = self
            .open_stream(method, uri, headers, end_of_stream)
            .await?;

        if !req.body.is_empty() {
            send_stream.send_chunk(req.body.to_bytes(), true).await?;
        }

        super::response::GrpcClientResponse::decode_from_future(
            response_future,
            config.max_message_size,
        )
        .await
    }

    /// Invokes a Unary gRPC request and returns the upstream [`L7Response`].
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
