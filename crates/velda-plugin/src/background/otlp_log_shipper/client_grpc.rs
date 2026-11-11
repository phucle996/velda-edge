//! Persistent HTTP/2 gRPC OTLP Logs Client (`opentelemetry_logger`).
//!
//! Maintains a persistent HTTP/2 stream multiplexer to an OpenTelemetry Collector gRPC endpoint
//! (`/opentelemetry.proto.collector.logs.v1.LogsService/Export`, standard port 4317).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use h2::client::{SendRequest, handshake};
use http::HeaderMap;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::PluginError;

/// Unified transport stream for plaintext TCP or TLS connections.
pub enum GrpcIoStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for GrpcIoStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for GrpcIoStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Parsed endpoint specification for gRPC communication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpLogsGrpcEndpoint {
    pub is_tls: bool,
    pub host: String,
    pub port: u16,
    pub host_header: String,
}

impl OtlpLogsGrpcEndpoint {
    /// Parses endpoint URL into a [`OtlpLogsGrpcEndpoint`].
    pub fn parse(endpoint_url: &str) -> Result<Self, PluginError> {
        let trimmed = endpoint_url.trim();
        let (is_tls, rest) = if let Some(stripped) = trimmed.strip_prefix("https://") {
            (true, stripped)
        } else if let Some(stripped) = trimmed.strip_prefix("http://") {
            (false, stripped)
        } else if let Some(stripped) = trimmed.strip_prefix("grpcs://") {
            (true, stripped)
        } else if let Some(stripped) = trimmed.strip_prefix("grpc://") {
            (false, stripped)
        } else {
            (false, trimmed)
        };

        let authority = match rest.find('/') {
            Some(idx) => &rest[..idx],
            None => rest,
        };

        if authority.is_empty() {
            return Err(PluginError::Config(format!(
                "Invalid gRPC endpoint URL '{endpoint_url}': missing host authority"
            )));
        }

        let (host, port) = match authority.rfind(':') {
            Some(idx) if !authority.ends_with(']') => {
                let host = authority[..idx].trim_matches(|c| c == '[' || c == ']');
                let port_str = &authority[idx + 1..];
                let port = port_str.parse::<u16>().map_err(|_| {
                    PluginError::Config(format!(
                        "Invalid port '{port_str}' in gRPC endpoint '{endpoint_url}'"
                    ))
                })?;
                (host.to_string(), port)
            }
            _ => {
                let host = authority.trim_matches(|c| c == '[' || c == ']');
                let default_port = 4317;
                (host.to_string(), default_port)
            }
        };

        let host_header = if (is_tls && port == 443) || (!is_tls && port == 80) {
            host.clone()
        } else {
            format!("{host}:{port}")
        };

        Ok(Self {
            is_tls,
            host,
            port,
            host_header,
        })
    }
}

/// Standalone Persistent HTTP/2 gRPC OTLP Logs Client.
pub struct OtlpLogsGrpcClient {
    endpoint: OtlpLogsGrpcEndpoint,
    send_request: Option<SendRequest<Bytes>>,
    tls_connector: Option<TlsConnector>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl OtlpLogsGrpcClient {
    pub const RPC_PATH: &'static str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

    /// Creates a new gRPC client targeting the given endpoint URL with explicit timeouts.
    pub fn new(
        endpoint_url: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, PluginError> {
        let endpoint = OtlpLogsGrpcEndpoint::parse(endpoint_url)?;

        let tls_connector = if endpoint.is_tls {
            let root_store = rustls::RootCertStore::empty();
            let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|e| PluginError::Tls(format!("Failed to set TLS protocol versions: {e}")))?
            .with_root_certificates(root_store)
            .with_no_client_auth();

            config.alpn_protocols = vec![b"h2".to_vec()];
            Some(TlsConnector::from(Arc::new(config)))
        } else {
            None
        };

        Ok(Self {
            endpoint,
            send_request: None,
            tls_connector,
            connect_timeout,
            request_timeout,
        })
    }

    /// Connects a new HTTP/2 session and spawns the connection driver onto Tokio.
    async fn connect(&mut self) -> Result<(), PluginError> {
        let addr_str = format!("{}:{}", self.endpoint.host, self.endpoint.port);
        let tcp = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&addr_str))
            .await
            .map_err(|_| {
                PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to gRPC collector at {addr_str}"),
                ))
            })?
            .map_err(|e| {
                PluginError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to connect to gRPC collector at {addr_str}: {e}"),
                ))
            })?;

        let _ = tcp.set_nodelay(true);

        let io_stream = if self.endpoint.is_tls {
            let connector = self
                .tls_connector
                .as_ref()
                .ok_or_else(|| PluginError::Tls("TLS connector not initialized".into()))?;
            let server_name =
                rustls::pki_types::ServerName::try_from(self.endpoint.host.clone())
                    .map_err(|e| PluginError::Tls(format!("Invalid DNS server name: {e}")))?;

            let tls_stream =
                tokio::time::timeout(self.connect_timeout, connector.connect(server_name, tcp))
                    .await
                    .map_err(|_| {
                        PluginError::Tls(format!(
                            "TLS handshake timeout to gRPC collector at {addr_str}"
                        ))
                    })?
                    .map_err(|e| {
                        PluginError::Tls(format!(
                            "TLS handshake failed to gRPC collector at {addr_str}: {e}"
                        ))
                    })?;

            GrpcIoStream::Tls(Box::new(tls_stream))
        } else {
            GrpcIoStream::Plain(tcp)
        };

        let (send_request, connection) = handshake(io_stream).await.map_err(|e| {
            PluginError::Http(format!("HTTP/2 handshake failed to {addr_str}: {e}"))
        })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!("H2 connection driver closed: {e}");
            }
        });

        self.send_request = Some(send_request);
        Ok(())
    }

    /// Sends a binary Protobuf logs payload via gRPC length-prefixed message over HTTP/2.
    pub async fn send_logs(
        &mut self,
        proto_bytes: &[u8],
        custom_headers: &HeaderMap,
    ) -> Result<(), PluginError> {
        match self.send_logs_attempt(proto_bytes, custom_headers).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "gRPC log export failed on warm stream; reconnecting to retry once"
                );
                self.send_request = None;
                self.connect().await?;
                self.send_logs_attempt(proto_bytes, custom_headers).await
            }
        }
    }

    async fn send_logs_attempt(
        &mut self,
        proto_bytes: &[u8],
        custom_headers: &HeaderMap,
    ) -> Result<(), PluginError> {
        if self.send_request.is_none() {
            self.connect().await?;
        }

        let send_req = self.send_request.as_mut().unwrap();

        let uri = http::Uri::builder()
            .scheme(if self.endpoint.is_tls {
                "https"
            } else {
                "http"
            })
            .authority(self.endpoint.host_header.as_str())
            .path_and_query(Self::RPC_PATH)
            .build()
            .map_err(|e| PluginError::Config(format!("Invalid gRPC URI: {e}")))?;

        let mut req = http::Request::builder()
            .method(http::Method::POST)
            .uri(uri)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header(http::header::TE, "trailers")
            .header(http::header::USER_AGENT, "velda-edge/0.1.0");

        for (k, v) in custom_headers {
            req = req.header(k, v);
        }

        let request = req
            .body(())
            .map_err(|e| PluginError::Config(format!("Failed to build HTTP request: {e}")))?;

        let (response_future, mut send_stream) = send_req
            .clone()
            .ready()
            .await
            .map_err(|e| PluginError::Http(format!("HTTP/2 client not ready: {e}")))?
            .send_request(request, false)
            .map_err(|e| PluginError::Http(format!("Failed to send HTTP/2 request: {e}")))?;

        let mut lpm_buf = BytesMut::with_capacity(5 + proto_bytes.len());
        velda_grpc::encode_grpc_frame(proto_bytes, false, &mut lpm_buf);

        send_stream
            .send_data(lpm_buf.freeze(), true)
            .map_err(|e| PluginError::Http(format!("Failed to send gRPC frame: {e}")))?;

        let response = tokio::time::timeout(self.request_timeout, response_future)
            .await
            .map_err(|_| PluginError::Http("gRPC response headers timeout".into()))?
            .map_err(|e| PluginError::Http(format!("HTTP/2 response error: {e}")))?;

        // Fast-path: Check if headers contain Trailers-Only grpc-status
        if let Some(status_val) = response.headers().get("grpc-status") {
            let status_str = status_val.to_str().unwrap_or("0");
            if status_str != "0" {
                let msg = response
                    .headers()
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown");
                return Err(PluginError::Http(format!(
                    "gRPC rejected with status {status_str}: {msg}"
                )));
            }
            return Ok(());
        }

        // Standard path: Drain body stream and inspect trailers
        let mut body = response.into_body();
        while let Some(chunk) = body.data().await {
            let _ = chunk.map_err(|e| PluginError::Http(format!("H2 body error: {e}")))?;
        }

        let trailers = body
            .trailers()
            .await
            .map_err(|e| PluginError::Http(format!("H2 trailers error: {e}")))?;

        let grpc_status = trailers
            .as_ref()
            .and_then(|t| t.get("grpc-status"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("0");

        if grpc_status == "0" {
            Ok(())
        } else {
            let msg = trailers
                .as_ref()
                .and_then(|t| t.get("grpc-message"))
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown error");
            Err(PluginError::Http(format!(
                "gRPC rejected with status {grpc_status}: {msg}"
            )))
        }
    }
}
