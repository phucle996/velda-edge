//! Persistent Keep-Alive Grafana Loki HTTP/1.1 Client (`loki_log_shipper::json::client`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                           LOKI HTTP/1.1 KEEP-ALIVE FLOW                                │
//! │                                                                                        │
//! │  send_logs(json_payload)                                                               │
//! │        │                                                                               │
//! │        ├─── Warm Stream Available?                                                     │
//! │        │          ├── YES ──► Write HTTP/1.1 POST /loki/api/v1/push                     │
//! │        │          └── NO  ──► Connect TCP ──► Set TCP_NODELAY ──► TLS Handshake (if)   │
//! │        │                                                                               │
//! │        ▼                                                                               │
//! │  Read Status Code (httparse):                                                          │
//! │        ├── 200..299 ──► Success, retain persistent stream in pool                      │
//! │        ├── EOF / Idle Teardown ──► Clear stream, transparent reconnect retry (attempt 2)│
//! │        └── Timeout ──► Drop stream, return PluginError::Io                             │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::PluginError;

enum TransportStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl TransportStream {
    async fn write_all(&mut self, src: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(src).await,
            Self::Tls(s) => s.write_all(src).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush().await,
            Self::Tls(s) => s.flush().await,
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf).await,
            Self::Tls(s) => s.read(buf).await,
        }
    }
}

/// Parsed endpoint target specification for Grafana Loki HTTP communication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiLogEndpoint {
    pub is_tls: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub host_header: String,
}

impl LokiLogEndpoint {
    /// Parses an HTTP/HTTPS URL into a [`LokiLogEndpoint`].
    pub fn parse(endpoint_url: &str, default_path: &str) -> Result<Self, PluginError> {
        let trimmed = endpoint_url.trim();
        let (is_tls, rest) = if let Some(stripped) = trimmed.strip_prefix("https://") {
            (true, stripped)
        } else if let Some(stripped) = trimmed.strip_prefix("http://") {
            (false, stripped)
        } else {
            return Err(PluginError::Config(format!(
                "Invalid Loki endpoint URL '{endpoint_url}': must begin with http:// or https://"
            )));
        };

        let (authority, path_part) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, default_path),
        };

        if authority.is_empty() {
            return Err(PluginError::Config(format!(
                "Invalid Loki endpoint URL '{endpoint_url}': missing host authority"
            )));
        }

        let (host, port) = match authority.rfind(':') {
            Some(idx) if !authority.ends_with(']') => {
                let host = authority[..idx].trim_matches(|c| c == '[' || c == ']');
                let port_str = &authority[idx + 1..];
                let port = port_str.parse::<u16>().map_err(|_| {
                    PluginError::Config(format!(
                        "Invalid port '{port_str}' in Loki endpoint '{endpoint_url}'"
                    ))
                })?;
                (host.to_string(), port)
            }
            _ => {
                let host = authority.trim_matches(|c| c == '[' || c == ']');
                let default_port = if is_tls { 443 } else { 3100 };
                (host.to_string(), default_port)
            }
        };

        let path = if path_part.is_empty() {
            default_path.to_string()
        } else {
            path_part.to_string()
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
            path,
            host_header,
        })
    }
}

/// Standalone Persistent Keep-Alive Grafana Loki HTTP Client.
pub struct LokiLogClient {
    endpoint: LokiLogEndpoint,
    tenant_id: String,
    stream: Option<TransportStream>,
    tls_connector: Option<TlsConnector>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl LokiLogClient {
    /// Creates a new client targeting the given endpoint URL with tenant id and explicit timeouts.
    pub fn new(
        endpoint_url: &str,
        tenant_id: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, PluginError> {
        let endpoint = LokiLogEndpoint::parse(endpoint_url, "/loki/api/v1/push")?;
        let tls_connector = if endpoint.is_tls {
            let root_store = rustls::RootCertStore::empty();
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|e| PluginError::Tls(format!("Failed to set TLS protocol versions: {e}")))?
            .with_root_certificates(root_store)
            .with_no_client_auth();
            Some(TlsConnector::from(Arc::new(config)))
        } else {
            None
        };

        Ok(Self {
            endpoint,
            tenant_id: tenant_id.to_string(),
            stream: None,
            tls_connector,
            connect_timeout,
            request_timeout,
        })
    }

    async fn connect(&self) -> Result<TransportStream, PluginError> {
        let addr_str = format!("{}:{}", self.endpoint.host, self.endpoint.port);
        let tcp = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&addr_str))
            .await
            .map_err(|_| {
                PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to Loki server at {addr_str}"),
                ))
            })?
            .map_err(|e| {
                PluginError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to connect to Loki server at {addr_str}: {e}"),
                ))
            })?;

        let _ = tcp.set_nodelay(true);

        if self.endpoint.is_tls {
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
                            "TLS handshake timeout to Loki server at {addr_str}"
                        ))
                    })?
                    .map_err(|e| {
                        PluginError::Tls(format!(
                            "TLS handshake failed to Loki server at {addr_str}: {e}"
                        ))
                    })?;

            Ok(TransportStream::Tls(Box::new(tls_stream)))
        } else {
            Ok(TransportStream::Plain(tcp))
        }
    }

    /// Transmits a Loki JSON push payload, maintaining persistent keep-alive connection.
    pub async fn send_logs(
        &mut self,
        json_payload: &[u8],
        custom_headers: &HeaderMap,
    ) -> Result<u16, PluginError> {
        let mut attempts = 0;
        loop {
            attempts += 1;

            if self.stream.is_none() {
                self.stream = Some(self.connect().await?);
            }

            let stream = self.stream.as_mut().expect("Stream established");
            match tokio::time::timeout(
                self.request_timeout,
                Self::write_and_read_response(
                    stream,
                    &self.endpoint,
                    &self.tenant_id,
                    json_payload,
                    custom_headers,
                ),
            )
            .await
            {
                Ok(Ok((status, keep_alive))) => {
                    if !keep_alive {
                        self.stream = None;
                    }
                    return Ok(status);
                }
                Ok(Err(e)) => {
                    self.stream = None;
                    if attempts < 2 {
                        tracing::debug!(
                            error = %e,
                            "Loki connection closed while idle, retrying once with new connection"
                        );
                        continue;
                    }
                    return Err(e);
                }
                Err(_) => {
                    self.stream = None;
                    return Err(PluginError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Loki flush request timed out",
                    )));
                }
            }
        }
    }

    async fn write_and_read_response(
        stream: &mut TransportStream,
        endpoint: &LokiLogEndpoint,
        tenant_id: &str,
        payload: &[u8],
        custom_headers: &HeaderMap,
    ) -> Result<(u16, bool), PluginError> {
        let mut head = String::with_capacity(512);
        write!(
            head,
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
            endpoint.path, endpoint.host_header, payload.len()
        )
        .map_err(|e| PluginError::Http(format!("Failed to format request head: {e}")))?;

        if !tenant_id.is_empty() {
            write!(head, "X-Scope-OrgID: {tenant_id}\r\n")
                .map_err(|e| PluginError::Http(format!("Failed to write tenant header: {e}")))?;
        }

        for (k, v) in custom_headers {
            if let Ok(v_str) = v.to_str() {
                write!(head, "{}: {}\r\n", k.as_str(), v_str).map_err(|e| {
                    PluginError::Http(format!("Failed to format custom header: {e}"))
                })?;
            }
        }
        head.push_str("\r\n");

        stream.write_all(head.as_bytes()).await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to write HTTP header: {e}"),
            ))
        })?;

        stream.write_all(payload).await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to write HTTP body: {e}"),
            ))
        })?;

        stream.flush().await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to flush HTTP stream: {e}"),
            ))
        })?;

        // Read HTTP status response
        let mut response_buf = [0u8; 1024];
        let bytes_read = stream.read(&mut response_buf).await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to read HTTP response: {e}"),
            ))
        })?;

        if bytes_read == 0 {
            return Err(PluginError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Loki server closed connection (EOF)",
            )));
        }

        let resp_slice = &response_buf[..bytes_read];
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut parsed_resp = httparse::Response::new(&mut headers);

        match parsed_resp.parse(resp_slice) {
            Ok(httparse::Status::Complete(_)) | Ok(httparse::Status::Partial) => {
                let status = parsed_resp.code.unwrap_or(500);
                let mut keep_alive = true;
                for h in parsed_resp.headers {
                    if h.name.eq_ignore_ascii_case("connection")
                        && h.value.eq_ignore_ascii_case(b"close")
                    {
                        keep_alive = false;
                        break;
                    }
                }
                Ok((status, keep_alive))
            }
            Err(e) => Err(PluginError::Http(format!(
                "Failed to parse Loki HTTP response: {e}"
            ))),
        }
    }
}
