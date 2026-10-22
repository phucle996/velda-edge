//! Persistent Keep-Alive OTLP HTTP/1.1 Client for Logs (`opentelemetry_logger`).
//!
//! Maintains a warm, persistent TCP/TLS stream to an OpenTelemetry Collector endpoint
//! (`/v1/logs`), avoiding connection churn and TLS handshake storms on repeated flushes.

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

/// Parsed endpoint target specification for OTLP communication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpLogEndpoint {
    pub is_tls: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub host_header: String,
}

impl OtlpLogEndpoint {
    /// Parses an HTTP/HTTPS URL into an [`OtlpLogEndpoint`].
    pub fn parse(endpoint_url: &str, default_path: &str) -> Result<Self, PluginError> {
        let trimmed = endpoint_url.trim();
        let (is_tls, rest) = if let Some(stripped) = trimmed.strip_prefix("https://") {
            (true, stripped)
        } else if let Some(stripped) = trimmed.strip_prefix("http://") {
            (false, stripped)
        } else {
            return Err(PluginError::Config(format!(
                "Invalid OTLP endpoint URL '{endpoint_url}': must begin with http:// or https://"
            )));
        };

        let (authority, path_part) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, default_path),
        };

        if authority.is_empty() {
            return Err(PluginError::Config(format!(
                "Invalid OTLP endpoint URL '{endpoint_url}': missing host authority"
            )));
        }

        let (host, port) = match authority.rfind(':') {
            Some(idx) if !authority.ends_with(']') => {
                let host = authority[..idx].trim_matches(|c| c == '[' || c == ']');
                let port_str = &authority[idx + 1..];
                let port = port_str.parse::<u16>().map_err(|_| {
                    PluginError::Config(format!(
                        "Invalid port '{port_str}' in OTLP endpoint '{endpoint_url}'"
                    ))
                })?;
                (host.to_string(), port)
            }
            _ => {
                let host = authority.trim_matches(|c| c == '[' || c == ']');
                let default_port = if is_tls { 443 } else { 4318 };
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

/// Standalone Persistent Keep-Alive OTLP Logs Client.
pub struct OtlpLogClient {
    endpoint: OtlpLogEndpoint,
    stream: Option<TransportStream>,
    tls_connector: Option<TlsConnector>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl OtlpLogClient {
    /// Creates a new client targeting the given endpoint URL with explicit timeouts.
    pub fn new(
        endpoint_url: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, PluginError> {
        let endpoint = OtlpLogEndpoint::parse(endpoint_url, "/v1/logs")?;
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
                    format!("Connection timeout to OTLP collector at {addr_str}"),
                ))
            })?
            .map_err(|e| {
                PluginError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to connect to OTLP collector at {addr_str}: {e}"),
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
                            "TLS handshake timeout to OTLP collector at {addr_str}"
                        ))
                    })?
                    .map_err(|e| {
                        PluginError::Tls(format!(
                            "TLS handshake failed to OTLP collector at {addr_str}: {e}"
                        ))
                    })?;

            Ok(TransportStream::Tls(Box::new(tls_stream)))
        } else {
            Ok(TransportStream::Plain(tcp))
        }
    }

    /// Transmits an OTLP JSON payload, maintaining persistent keep-alive connection.
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
                Self::write_and_read_response(stream, &self.endpoint, json_payload, custom_headers),
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
                        tracing::debug!(error = %e, "OTLP logs connection closed while idle, retrying once with new connection");
                        continue;
                    }
                    return Err(e);
                }
                Err(_) => {
                    self.stream = None;
                    return Err(PluginError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "OTLP logs flush request timed out",
                    )));
                }
            }
        }
    }

    async fn write_and_read_response(
        stream: &mut TransportStream,
        endpoint: &OtlpLogEndpoint,
        payload: &[u8],
        custom_headers: &HeaderMap,
    ) -> Result<(u16, bool), PluginError> {
        let mut header_buf = String::with_capacity(512);
        let _ = write!(
            header_buf,
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
            endpoint.path,
            endpoint.host_header,
            payload.len()
        );

        for (k, v) in custom_headers {
            let _ = write!(
                header_buf,
                "{}: {}\r\n",
                k.as_str(),
                v.to_str().unwrap_or("")
            );
        }
        header_buf.push_str("\r\n");

        stream.write_all(header_buf.as_bytes()).await?;
        if !payload.is_empty() {
            stream.write_all(payload).await?;
        }
        stream.flush().await?;

        let mut read_buf = [0u8; 4096];
        let mut total_read = 0;

        loop {
            let n = stream.read(&mut read_buf[total_read..]).await?;
            if n == 0 {
                if total_read == 0 {
                    return Err(PluginError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "Remote server closed connection before sending response",
                    )));
                }
                break;
            }
            total_read += n;

            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut resp = httparse::Response::new(&mut headers);
            match resp.parse(&read_buf[..total_read]) {
                Ok(httparse::Status::Complete(body_start)) => {
                    let status = resp.code.unwrap_or(200);
                    let mut keep_alive = true;
                    let mut content_length = None;

                    for h in resp.headers.iter() {
                        if h.name.eq_ignore_ascii_case("connection")
                            && h.value.eq_ignore_ascii_case(b"close")
                        {
                            keep_alive = false;
                        } else if h.name.eq_ignore_ascii_case("content-length")
                            && let Ok(val_str) = std::str::from_utf8(h.value)
                        {
                            content_length = val_str.trim().parse::<usize>().ok();
                        }
                    }

                    if let Some(cl) = content_length {
                        let body_read = total_read.saturating_sub(body_start);
                        if body_read < cl {
                            let mut remaining = cl - body_read;
                            let mut discard = [0u8; 1024];
                            while remaining > 0 {
                                let r = stream.read(&mut discard).await?;
                                if r == 0 {
                                    keep_alive = false;
                                    break;
                                }
                                remaining = remaining.saturating_sub(r);
                            }
                        }
                    }

                    return Ok((status, keep_alive));
                }
                Ok(httparse::Status::Partial) => {
                    if total_read >= read_buf.len() {
                        return Err(PluginError::Http(
                            "HTTP response headers exceeded buffer limit".into(),
                        ));
                    }
                    continue;
                }
                Err(e) => {
                    return Err(PluginError::Http(format!(
                        "Invalid HTTP response from OTLP collector: {e}"
                    )));
                }
            }
        }

        Err(PluginError::Http(
            "Connection closed unexpectedly by OTLP collector".into(),
        ))
    }
}
