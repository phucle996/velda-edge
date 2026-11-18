//! Persistent Keep-Alive OTLP HTTP/1.1 Client for Traces (`otlp_trace_exporter::json::client`).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use rustls::ClientConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::config::OutboundTlsConfig;
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
pub struct OtlpTraceTarget {
    pub is_tls: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub host_header: String,
}

impl OtlpTraceTarget {
    /// Parses an HTTP/HTTPS URL into an [`OtlpTraceTarget`].
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

/// Standalone Persistent Keep-Alive OTLP Traces Client.
pub struct OtlpTraceHttpClient {
    endpoint: OtlpTraceTarget,
    stream: Option<TransportStream>,
    tls_config: Option<Arc<ClientConfig>>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl OtlpTraceHttpClient {
    /// Creates a new client targeting the given endpoint URL with explicit timeouts and outbound TLS configuration.
    pub fn new(
        endpoint_url: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
        tls_cfg: &OutboundTlsConfig,
    ) -> Result<Self, PluginError> {
        let endpoint = OtlpTraceTarget::parse(endpoint_url, "/v1/traces")?;
        let tls_config = if endpoint.is_tls {
            Some(tls_cfg.build_client_config(&["http/1.1"])?)
        } else {
            None
        };

        Ok(Self {
            endpoint,
            stream: None,
            tls_config,
            connect_timeout,
            request_timeout,
        })
    }

    async fn connect(&mut self) -> Result<(), PluginError> {
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
            let config = self
                .tls_config
                .as_ref()
                .ok_or_else(|| PluginError::Tls("TLS config not initialized".into()))?;

            let tls_stream = tokio::time::timeout(
                self.connect_timeout,
                velda_tls::client::connect(tcp, config, &self.endpoint.host),
            )
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

            self.stream = Some(TransportStream::Tls(Box::new(tls_stream)));
        } else {
            self.stream = Some(TransportStream::Plain(tcp));
        }

        Ok(())
    }

    /// Sends raw JSON payload string to the collector over persistent connection.
    pub async fn send_traces_json(
        &mut self,
        json_payload: &str,
        custom_headers: &HeaderMap,
    ) -> Result<(), PluginError> {
        match self.send_attempt(json_payload, custom_headers).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "OTLP HTTP trace export failed on persistent connection; retrying with fresh socket"
                );
                self.stream = None;
                self.connect().await?;
                self.send_attempt(json_payload, custom_headers).await
            }
        }
    }

    async fn send_attempt(
        &mut self,
        json_payload: &str,
        custom_headers: &HeaderMap,
    ) -> Result<(), PluginError> {
        if self.stream.is_none() {
            self.connect().await?;
        }

        let stream = self.stream.as_mut().unwrap();

        let mut req_header = String::with_capacity(256);
        let _ = write!(
            req_header,
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
            self.endpoint.path,
            self.endpoint.host_header,
            json_payload.len()
        );

        for (k, v) in custom_headers {
            if let Ok(val) = v.to_str() {
                let _ = write!(req_header, "{}: {}\r\n", k.as_str(), val);
            }
        }
        req_header.push_str("\r\n");

        tokio::time::timeout(self.request_timeout, async {
            stream
                .write_all(req_header.as_bytes())
                .await
                .map_err(PluginError::Io)?;
            stream
                .write_all(json_payload.as_bytes())
                .await
                .map_err(PluginError::Io)?;
            stream.flush().await.map_err(PluginError::Io)?;

            let mut resp_buf = [0u8; 1024];
            let n = stream.read(&mut resp_buf).await.map_err(PluginError::Io)?;
            if n == 0 {
                return Err(PluginError::Http(
                    "Remote collector closed connection prematurely (EOF)".into(),
                ));
            }

            let resp_str = String::from_utf8_lossy(&resp_buf[..n]);
            if !resp_str.starts_with("HTTP/1.1 200") && !resp_str.starts_with("HTTP/1.1 202") {
                let status_line = resp_str.lines().next().unwrap_or("Unknown response");
                return Err(PluginError::Http(format!(
                    "OTLP Collector rejected traces: {status_line}"
                )));
            }

            Ok(())
        })
        .await
        .map_err(|_| {
            PluginError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Request timeout while awaiting OTLP collector HTTP response",
            ))
        })?
    }
}
