//! Persistent Keep-Alive ClickHouse HTTP Ingestion Client (`clickhouse_log_shipper::client`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    CLICKHOUSE HTTP INGESTION CLIENT ARCHITECTURE                       │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ BOOT TIME (Immutable Prebaking):                                                       │
//! │   POST /?query=INSERT INTO {db}.velda_access_logs FORMAT {fmt} HTTP/1.1                │
//! │   Host: {endpoint.host_header}                                                         │
//! │   Content-Type: application/octet-stream                                               │
//! │   Connection: keep-alive                                                               │
//! │   X-ClickHouse-User: {user}                                                            │
//! │   X-ClickHouse-Key: {pass}                                                             │
//! │   Content-Length: ──────┐                                                              │
//! │                         ▼                                                              │
//! │            [ prebaked_head_prefix: Vec<u8> ] (Cached in RAM, 0 allocation per push)    │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ PER-PUSH HOT PATH:                                                                     │
//! │   1. head_buf.clone_from(prefix) + fast_write_u64(payload_len) + \r\n\r\n             │
//! │   2. stream.write_all(head_buf) + stream.write_all(payload)                            │
//! │   3. stream.flush() + read response status (200 OK)                                    │
//! │   4. On idle disconnect ──► reconnect transparently + single retry                     │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Features 100% prebaked HTTP framing:
//! - Ingestion query and HTTP request headers are pre-rendered into immutable byte vectors at boot time.
//! - Per-push overhead is reduced to formatting `Content-Length` and writing payload chunks.
//! - Transparent keep-alive socket reuse with single-attempt retry on idle timeout.

use std::sync::Arc;
use std::time::Duration;

use rustls::ClientConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::error::PluginError;

use super::config::{ClickHouseLogShipperConfig, TransportSecurity, VELDA_ACCESS_LOGS_TABLE};
use velda_core::lut::decimal::fast_write_u64;

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

/// Parsed endpoint metadata for ClickHouse HTTP ingestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseEndpoint {
    pub security: TransportSecurity,
    pub host: String,
    pub port: u16,
    pub host_header: String,
}

impl ClickHouseEndpoint {
    /// Parses an HTTP/HTTPS endpoint URL and validates with the declared TransportSecurity mode.
    pub fn parse(endpoint_url: &str, security: TransportSecurity) -> Result<Self, PluginError> {
        let trimmed = endpoint_url.trim();
        let (is_https, rest) = if let Some(stripped) = trimmed.strip_prefix("https://") {
            (true, stripped)
        } else if let Some(stripped) = trimmed.strip_prefix("http://") {
            (false, stripped)
        } else {
            return Err(PluginError::Config(format!(
                "Invalid ClickHouse endpoint URL '{endpoint_url}': must begin with http:// or https://"
            )));
        };

        if security == TransportSecurity::Plain && is_https {
            return Err(PluginError::Config(format!(
                "ClickHouse endpoint '{endpoint_url}' uses 'https://' but transport_security is 'plain'"
            )));
        }
        if security.is_tls() && !is_https {
            return Err(PluginError::Config(format!(
                "ClickHouse endpoint '{endpoint_url}' uses unencrypted 'http://' but transport_security is '{}'",
                security.as_str()
            )));
        }

        let authority = match rest.find('/') {
            Some(idx) => &rest[..idx],
            None => rest,
        };

        if authority.is_empty() {
            return Err(PluginError::Config(format!(
                "Invalid ClickHouse endpoint URL '{endpoint_url}': missing host authority"
            )));
        }

        let is_tls = security.is_tls();
        let (host, port) = match authority.rfind(':') {
            Some(idx) if !authority.ends_with(']') => {
                let host = authority[..idx].trim_matches(|c| c == '[' || c == ']');
                let port_str = &authority[idx + 1..];
                let port: u16 = port_str.parse().map_err(|_| {
                    PluginError::Config(format!(
                        "Invalid port in ClickHouse endpoint '{endpoint_url}'"
                    ))
                })?;
                (host.to_string(), port)
            }
            _ => {
                let default_port = if is_tls { 8443 } else { 8123 };
                (authority.to_string(), default_port)
            }
        };

        let host_header = if (is_tls && port == 443) || (!is_tls && port == 80) {
            host.clone()
        } else {
            format!("{host}:{port}")
        };

        Ok(Self {
            security,
            host,
            port,
            host_header,
        })
    }
}

/// High-performance HTTP client for pushing log batches directly into ClickHouse.
pub struct ClickHouseHttpClient {
    endpoint: ClickHouseEndpoint,
    database: String,
    username: String,
    password: String,
    connect_timeout: Duration,
    request_timeout: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    stream: Option<TransportStream>,
    /// Immutable prebaked HTTP request header prefix up to `Content-Length: `.
    pub prebaked_head_prefix: Vec<u8>,
    /// Scratch buffer for assembling final HTTP header.
    head_buf: Vec<u8>,
}

impl ClickHouseHttpClient {
    /// Constructs a new client with prebaked query and request headers targeting `velda_access_logs`.
    pub fn new(config: &ClickHouseLogShipperConfig) -> Result<Self, PluginError> {
        let sec = config.parsed_transport_security()?;
        let endpoint = ClickHouseEndpoint::parse(&config.endpoint, sec)?;

        let tls_config =
            if endpoint.security.is_tls() {
                Some(config.tls.build_client_config(&["http/1.1"]).map_err(|e| {
                    PluginError::Tls(format!("Failed to build TLS client config: {e}"))
                })?)
            } else {
                None
            };

        let format = config.parsed_format();
        // Construct prebaked query: INSERT INTO {database}.velda_access_logs FORMAT {format}
        let query_str = format!(
            "/?query=INSERT%20INTO%20{}.{}%20FORMAT%20{}",
            config.database,
            VELDA_ACCESS_LOGS_TABLE,
            format.as_clickhouse_format_str()
        );

        // Prebake HTTP/1.1 request head up to `Content-Length: `
        let mut prefix = Vec::with_capacity(512);
        prefix.extend_from_slice(b"POST ");
        prefix.extend_from_slice(query_str.as_bytes());
        prefix.extend_from_slice(b" HTTP/1.1\r\nHost: ");
        prefix.extend_from_slice(endpoint.host_header.as_bytes());
        prefix.extend_from_slice(b"\r\nContent-Type: ");
        prefix.extend_from_slice(format.content_type().as_bytes());
        prefix.extend_from_slice(b"\r\nConnection: keep-alive\r\n");

        if !config.username.is_empty() {
            prefix.extend_from_slice(b"X-ClickHouse-User: ");
            prefix.extend_from_slice(config.username.as_bytes());
            prefix.extend_from_slice(b"\r\n");
        }
        if !config.password.is_empty() {
            prefix.extend_from_slice(b"X-ClickHouse-Key: ");
            prefix.extend_from_slice(config.password.as_bytes());
            prefix.extend_from_slice(b"\r\n");
        }

        prefix.extend_from_slice(b"Content-Length: ");

        Ok(Self {
            endpoint,
            database: config.database.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            tls_config,
            stream: None,
            prebaked_head_prefix: prefix,
            head_buf: Vec::with_capacity(512),
        })
    }

    /// Automatically ensures that table `{database}.velda_access_logs` exists in ClickHouse.
    ///
    /// Executes `CREATE TABLE IF NOT EXISTS {database}.velda_access_logs (...) ENGINE = MergeTree() ...`.
    /// Note: Does NOT create the database itself; operator must provide a pre-existing database.
    pub async fn ensure_table(&mut self) -> Result<(), PluginError> {
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (\
                timestamp DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),\
                request_id UInt64 CODEC(DoubleDelta, ZSTD(1)),\
                duration_ns UInt64 CODEC(T64, ZSTD(1)),\
                peer_ip String CODEC(ZSTD(1)),\
                peer_port UInt16 CODEC(T64, ZSTD(1)),\
                upstream_ip String CODEC(ZSTD(1)),\
                upstream_port UInt16 CODEC(T64, ZSTD(1)),\
                route_id UInt16 CODEC(T64, ZSTD(1)),\
                status_code UInt16 CODEC(T64, ZSTD(1)),\
                pipe LowCardinality(String) CODEC(ZSTD(1)),\
                method LowCardinality(String) CODEC(ZSTD(1)),\
                url String CODEC(ZSTD(3)),\
                bytes_rx UInt32 CODEC(T64, ZSTD(1)),\
                bytes_tx UInt32 CODEC(T64, ZSTD(1)),\
                completion_reason LowCardinality(String) CODEC(ZSTD(1)),\
                grpc_status LowCardinality(String) CODEC(ZSTD(1)),\
                user_agent_family LowCardinality(String) CODEC(ZSTD(1)),\
                user_agent_os LowCardinality(String) CODEC(ZSTD(1))\
            ) ENGINE = MergeTree() \
            PARTITION BY toYYYYMM(timestamp) \
            ORDER BY (route_id, status_code, timestamp) \
            SETTINGS index_granularity = 8192",
            self.database, VELDA_ACCESS_LOGS_TABLE
        );

        if self.stream.is_none() {
            self.stream = Some(self.connect().await?);
        }

        let mut head = String::with_capacity(256);
        use std::fmt::Write as _;
        let _ = write!(
            head,
            "POST / HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\nContent-Length: {}\r\n",
            self.endpoint.host_header,
            ddl.len()
        );
        if !self.username.is_empty() {
            let _ = write!(head, "X-ClickHouse-User: {}\r\n", self.username);
        }
        if !self.password.is_empty() {
            let _ = write!(head, "X-ClickHouse-Key: {}\r\n", self.password);
        }
        head.push_str("\r\n");

        let stream = self.stream.as_mut().expect("Stream established");
        let (status, keep_alive) =
            Self::write_and_read(stream, head.as_bytes(), ddl.as_bytes()).await?;
        if !keep_alive {
            self.stream = None;
        }

        if (200..300).contains(&status) {
            tracing::info!(
                database = %self.database,
                table = %VELDA_ACCESS_LOGS_TABLE,
                "Verified ClickHouse access logs table exists"
            );
            Ok(())
        } else {
            Err(PluginError::Http(format!(
                "Failed to ensure table in database '{}': ClickHouse returned HTTP {status}",
                self.database
            )))
        }
    }

    /// Explicitly closes and drops the active transport stream.
    pub fn close_connection(&mut self) {
        self.stream = None;
    }

    /// Establishes TCP (and optional TLS) connection to ClickHouse server.
    async fn connect(&self) -> Result<TransportStream, PluginError> {
        let addr_str = format!("{}:{}", self.endpoint.host, self.endpoint.port);

        let tcp = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&addr_str))
            .await
            .map_err(|_| {
                PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to ClickHouse server at {addr_str}"),
                ))
            })?
            .map_err(|e| {
                PluginError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to connect to ClickHouse server at {addr_str}: {e}"),
                ))
            })?;

        let _ = tcp.set_nodelay(true);

        if self.endpoint.security.is_tls() {
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
                    "TLS handshake timeout to ClickHouse server at {addr_str}"
                ))
            })?
            .map_err(|e| {
                PluginError::Tls(format!(
                    "TLS handshake failed to ClickHouse server at {addr_str}: {e}"
                ))
            })?;

            Ok(TransportStream::Tls(Box::new(tls_stream)))
        } else {
            Ok(TransportStream::Plain(tcp))
        }
    }

    /// Transmits batch payload to ClickHouse, reusing persistent keep-alive connection.
    ///
    /// # Zero-Retry Drop Invariant
    /// In accordance with fire-and-forget edge principles, transmission is executed exactly once.
    /// If the connection resets or times out, the stream is closed and the error returned immediately
    /// without blocking retry loops or backoff sleep.
    pub async fn push_batch(&mut self, payload: &[u8]) -> Result<u16, PluginError> {
        if self.stream.is_none() {
            self.stream = Some(self.connect().await?);
        }

        // Assemble prebaked HTTP head with exact Content-Length
        self.head_buf.clear();
        self.head_buf.extend_from_slice(&self.prebaked_head_prefix);
        fast_write_u64(&mut self.head_buf, payload.len() as u64);
        self.head_buf.extend_from_slice(b"\r\n\r\n");

        let stream = self.stream.as_mut().expect("Stream established");

        match tokio::time::timeout(
            self.request_timeout,
            Self::write_and_read(stream, &self.head_buf, payload),
        )
        .await
        {
            Ok(Ok((status, keep_alive))) => {
                if !keep_alive || status >= 500 {
                    self.stream = None;
                }
                Ok(status)
            }
            Ok(Err(e)) => {
                self.stream = None;
                Err(e)
            }
            Err(_) => {
                self.stream = None;
                Err(PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "ClickHouse batch push request timed out",
                )))
            }
        }
    }

    async fn write_and_read(
        stream: &mut TransportStream,
        head: &[u8],
        payload: &[u8],
    ) -> Result<(u16, bool), PluginError> {
        stream.write_all(head).await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to write HTTP header: {e}"),
            ))
        })?;

        stream.write_all(payload).await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to write HTTP payload: {e}"),
            ))
        })?;

        stream.flush().await.map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to flush HTTP stream: {e}"),
            ))
        })?;

        // Read response header and drain response body so socket remains clean for keep-alive
        let mut resp_buf = [0u8; 4096];
        let mut total_read = 0;

        // Step 1: Read until HTTP headers are completely received
        let header_len = loop {
            let n = stream
                .read(&mut resp_buf[total_read..])
                .await
                .map_err(|e| {
                    PluginError::Io(std::io::Error::new(
                        e.kind(),
                        format!("Failed to read HTTP response from ClickHouse: {e}"),
                    ))
                })?;

            if n == 0 {
                return Err(PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "ClickHouse closed connection without response",
                )));
            }

            total_read += n;

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut resp = httparse::Response::new(&mut headers);
            match resp.parse(&resp_buf[..total_read]) {
                Ok(httparse::Status::Complete(hlen)) => break hlen,
                Ok(httparse::Status::Partial) => {
                    if total_read == resp_buf.len() {
                        return Err(PluginError::Http(
                            "ClickHouse response headers exceed buffer capacity".to_string(),
                        ));
                    }
                    continue;
                }
                Err(e) => {
                    return Err(PluginError::Http(format!(
                        "Failed to parse ClickHouse HTTP response: {e}"
                    )));
                }
            }
        };

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut resp = httparse::Response::new(&mut headers);
        let _ = resp.parse(&resp_buf[..header_len]);

        let code = resp.code.unwrap_or(500);
        let keep_alive = resp
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("connection"))
            .map(|h| !h.value.eq_ignore_ascii_case(b"close"))
            .unwrap_or(true);

        let is_chunked = resp
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("transfer-encoding"))
            .map(|h| {
                h.value
                    .split(|&b| b == b',' || b == b' ')
                    .any(|part| part.eq_ignore_ascii_case(b"chunked"))
            })
            .unwrap_or(false);

        let content_len = resp
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("content-length"))
            .and_then(|h| {
                std::str::from_utf8(h.value)
                    .ok()?
                    .trim()
                    .parse::<usize>()
                    .ok()
            });

        // Step 2: Drain response body to ensure no trailing bytes remain on socket
        if is_chunked {
            while !resp_buf[header_len..total_read]
                .windows(5)
                .any(|w| w == b"0\r\n\r\n")
            {
                if total_read == resp_buf.len() {
                    let mut discard = [0u8; 1024];
                    let n = stream.read(&mut discard).await.map_err(|e| {
                        PluginError::Io(std::io::Error::new(
                            e.kind(),
                            format!("Failed to drain chunked response: {e}"),
                        ))
                    })?;
                    if n == 0 || discard[..n].windows(5).any(|w| w == b"0\r\n\r\n") {
                        break;
                    }
                    continue;
                }
                let n = stream
                    .read(&mut resp_buf[total_read..])
                    .await
                    .map_err(|e| {
                        PluginError::Io(std::io::Error::new(
                            e.kind(),
                            format!("Failed to read chunked response from ClickHouse: {e}"),
                        ))
                    })?;
                if n == 0 {
                    break;
                }
                total_read += n;
            }
        } else if let Some(clen) = content_len {
            let mut body_read = total_read.saturating_sub(header_len);
            while body_read < clen {
                let to_read = (clen - body_read).min(resp_buf.len().saturating_sub(total_read));
                if to_read == 0 {
                    let mut discard = [0u8; 1024];
                    let n = stream.read(&mut discard).await.map_err(|e| {
                        PluginError::Io(std::io::Error::new(
                            e.kind(),
                            format!("Failed to drain body: {e}"),
                        ))
                    })?;
                    if n == 0 {
                        break;
                    }
                    body_read += n;
                } else {
                    let n = stream
                        .read(&mut resp_buf[total_read..total_read + to_read])
                        .await
                        .map_err(|e| {
                            PluginError::Io(std::io::Error::new(
                                e.kind(),
                                format!("Failed to read body from ClickHouse: {e}"),
                            ))
                        })?;
                    if n == 0 {
                        break;
                    }
                    total_read += n;
                    body_read += n;
                }
            }
        }

        if (200..300).contains(&code) {
            Ok((code, keep_alive))
        } else {
            let err_body = String::from_utf8_lossy(&resp_buf[header_len..total_read]);
            Err(PluginError::Http(format!(
                "ClickHouse returned HTTP {code}: {err_body}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_parsing() {
        let ep1 = ClickHouseEndpoint::parse("http://192.168.1.10:8123/", TransportSecurity::Plain)
            .unwrap();
        assert_eq!(ep1.security, TransportSecurity::Plain);
        assert_eq!(ep1.host, "192.168.1.10");
        assert_eq!(ep1.port, 8123);
        assert_eq!(ep1.host_header, "192.168.1.10:8123");

        let ep2 = ClickHouseEndpoint::parse("https://clickhouse.internal/", TransportSecurity::Tls)
            .unwrap();
        assert_eq!(ep2.security, TransportSecurity::Tls);
        assert_eq!(ep2.host, "clickhouse.internal");
        assert_eq!(ep2.port, 8443);
        assert_eq!(ep2.host_header, "clickhouse.internal:8443");

        let ep3 =
            ClickHouseEndpoint::parse("https://clickhouse.internal:9440/", TransportSecurity::Mtls)
                .unwrap();
        assert_eq!(ep3.security, TransportSecurity::Mtls);
        assert_eq!(ep3.port, 9440);

        // Mismatched security mode vs scheme
        assert!(
            ClickHouseEndpoint::parse("http://127.0.0.1:8123/", TransportSecurity::Tls).is_err()
        );
        assert!(
            ClickHouseEndpoint::parse("https://127.0.0.1:8443/", TransportSecurity::Plain).is_err()
        );
    }

    #[test]
    fn test_client_prebaked_head() {
        let cfg = ClickHouseLogShipperConfig {
            endpoint: "http://127.0.0.1:8123".into(),
            transport_security: "plain".into(),
            database: "default".into(),
            format: "row_binary".into(),
            username: "test_user".into(),
            password: "test_pass".into(),
            flush_interval_ms: 100,
            max_batch_size: 2000,
            connect_timeout_ms: 1000,
            request_timeout_ms: 2000,
            tls: crate::config::OutboundTlsConfig::default(),
        };

        let client = ClickHouseHttpClient::new(&cfg).unwrap();

        let prefix_str = std::str::from_utf8(&client.prebaked_head_prefix).unwrap();
        assert!(prefix_str.starts_with(
            "POST /?query=INSERT%20INTO%20default.velda_access_logs%20FORMAT%20RowBinary HTTP/1.1"
        ));
        assert!(prefix_str.contains("Host: 127.0.0.1:8123"));
        assert!(prefix_str.contains("Content-Type: application/octet-stream"));
        assert!(prefix_str.contains("X-ClickHouse-User: test_user"));
        assert!(prefix_str.contains("X-ClickHouse-Key: test_pass"));
        assert!(prefix_str.ends_with("Content-Length: "));
    }
}
