//! Persistent Keep-Alive Elasticsearch `_bulk` HTTP Client.
//!
//! Prebakes the entire HTTP request header prefix (path, `Host`, `Content-Type`,
//! `Content-Encoding`, `Authorization`, `Connection`, `Content-Length: `) at boot time.
//! On each flush, stamps only the decimal `Content-Length` + `\r\n\r\n`.

use std::io::Write;
use std::time::Duration;

use base64::Engine;
use flate2::Compression;
use flate2::write::GzEncoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use velda_core::lut::decimal::fast_write_u64;

use crate::config::OutboundTlsConfig;
use crate::error::PluginError;

use super::config::{ElasticsearchLogShipperConfig, EsAuthConfig, EsCompression};

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

struct PrebakedEndpoint {
    host: String,
    port: u16,
    is_tls: bool,
    prebaked_head_prefix: Vec<u8>,
}

/// Persistent keep-alive HTTP/1.1 client targeting Elasticsearch / OpenSearch `_bulk`.
pub struct ElasticsearchHttpClient {
    endpoints: Vec<PrebakedEndpoint>,
    active_idx: usize,
    stream: Option<TransportStream>,
    tls_connector: Option<tokio_rustls::TlsConnector>,
    compression: EsCompression,
    connect_timeout: Duration,
    request_timeout: Duration,
    head_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    resp_buf: Vec<u8>,
}

impl ElasticsearchHttpClient {
    /// Builds a new client with prebaked HTTP headers for all configured endpoints.
    pub fn new(config: &ElasticsearchLogShipperConfig) -> Result<Self, PluginError> {
        let auth_header = match &config.auth {
            EsAuthConfig::None => String::new(),
            EsAuthConfig::ApiKey { key } => format!("Authorization: ApiKey {key}\r\n"),
            EsAuthConfig::Basic { username, password } => {
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                format!("Authorization: Basic {encoded}\r\n")
            }
            EsAuthConfig::Bearer { token } => format!("Authorization: Bearer {token}\r\n"),
        };

        let path = match &config.pipeline {
            Some(p) => format!("/_bulk?pipeline={p}"),
            None => "/_bulk".to_string(),
        };

        let encoding_hdr = match config.compression {
            EsCompression::Gzip => "Content-Encoding: gzip\r\n",
            EsCompression::None => "",
        };

        let mut endpoints = Vec::with_capacity(config.endpoints.len());
        let mut needs_tls = false;

        for raw_ep in &config.endpoints {
            let trimmed = raw_ep.trim().trim_end_matches('/');
            let (is_tls, rest, default_port) = if let Some(r) = trimmed.strip_prefix("https://") {
                needs_tls = true;
                (true, r, 9200u16)
            } else if let Some(r) = trimmed.strip_prefix("http://") {
                (false, r, 9200u16)
            } else {
                return Err(PluginError::Config(format!(
                    "Invalid Elasticsearch endpoint '{raw_ep}'"
                )));
            };

            let host_port = rest.split('/').next().unwrap_or(rest);
            let (host, port) = if let Some(idx) = host_port.rfind(':') {
                let h = &host_port[..idx];
                let p = host_port[idx + 1..].parse::<u16>().map_err(|_| {
                    PluginError::Config(format!("Invalid port in ES endpoint '{raw_ep}'"))
                })?;
                (h.trim_matches(['[', ']']).to_string(), p)
            } else {
                (host_port.trim_matches(['[', ']']).to_string(), default_port)
            };

            let prebaked_head_prefix = format!(
                "POST {path} HTTP/1.1\r\n\
                 Host: {host_port}\r\n\
                 Content-Type: application/x-ndjson\r\n\
                 {encoding_hdr}\
                 {auth_header}\
                 Connection: keep-alive\r\n\
                 Content-Length: "
            )
            .into_bytes();

            endpoints.push(PrebakedEndpoint {
                host,
                port,
                is_tls,
                prebaked_head_prefix,
            });
        }

        let tls_connector = if needs_tls {
            let tls_cfg = match &config.tls {
                Some(t) => t.build_client_config(&["http/1.1"])?,
                None => OutboundTlsConfig::default().build_client_config(&["http/1.1"])?,
            };
            Some(tokio_rustls::TlsConnector::from(tls_cfg))
        } else {
            None
        };

        Ok(Self {
            endpoints,
            active_idx: 0,
            stream: None,
            tls_connector,
            compression: config.compression,
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            head_buf: Vec::with_capacity(512),
            compress_buf: Vec::with_capacity(64 * 1024),
            resp_buf: vec![0u8; 4096],
        })
    }

    /// Closes any open keep-alive socket.
    pub fn close_connection(&mut self) {
        self.stream = None;
    }

    async fn ensure_connected(&mut self) -> Result<(), PluginError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let total = self.endpoints.len();
        let mut last_err = None;
        for offset in 0..total {
            let idx = (self.active_idx + offset) % total;
            let ep = &self.endpoints[idx];
            match tokio::time::timeout(
                self.connect_timeout,
                TcpStream::connect((ep.host.as_str(), ep.port)),
            )
            .await
            {
                Ok(Ok(tcp)) => {
                    let _ = tcp.set_nodelay(true);
                    if ep.is_tls {
                        let connector = self
                            .tls_connector
                            .as_ref()
                            .expect("TLS connector initialized");
                        let server_name = rustls::pki_types::ServerName::try_from(ep.host.clone())
                            .map_err(|_| {
                                PluginError::Http(format!("Invalid TLS server name '{}'", ep.host))
                            })?;
                        let tls = tokio::time::timeout(
                            self.connect_timeout,
                            connector.connect(server_name, tcp),
                        )
                        .await
                        .map_err(|_| PluginError::Http("ES TLS handshake timed out".into()))?
                        .map_err(|e| PluginError::Http(format!("ES TLS error: {e}")))?;
                        self.stream = Some(TransportStream::Tls(Box::new(tls)));
                    } else {
                        self.stream = Some(TransportStream::Plain(tcp));
                    }
                    self.active_idx = idx;
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_err = Some(format!("{}:{}: {e}", ep.host, ep.port));
                }
                Err(_) => {
                    last_err = Some(format!("{}:{}: connect timeout", ep.host, ep.port));
                }
            }
        }

        Err(PluginError::Http(format!(
            "Failed to connect to any Elasticsearch endpoint (last: {})",
            last_err.unwrap_or_default()
        )))
    }

    /// Compresses (if enabled) and ships a `_bulk` NDJSON payload, returning `(status, has_bulk_errors)`.
    pub async fn push_bulk(&mut self, ndjson_payload: &[u8]) -> Result<(u16, bool), PluginError> {
        let use_compressed = match self.compression {
            EsCompression::Gzip => {
                self.compress_buf.clear();
                let mut encoder = GzEncoder::new(&mut self.compress_buf, Compression::fast());
                encoder
                    .write_all(ndjson_payload)
                    .map_err(|e| PluginError::Http(format!("ES gzip encode error: {e}")))?;
                encoder
                    .finish()
                    .map_err(|e| PluginError::Http(format!("ES gzip finish error: {e}")))?;
                true
            }
            EsCompression::None => false,
        };

        for attempt in 0..2 {
            self.ensure_connected().await?;
            let wire_payload: &[u8] = if use_compressed {
                &self.compress_buf
            } else {
                ndjson_payload
            };
            let ep = &self.endpoints[self.active_idx];

            self.head_buf.clear();
            self.head_buf.extend_from_slice(&ep.prebaked_head_prefix);
            fast_write_u64(&mut self.head_buf, wire_payload.len() as u64);
            self.head_buf.extend_from_slice(b"\r\n\r\n");

            let res = tokio::time::timeout(self.request_timeout, async {
                let s = self.stream.as_mut().unwrap();
                s.write_all(&self.head_buf).await?;
                s.write_all(wire_payload).await?;
                s.flush().await?;
                read_and_drain_http1_response(s, &mut self.resp_buf).await
            })
            .await;

            match res {
                Ok(Ok((status, has_errors, close_conn))) => {
                    if status >= 500 || status == 429 || close_conn {
                        self.stream = None;
                        if status >= 500 || status == 429 {
                            self.active_idx = (self.active_idx + 1) % self.endpoints.len();
                        }
                    }
                    return Ok((status, has_errors));
                }
                _ => {
                    self.stream = None;
                    if attempt == 0 {
                        self.active_idx = (self.active_idx + 1) % self.endpoints.len();
                        continue;
                    }
                    return Err(PluginError::Http(
                        "Elasticsearch _bulk write/read failed".to_string(),
                    ));
                }
            }
        }

        unreachable!()
    }
}

#[inline]
fn parse_http_status(resp: &[u8]) -> Option<u16> {
    let space = resp.iter().position(|&b| b == b' ')?;
    let code_bytes = resp.get(space + 1..space + 4)?;
    std::str::from_utf8(code_bytes).ok()?.parse::<u16>().ok()
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn parse_content_length_and_flags(headers: &[u8]) -> (Option<usize>, bool, bool) {
    let mut content_length = None;
    let mut is_chunked = false;
    let mut close_conn = false;
    if let Ok(text) = std::str::from_utf8(headers) {
        for line in text.split("\r\n").skip(1) {
            if let Some((name, val)) = line.split_once(':') {
                let name = name.trim();
                let val = val.trim();
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = val.parse::<usize>().ok();
                } else if name.eq_ignore_ascii_case("transfer-encoding")
                    && val.to_ascii_lowercase().contains("chunked")
                {
                    is_chunked = true;
                } else if name.eq_ignore_ascii_case("connection")
                    && val.eq_ignore_ascii_case("close")
                {
                    close_conn = true;
                }
            }
        }
    }
    (content_length, is_chunked, close_conn)
}

async fn read_and_drain_http1_response(
    s: &mut TransportStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<(u16, bool, bool)> {
    let mut filled = 0usize;
    let header_end = loop {
        if filled == buf.len() {
            if buf.len() >= 16 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "HTTP response headers exceeded 16 KiB",
                ));
            }
            buf.resize(buf.len() * 2, 0);
        }
        let n = s.read(&mut buf[filled..]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF while reading HTTP response headers",
            ));
        }
        filled += n;
        if let Some(end) = find_header_end(&buf[..filled]) {
            break end;
        }
    };

    let status = parse_http_status(&buf[..header_end]).unwrap_or(0);
    let (content_length, is_chunked, mut close_conn) =
        parse_content_length_and_flags(&buf[..header_end]);
    let mut has_errors = buf[header_end..filled]
        .windows(13)
        .any(|w| w == b"\"errors\":true");

    if let Some(cl) = content_length {
        let already_read = filled - header_end;
        if already_read < cl {
            let mut remaining = cl - already_read;
            let mut scratch = [0u8; 4096];
            let mut tail_carry = [0u8; 12];
            let mut carry_len = 0usize;
            if already_read > 0 {
                let slice = &buf[header_end..filled];
                let take = slice.len().min(12);
                tail_carry[..take].copy_from_slice(&slice[slice.len() - take..]);
                carry_len = take;
            }
            while remaining > 0 {
                let to_read = remaining.min(scratch.len());
                let n = s.read(&mut scratch[..to_read]).await?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "EOF while draining HTTP response body",
                    ));
                }
                if !has_errors {
                    let mut combined = [0u8; 4108];
                    combined[..carry_len].copy_from_slice(&tail_carry[..carry_len]);
                    combined[carry_len..carry_len + n].copy_from_slice(&scratch[..n]);
                    let total = carry_len + n;
                    if combined[..total]
                        .windows(13)
                        .any(|w| w == b"\"errors\":true")
                    {
                        has_errors = true;
                    }
                    let take = total.min(12);
                    tail_carry[..take].copy_from_slice(&combined[total - take..total]);
                    carry_len = take;
                }
                remaining -= n;
            }
        }
    } else if is_chunked {
        let mut tail = [0u8; 7];
        let mut tail_len = 0usize;
        let check_slice = &buf[header_end..filled];
        let mut done = check_slice.starts_with(b"0\r\n\r\n")
            || check_slice.windows(7).any(|w| w == b"\r\n0\r\n\r\n");
        if !done && !check_slice.is_empty() {
            let take = check_slice.len().min(7);
            tail[..take].copy_from_slice(&check_slice[check_slice.len() - take..]);
            tail_len = take;
        }
        let mut scratch = [0u8; 4096];
        while !done {
            let n = s.read(&mut scratch).await?;
            if n == 0 {
                close_conn = true;
                break;
            }
            let mut combined = [0u8; 4103];
            combined[..tail_len].copy_from_slice(&tail[..tail_len]);
            combined[tail_len..tail_len + n].copy_from_slice(&scratch[..n]);
            let total = tail_len + n;
            if combined[..total]
                .windows(13)
                .any(|w| w == b"\"errors\":true")
            {
                has_errors = true;
            }
            if combined[..total].windows(7).any(|w| w == b"\r\n0\r\n\r\n") {
                done = true;
            } else {
                let take = total.min(7);
                tail[..take].copy_from_slice(&combined[total - take..total]);
                tail_len = take;
            }
        }
    } else {
        // No Content-Length and not chunked: cannot safely reuse socket.
        close_conn = true;
    }

    Ok((status, has_errors, close_conn))
}
