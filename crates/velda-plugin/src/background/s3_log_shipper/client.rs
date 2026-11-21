//! Persistent Keep-Alive S3 `PUT` HTTP/1.1 Client with SigV4 Signing.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::digest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::config::OutboundTlsConfig;
use crate::error::PluginError;

use super::config::{S3AddressingStyle, S3Compression, S3LogShipperConfig};
use super::format::compress_object;
use super::sigv4::{SigV4Signer, unix_secs_to_utc_ymdhms, write_hex_lower};

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

/// Persistent keep-alive HTTP/1.1 `PUT` client for S3-compatible object storage.
pub struct S3HttpClient {
    host: String,
    port: u16,
    is_tls: bool,
    bucket: String,
    prefix: String,
    addressing_style: S3AddressingStyle,
    session_token: Option<String>,
    compression: S3Compression,
    signer: SigV4Signer,
    stream: Option<TransportStream>,
    tls_connector: Option<tokio_rustls::TlsConnector>,
    connect_timeout: Duration,
    request_timeout: Duration,
    head_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    resp_buf: Vec<u8>,
    seq: u64,
}

impl S3HttpClient {
    /// Creates a new S3 client with precomputed endpoint & SigV4 key material.
    pub fn new(config: &S3LogShipperConfig) -> Result<Self, PluginError> {
        let trimmed = config.endpoint.trim().trim_end_matches('/');
        let (is_tls, rest, default_port) = if let Some(r) = trimmed.strip_prefix("https://") {
            (true, r, 443u16)
        } else if let Some(r) = trimmed.strip_prefix("http://") {
            (false, r, 80u16)
        } else {
            return Err(PluginError::Config(format!(
                "Invalid S3 endpoint '{}'",
                config.endpoint
            )));
        };

        let host_port = rest.split('/').next().unwrap_or(rest);
        let (base_host, port) = if let Some(idx) = host_port.rfind(':') {
            let h = &host_port[..idx];
            let p = host_port[idx + 1..].parse::<u16>().map_err(|_| {
                PluginError::Config(format!("Invalid port in S3 endpoint '{}'", config.endpoint))
            })?;
            (h.trim_matches(['[', ']']).to_string(), p)
        } else {
            (host_port.trim_matches(['[', ']']).to_string(), default_port)
        };

        let connect_host = match config.addressing_style {
            S3AddressingStyle::Path => base_host.clone(),
            S3AddressingStyle::VirtualHosted => format!("{}.{}", config.bucket, base_host),
        };
        let host_header = if (is_tls && port == 443) || (!is_tls && port == 80) {
            connect_host.clone()
        } else {
            format!("{connect_host}:{port}")
        };

        let tls_connector = if is_tls {
            let tls_cfg = match &config.tls {
                Some(t) => t.build_client_config(&["http/1.1"])?,
                None => OutboundTlsConfig::default().build_client_config(&["http/1.1"])?,
            };
            Some(tokio_rustls::TlsConnector::from(tls_cfg))
        } else {
            None
        };

        let signer = SigV4Signer::new(
            &config.access_key_id,
            &config.secret_access_key,
            &config.region,
            &host_header,
        );

        Ok(Self {
            host: connect_host,
            port,
            is_tls,
            bucket: config.bucket.clone(),
            prefix: config.prefix.trim_matches('/').to_string(),
            addressing_style: config.addressing_style,
            session_token: config.session_token.clone(),
            compression: config.compression,
            signer,
            stream: None,
            tls_connector,
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            head_buf: Vec::with_capacity(1024),
            compress_buf: Vec::with_capacity(128 * 1024),
            resp_buf: vec![0u8; 2048],
            seq: 0,
        })
    }

    pub fn close_connection(&mut self) {
        self.stream = None;
    }

    async fn ensure_connected(&mut self) -> Result<(), PluginError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let tcp = tokio::time::timeout(
            self.connect_timeout,
            TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .map_err(|_| PluginError::Http("S3 TCP connect timed out".into()))?
        .map_err(|e| PluginError::Http(format!("S3 TCP connect error: {e}")))?;

        let _ = tcp.set_nodelay(true);
        if self.is_tls {
            let connector = self.tls_connector.as_ref().unwrap();
            let server_name =
                rustls::pki_types::ServerName::try_from(self.host.clone()).map_err(|_| {
                    PluginError::Http(format!("Invalid S3 TLS server name '{}'", self.host))
                })?;
            let tls =
                tokio::time::timeout(self.connect_timeout, connector.connect(server_name, tcp))
                    .await
                    .map_err(|_| PluginError::Http("S3 TLS handshake timed out".into()))?
                    .map_err(|e| PluginError::Http(format!("S3 TLS handshake error: {e}")))?;
            self.stream = Some(TransportStream::Tls(Box::new(tls)));
        } else {
            self.stream = Some(TransportStream::Plain(tcp));
        }
        Ok(())
    }

    /// Compresses `ndjson_payload`, signs with SigV4, and executes a persistent `PUT`.
    pub async fn put_object(&mut self, ndjson_payload: &[u8]) -> Result<u16, PluginError> {
        let now_dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let unix_secs = now_dur.as_secs();
        let (y, m, d, hh, _, _) = unix_secs_to_utc_ymdhms(unix_secs);
        self.seq = self.seq.wrapping_add(1);
        let seq = self.seq;

        let (wire_len, ext) = {
            let (wire_bytes, ext) =
                compress_object(ndjson_payload, self.compression, &mut self.compress_buf)
                    .map_err(|e| PluginError::Http(format!("S3 compression error: {e}")))?;
            (wire_bytes.len(), ext)
        };
        let use_compressed = !matches!(self.compression, S3Compression::None);

        let uri = match self.addressing_style {
            S3AddressingStyle::Path => {
                if self.prefix.is_empty() {
                    format!(
                        "/{}/year={y:04}/month={m:02}/day={d:02}/hour={hh:02}/velda-{unix_secs}-{seq}.{ext}",
                        self.bucket
                    )
                } else {
                    format!(
                        "/{}/{}/year={y:04}/month={m:02}/day={d:02}/hour={hh:02}/velda-{unix_secs}-{seq}.{ext}",
                        self.bucket, self.prefix
                    )
                }
            }
            S3AddressingStyle::VirtualHosted => {
                if self.prefix.is_empty() {
                    format!(
                        "/year={y:04}/month={m:02}/day={d:02}/hour={hh:02}/velda-{unix_secs}-{seq}.{ext}"
                    )
                } else {
                    format!(
                        "/{}/year={y:04}/month={m:02}/day={d:02}/hour={hh:02}/velda-{unix_secs}-{seq}.{ext}",
                        self.prefix
                    )
                }
            }
        };

        let mut payload_hex = [0u8; 64];
        {
            let wire_bytes = if use_compressed {
                self.compress_buf.as_slice()
            } else {
                ndjson_payload
            };
            let payload_digest = digest::digest(&digest::SHA256, wire_bytes);
            write_hex_lower(payload_digest.as_ref(), &mut payload_hex);
        }

        self.signer.write_signed_put_headers(
            &uri,
            wire_len,
            &payload_hex,
            unix_secs,
            self.session_token.as_deref(),
            &mut self.head_buf,
        );

        for attempt in 0..2 {
            self.ensure_connected().await?;
            let wire_bytes = if use_compressed {
                self.compress_buf.as_slice()
            } else {
                ndjson_payload
            };
            let res = tokio::time::timeout(self.request_timeout, async {
                let s = self.stream.as_mut().unwrap();
                s.write_all(&self.head_buf).await?;
                s.write_all(wire_bytes).await?;
                s.flush().await?;
                let n = s.read(&mut self.resp_buf).await?;
                Ok::<usize, std::io::Error>(n)
            })
            .await;

            match res {
                Ok(Ok(n)) if n > 0 => {
                    let status = parse_http_status(&self.resp_buf[..n]).unwrap_or(0);
                    if status >= 500 || status == 429 {
                        self.stream = None;
                    }
                    return Ok(status);
                }
                _ => {
                    self.stream = None;
                    if attempt == 0 {
                        continue;
                    }
                    return Err(PluginError::Http("S3 PUT write/read failed".to_string()));
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
