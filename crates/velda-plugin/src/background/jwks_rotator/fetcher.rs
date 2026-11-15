//! Non-blocking Background HTTP/HTTPS Client for JWKS Endpoint Fetching.
//!
//! ```text
//!                         JWKS BACKGROUND FETCHER
//!
//!     jwks_uri: "https://auth.example.com/.well-known/jwks.json"
//!                            |
//!                            v
//!     +-------------------------------------------------------+
//!     | URL Decomposer (Host, Port, Path, Scheme)             |
//!     +-------------------------------------------------------+
//!                            |
//!            +---------------+---------------+
//!            | (http://)                     | (https://)
//!            v                               v
//!     Plain TCP Stream               tokio-rustls TLS Handshake
//!     (TcpStream::connect)           (ClientConfig with Root Store)
//!            |                               |
//!            +---------------+---------------+
//!                            |
//!                            v
//!     +-------------------------------------------------------+
//!     | HTTP/1.1 Request Pipeline                             |
//!     | - GET {path} HTTP/1.1                                 |
//!     | - Host: {authority}                                   |
//!     | - Accept: application/json                            |
//!     | - Connection: close                                   |
//!     +-------------------------------------------------------+
//!                            |
//!                            v (httparse zero-alloc status & header parse)
//!     +-------------------------------------------------------+
//!     | Status 200 OK -> Return JSON Bytes in RAM             |
//!     +-------------------------------------------------------+
//! ```
//!
//! # Architectural Invariants
//! - **Strict Background Execution**: Driven entirely on Shadow Logistics runtimes (`SCHED_IDLE`).
//! - **Bounded Allocation**: Caps response body at 64 KB (fits dozens of keys) to prevent memory exhaustion attacks.
//! - **Zero Disk I/O**: Operates 100% in RAM buffers.

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::PluginError;

const MAX_JWKS_RESPONSE_BYTES: usize = 64 * 1024; // 64 KB cap

enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(buf).await,
            Self::Tls(s) => s.write_all(buf).await,
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

/// Standalone, non-blocking HTTP client for fetching JWKS keys.
pub struct JwksFetcher {
    tls_connector: Option<TlsConnector>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl JwksFetcher {
    /// Constructs a new JWKS fetcher with the specified request timeout.
    pub fn new(request_timeout_secs: u64) -> Self {
        let timeout = Duration::from_secs(request_timeout_secs);
        let connect_timeout = timeout.min(Duration::from_secs(5));

        let root_store = rustls::RootCertStore::empty();
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("valid protocol versions")
        .with_root_certificates(root_store)
        .with_no_client_auth();

        Self {
            tls_connector: Some(TlsConnector::from(Arc::new(tls_config))),
            connect_timeout,
            request_timeout: timeout,
        }
    }

    /// Fetches the JWKS JSON body from the specified URL over HTTP or HTTPS.
    pub async fn fetch_jwks_json(&self, url: &str) -> Result<Vec<u8>, PluginError> {
        let (is_tls, rest) = if let Some(stripped) = url.strip_prefix("https://") {
            (true, stripped)
        } else if let Some(stripped) = url.strip_prefix("http://") {
            (false, stripped)
        } else {
            return Err(PluginError::Config(format!("Invalid URL '{url}'")));
        };

        let (authority, path_and_query) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/"),
        };

        let (host, port) = match authority.rfind(':') {
            Some(idx) if !authority.ends_with(']') => {
                let h = authority[..idx].trim_matches(|c| c == '[' || c == ']');
                let p = authority[idx + 1..]
                    .parse::<u16>()
                    .map_err(|_| PluginError::Config(format!("Invalid port in URL '{url}'")))?;
                (h, p)
            }
            _ => (
                authority.trim_matches(|c| c == '[' || c == ']'),
                if is_tls { 443 } else { 80 },
            ),
        };

        let mut stream = self.connect_stream(is_tls, host, port).await?;

        // Format minimal HTTP/1.1 GET request
        let mut req_bytes = Vec::with_capacity(256);
        let _ = write!(
            req_bytes,
            "GET {path_and_query} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: velda-edge/jwks-rotator\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
        );

        tokio::time::timeout(self.request_timeout, stream.write_all(&req_bytes))
            .await
            .map_err(|_| PluginError::Config(format!("Write timeout to '{url}'")))?
            .map_err(PluginError::Io)?;

        stream.flush().await.map_err(PluginError::Io)?;

        // Read complete response up to buffer limit
        let mut resp_raw = Vec::with_capacity(8192);
        let mut buf = [0u8; 4096];
        loop {
            let n = tokio::time::timeout(self.request_timeout, stream.read(&mut buf))
                .await
                .map_err(|_| PluginError::Config(format!("Read timeout from '{url}'")))?
                .map_err(PluginError::Io)?;
            if n == 0 {
                break;
            }
            resp_raw.extend_from_slice(&buf[..n]);
            if resp_raw.len() > MAX_JWKS_RESPONSE_BYTES {
                return Err(PluginError::Config(format!(
                    "JWKS response exceeded maximum allowable size ({MAX_JWKS_RESPONSE_BYTES} bytes)"
                )));
            }
        }

        Self::extract_http_body(&resp_raw)
    }

    async fn connect_stream(
        &self,
        is_tls: bool,
        host: &str,
        port: u16,
    ) -> Result<ClientStream, PluginError> {
        let addr = format!("{host}:{port}");
        let tcp = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| PluginError::Config(format!("Connection timeout to '{addr}'")))?
            .map_err(PluginError::Io)?;

        if is_tls {
            let connector = self
                .tls_connector
                .as_ref()
                .ok_or_else(|| PluginError::Config("TLS connector is uninitialized".into()))?;
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| PluginError::Config(format!("Invalid DNS name '{host}': {e}")))?;

            let tls =
                tokio::time::timeout(self.connect_timeout, connector.connect(server_name, tcp))
                    .await
                    .map_err(|_| PluginError::Config(format!("TLS handshake timeout to '{addr}'")))?
                    .map_err(|e| PluginError::Tls(format!("TLS handshake failed: {e}")))?;

            Ok(ClientStream::Tls(Box::new(tls)))
        } else {
            Ok(ClientStream::Plain(tcp))
        }
    }

    fn extract_http_body(raw: &[u8]) -> Result<Vec<u8>, PluginError> {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut parsed = httparse::Response::new(&mut headers);

        let body_offset = match parsed.parse(raw) {
            Ok(httparse::Status::Complete(offset)) => offset,
            Ok(httparse::Status::Partial) => {
                return Err(PluginError::Config(
                    "Incomplete HTTP response from JWKS server".into(),
                ));
            }
            Err(e) => {
                return Err(PluginError::Config(format!(
                    "HTTP response parse error: {e}"
                )));
            }
        };

        let status = parsed.code.unwrap_or(0);
        if status != 200 {
            return Err(PluginError::Config(format!(
                "JWKS server returned non-200 status code: {status}"
            )));
        }

        Ok(raw[body_offset..].to_vec())
    }
}
