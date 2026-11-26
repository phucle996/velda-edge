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
//!                            v (httparse status + Content-Length / chunked decode)
//!     +-------------------------------------------------------+
//!     | Status 200 OK -> Return JSON Bytes in RAM             |
//!     +-------------------------------------------------------+
//! ```

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use rustls::ClientConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::config::OutboundTlsConfig;
use crate::error::PluginError;

const MAX_JWKS_RESPONSE_BYTES: usize = 64 * 1024; // 64 KiB cap

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

/// Standalone, non-blocking HTTP/1.1 client for fetching JWKS keys on Shadow Workers.
pub struct JwksFetcher {
    tls_config: Arc<ClientConfig>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl JwksFetcher {
    /// Constructs a new JWKS fetcher with the specified request timeout and outbound TLS configuration.
    pub fn new(request_timeout_secs: u64, tls: &OutboundTlsConfig) -> Result<Self, PluginError> {
        let timeout = Duration::from_secs(request_timeout_secs);
        let connect_timeout = timeout.min(Duration::from_secs(5));
        let client_config = tls.build_client_config(&["http/1.1"])?;

        Ok(Self {
            tls_config: client_config,
            connect_timeout,
            request_timeout: timeout,
        })
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

        let mut resp_raw = Vec::with_capacity(8192);
        let mut buf = [0u8; 4096];
        loop {
            let n = tokio::time::timeout(self.request_timeout, stream.read(&mut buf))
                .await
                .map_err(|_| PluginError::Config(format!("Read timeout from '{url}'")))?
                .map_err(PluginError::Io)?;
            if n == 0 {
                return Self::extract_http_body(&resp_raw);
            }
            resp_raw.extend_from_slice(&buf[..n]);
            if resp_raw.len() > MAX_JWKS_RESPONSE_BYTES {
                return Err(PluginError::Config(format!(
                    "JWKS response exceeded maximum allowable size ({MAX_JWKS_RESPONSE_BYTES} bytes)"
                )));
            }
            // Return immediately once Content-Length or chunked EOF (`0\r\n\r\n`) is complete
            // without blocking on TLS close_notify / TCP FIN from Keep-Alive servers.
            if let Some(body) = Self::try_extract_http_body_complete(&resp_raw)? {
                return Ok(body);
            }
        }
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
            let tls = tokio::time::timeout(
                self.connect_timeout,
                velda_tls::client::connect(tcp, &self.tls_config, host),
            )
            .await
            .map_err(|_| PluginError::Config(format!("TLS handshake timeout to '{addr}'")))?
            .map_err(|e| PluginError::Tls(format!("TLS handshake failed: {e}")))?;

            Ok(ClientStream::Tls(Box::new(tls)))
        } else {
            Ok(ClientStream::Plain(tcp))
        }
    }

    fn try_extract_http_body_complete(raw: &[u8]) -> Result<Option<Vec<u8>>, PluginError> {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut parsed = httparse::Response::new(&mut headers);

        let body_offset = match parsed.parse(raw) {
            Ok(httparse::Status::Complete(offset)) => offset,
            Ok(httparse::Status::Partial) => return Ok(None),
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

        let mut is_chunked = false;
        let mut content_length: Option<usize> = None;
        for hdr in parsed.headers.iter() {
            if hdr.name.eq_ignore_ascii_case("transfer-encoding") {
                if let Ok(val) = std::str::from_utf8(hdr.value)
                    && val
                        .split(',')
                        .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
                {
                    is_chunked = true;
                }
            } else if hdr.name.eq_ignore_ascii_case("content-length")
                && let Ok(val) = std::str::from_utf8(hdr.value)
                && let Ok(len) = val.trim().parse::<usize>()
            {
                content_length = Some(len);
            }
        }

        let wire_body = &raw[body_offset..];
        if is_chunked {
            return try_decode_chunked_body(wire_body);
        }

        if let Some(len) = content_length {
            if wire_body.len() >= len {
                return Ok(Some(wire_body[..len].to_vec()));
            }
            return Ok(None);
        }

        Ok(None)
    }

    fn extract_http_body(raw: &[u8]) -> Result<Vec<u8>, PluginError> {
        if let Some(complete) = Self::try_extract_http_body_complete(raw)? {
            return Ok(complete);
        }
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
        let is_chunked = parsed.headers.iter().any(|hdr| {
            hdr.name.eq_ignore_ascii_case("transfer-encoding")
                && std::str::from_utf8(hdr.value).is_ok_and(|v| {
                    v.split(',')
                        .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
                })
        });
        let wire_body = &raw[body_offset..];
        if is_chunked {
            return try_decode_chunked_body(wire_body)?.ok_or_else(|| {
                PluginError::Config("Truncated chunked JWKS HTTP body before EOF".into())
            });
        }
        for hdr in parsed.headers.iter() {
            if hdr.name.eq_ignore_ascii_case("content-length")
                && let Ok(val) = std::str::from_utf8(hdr.value)
                && let Ok(len) = val.trim().parse::<usize>()
            {
                return Err(PluginError::Config(format!(
                    "Truncated JWKS HTTP body: expected {len} bytes, got {} bytes",
                    wire_body.len()
                )));
            }
        }
        Ok(wire_body.to_vec())
    }
}

/// Incrementally decodes RFC 9112 §7.1 HTTP/1.1 `Transfer-Encoding: chunked` frames.
/// Returns `Ok(None)` when more network bytes are needed, or `Ok(Some(payload))` on `0\r\n\r\n`.
fn try_decode_chunked_body(mut input: &[u8]) -> Result<Option<Vec<u8>>, PluginError> {
    let mut out = Vec::with_capacity(input.len());
    loop {
        let Some(crlf_pos) = input.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let size_line = std::str::from_utf8(&input[..crlf_pos]).map_err(|_| {
            PluginError::Config("Malformed chunked JWKS response: non-ASCII chunk size".into())
        })?;
        // Strip optional chunk extensions (`;ext=val`)
        let hex_part = size_line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|_| {
            PluginError::Config(format!(
                "Malformed chunked JWKS response: invalid hex chunk size '{hex_part}'"
            ))
        })?;

        input = &input[crlf_pos + 2..];
        if chunk_size == 0 {
            if input.starts_with(b"\r\n") {
                return Ok(Some(out));
            }
            if let Some(trailer_end) = input.windows(4).position(|w| w == b"\r\n\r\n") {
                let _ = trailer_end;
                return Ok(Some(out));
            }
            return Ok(None);
        }

        if input.len() < chunk_size + 2 {
            return Ok(None);
        }
        if &input[chunk_size..chunk_size + 2] != b"\r\n" {
            return Err(PluginError::Config(
                "Malformed chunked JWKS response: missing CRLF after chunk payload".into(),
            ));
        }

        out.extend_from_slice(&input[..chunk_size]);
        input = &input[chunk_size + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_http_body_decodes_chunked_transfer_encoding() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n9\r\n{\"keys\":[\r\n3;ext=1\r\n]}\n\r\n0\r\n\r\n";
        let decoded = JwksFetcher::extract_http_body(raw).unwrap();
        assert_eq!(decoded, b"{\"keys\":[]}\n");
    }
}
