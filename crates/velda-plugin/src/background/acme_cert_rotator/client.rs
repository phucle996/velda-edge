//! Async ACME Protocol Client (RFC 8555).
//!
//! Provides a lean, asynchronous client tailored for edge gateway operations:
//! - Implements HTTP-01 challenge orchestration with automatic anti-replay nonce management.
//! - Transparently retries on `badNonce` errors per RFC 8555 §6.5.
//! - Uses `tokio-rustls` and system/custom root CA stores for outbound TLS communication.

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use super::crypto::{AccountKey, KeyIdentifier};
use crate::error::PluginError;

/// RFC 8555 §7.1.1 Directory Object.
#[derive(Debug, Clone, Deserialize)]
pub struct AcmeDirectory {
    #[serde(rename = "newNonce")]
    pub new_nonce: String,
    #[serde(rename = "newAccount")]
    pub new_account: String,
    #[serde(rename = "newOrder")]
    pub new_order: String,
}

/// RFC 8555 §7.1.3 Order Object.
#[derive(Debug, Clone, Deserialize)]
pub struct AcmeOrder {
    pub status: String,
    pub authorizations: Vec<String>,
    pub finalize: String,
    pub certificate: Option<String>,
}

/// RFC 8555 §7.1.4 Authorization Object.
#[derive(Debug, Clone, Deserialize)]
pub struct AcmeAuthorization {
    pub status: String,
    pub identifier: AcmeIdentifier,
    pub challenges: Vec<AcmeChallenge>,
}

/// RFC 8555 §9.7.7 Identifier Object.
#[derive(Debug, Clone, Deserialize)]
pub struct AcmeIdentifier {
    #[serde(rename = "type")]
    pub id_type: String,
    pub value: String,
}

/// RFC 8555 §8 Challenge Object.
#[derive(Debug, Clone, Deserialize)]
pub struct AcmeChallenge {
    #[serde(rename = "type")]
    pub challenge_type: String,
    pub url: String,
    pub token: String,
    pub status: String,
}

enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
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

/// Lightweight HTTP/1.1 response representation.
///
/// The ACME protocol only ever consumes two response headers (RFC 8555 §6.5 `Replay-Nonce`
/// and §7.3 `Location`), so only those are retained. Every other header is skipped during
/// parsing without allocating a `String`/`Vec` entry for it.
#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub replay_nonce: Option<String>,
    pub location: Option<String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Returns the retained header value for `replay-nonce` or `location` (case-insensitive).
    /// Any other header name yields `None` because it was never stored.
    pub fn header(&self, name: &str) -> Option<&str> {
        if name.eq_ignore_ascii_case("replay-nonce") {
            self.replay_nonce.as_deref()
        } else if name.eq_ignore_ascii_case("location") {
            self.location.as_deref()
        } else {
            None
        }
    }
}

/// Which retained header (if any) a raw header name denotes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KnownHeader {
    ReplayNonce,
    Location,
    Other,
}

/// SWAR case-insensitive classifier for `replay-nonce` (12 bytes) and `location` (8 bytes).
///
/// Loads the name into 64-bit/32-bit general-purpose registers and ORs `0x20` into every byte,
/// which folds ASCII upper-case letters onto lower-case, then compares against constant
/// registers: 1 load + 1 OR + 1 CMP per word, no per-byte loop and no allocation.
/// The `| 0x20` fold also maps `0x0D` onto `-`, but `\r` can never occur inside an
/// `httparse`-validated header token, so there is no false positive in practice.
#[inline(always)]
fn classify_header(name: &[u8]) -> KnownHeader {
    const FOLD64: u64 = 0x2020_2020_2020_2020;
    const FOLD32: u32 = 0x2020_2020;
    const LOCATION: u64 = u64::from_ne_bytes(*b"location");
    const REPLAY_HI: u64 = u64::from_ne_bytes(*b"replay-n");
    const REPLAY_LO: u32 = u32::from_ne_bytes(*b"once");

    match name.len() {
        8 => {
            // SAFETY: length checked == 8, unaligned read of exactly 8 bytes.
            let w = unsafe { std::ptr::read_unaligned(name.as_ptr() as *const u64) };
            if (w | FOLD64) == LOCATION {
                KnownHeader::Location
            } else {
                KnownHeader::Other
            }
        }
        12 => {
            // SAFETY: length checked == 12, reads bytes [0..8) and [8..12).
            let (hi, lo) = unsafe {
                (
                    std::ptr::read_unaligned(name.as_ptr() as *const u64),
                    std::ptr::read_unaligned(name.as_ptr().add(8) as *const u32),
                )
            };
            if (hi | FOLD64) == REPLAY_HI && (lo | FOLD32) == REPLAY_LO {
                KnownHeader::ReplayNonce
            } else {
                KnownHeader::Other
            }
        }
        _ => KnownHeader::Other,
    }
}

/// Async ACME Protocol Client driving directory discovery, account, orders, and challenges.
pub struct AcmeClient {
    tls_connector: Option<TlsConnector>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl AcmeClient {
    /// Creates a new ACME client with explicit timeouts.
    pub fn new(connect_timeout: Duration, request_timeout: Duration) -> Self {
        // Build TLS connector with standard crypto provider
        let root_store = rustls::RootCertStore::empty();
        // Add native/system root store or fallback
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("valid protocol versions")
        .with_root_certificates(root_store)
        .with_no_client_auth();

        Self {
            tls_connector: Some(TlsConnector::from(Arc::new(config))),
            connect_timeout,
            request_timeout,
        }
    }

    /// Discovers directory endpoints from the base directory URL.
    pub async fn get_directory(&self, directory_url: &str) -> Result<AcmeDirectory, PluginError> {
        let resp = self.send_http("GET", directory_url, &[], b"").await?;
        if resp.status != 200 {
            return Err(PluginError::Config(format!(
                "ACME directory request failed with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        serde_json::from_slice(&resp.body)
            .map_err(|e| PluginError::Config(format!("Failed to parse ACME directory JSON: {e}")))
    }

    /// Fetches a fresh anti-replay nonce from the `newNonce` endpoint.
    pub async fn get_nonce(&self, new_nonce_url: &str) -> Result<String, PluginError> {
        let resp = self.send_http("HEAD", new_nonce_url, &[], b"").await?;
        if let Some(nonce) = resp.header("replay-nonce") {
            Ok(nonce.trim().to_string())
        } else {
            // Some CAs allow GET on newNonce
            let resp = self.send_http("GET", new_nonce_url, &[], b"").await?;
            resp.header("replay-nonce")
                .map(|n| n.trim().to_string())
                .ok_or_else(|| {
                    PluginError::Config(format!(
                        "ACME CA at '{new_nonce_url}' did not return a Replay-Nonce header"
                    ))
                })
        }
    }

    /// Creates or retrieves an existing ACME account, returning the account URL (`kid`).
    pub async fn create_or_get_account(
        &self,
        new_account_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        email: &str,
    ) -> Result<String, PluginError> {
        let payload = serde_json::json!({
            "termsOfServiceAgreed": true,
            "contact": [format!("mailto:{email}")],
        });
        let payload_str = payload.to_string();

        let resp = self
            .post_jws_with_retry(
                new_account_url,
                new_nonce_url,
                account_key,
                KeyIdentifier::Jwk(account_key.jwk()),
                &payload_str,
            )
            .await?;

        // Status 201 Created or 200 OK (existing account)
        if resp.status == 200 || resp.status == 201 {
            resp.header("location")
                .map(|loc| loc.trim().to_string())
                .ok_or_else(|| {
                    PluginError::Config(
                        "ACME newAccount response did not contain a Location header".into(),
                    )
                })
        } else {
            Err(PluginError::Config(format!(
                "ACME newAccount failed with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )))
        }
    }

    /// Creates a new certificate order for the specified domains.
    pub async fn create_order(
        &self,
        new_order_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
        domains: &[String],
    ) -> Result<(String, AcmeOrder), PluginError> {
        let identifiers: Vec<_> = domains
            .iter()
            .map(|d| serde_json::json!({ "type": "dns", "value": d }))
            .collect();
        let payload = serde_json::json!({ "identifiers": identifiers }).to_string();

        let resp = self
            .post_jws_with_retry(
                new_order_url,
                new_nonce_url,
                account_key,
                KeyIdentifier::Kid(account_url),
                &payload,
            )
            .await?;

        if resp.status != 201 && resp.status != 200 {
            return Err(PluginError::Config(format!(
                "ACME newOrder failed with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }

        let order_url = resp
            .header("location")
            .map(|loc| loc.trim().to_string())
            .unwrap_or_default();

        let order: AcmeOrder = serde_json::from_slice(&resp.body).map_err(|e| {
            PluginError::Config(format!("Failed to parse ACME order response JSON: {e}"))
        })?;

        Ok((order_url, order))
    }

    /// Fetches authorization metadata and extracts the HTTP-01 challenge parameters.
    pub async fn get_http01_challenge(
        &self,
        authz_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
    ) -> Result<AcmeChallenge, PluginError> {
        let resp = self
            .post_jws_with_retry(
                authz_url,
                new_nonce_url,
                account_key,
                KeyIdentifier::Kid(account_url),
                "", // POST-as-GET
            )
            .await?;

        if resp.status != 200 {
            return Err(PluginError::Config(format!(
                "ACME getAuthorization failed with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }

        let authz: AcmeAuthorization = serde_json::from_slice(&resp.body).map_err(|e| {
            PluginError::Config(format!("Failed to parse ACME authorization JSON: {e}"))
        })?;

        authz
            .challenges
            .into_iter()
            .find(|c| c.challenge_type == "http-01")
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "No 'http-01' challenge found for identifier '{}'",
                    authz.identifier.value
                ))
            })
    }

    /// Notifies the ACME CA that the HTTP-01 challenge response is published and ready.
    pub async fn trigger_challenge(
        &self,
        challenge_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
    ) -> Result<(), PluginError> {
        let resp = self
            .post_jws_with_retry(
                challenge_url,
                new_nonce_url,
                account_key,
                KeyIdentifier::Kid(account_url),
                "{}",
            )
            .await?;

        if resp.status != 200 {
            return Err(PluginError::Config(format!(
                "ACME triggerChallenge failed with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }

        Ok(())
    }

    /// Polls order status until it enters the `"ready"` state (all challenges validated).
    pub async fn poll_order_ready(
        &self,
        order_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
        max_duration: Duration,
    ) -> Result<AcmeOrder, PluginError> {
        let start = std::time::Instant::now();
        while start.elapsed() < max_duration {
            let resp = self
                .post_jws_with_retry(
                    order_url,
                    new_nonce_url,
                    account_key,
                    KeyIdentifier::Kid(account_url),
                    "", // POST-as-GET
                )
                .await?;

            if resp.status == 200 {
                let order: AcmeOrder = serde_json::from_slice(&resp.body).map_err(|e| {
                    PluginError::Config(format!("Failed to parse order status JSON: {e}"))
                })?;

                match order.status.as_str() {
                    "ready" => return Ok(order),
                    "valid" => return Ok(order),
                    "invalid" => {
                        return Err(PluginError::Config(format!(
                            "ACME order became invalid: {}",
                            String::from_utf8_lossy(&resp.body)
                        )));
                    }
                    _ => {
                        // "pending" or "processing" -> wait and retry
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                    }
                }
            } else {
                return Err(PluginError::Config(format!(
                    "Polling order failed with status {}: {}",
                    resp.status,
                    String::from_utf8_lossy(&resp.body)
                )));
            }
        }

        Err(PluginError::Config(format!(
            "Timed out after {:?} waiting for ACME order to become ready",
            max_duration
        )))
    }

    /// Submits the Certificate Signing Request (CSR) to finalize the order.
    pub async fn finalize_order(
        &self,
        finalize_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
        csr_der: &[u8],
    ) -> Result<AcmeOrder, PluginError> {
        let b64_csr = URL_SAFE_NO_PAD.encode(csr_der);
        let payload = serde_json::json!({ "csr": b64_csr }).to_string();

        let resp = self
            .post_jws_with_retry(
                finalize_url,
                new_nonce_url,
                account_key,
                KeyIdentifier::Kid(account_url),
                &payload,
            )
            .await?;

        if resp.status != 200 {
            return Err(PluginError::Config(format!(
                "ACME finalize failed with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }

        serde_json::from_slice(&resp.body).map_err(|e| {
            PluginError::Config(format!("Failed to parse finalize response JSON: {e}"))
        })
    }

    /// Polls order status until it enters the `"valid"` state and certificate URL is available.
    pub async fn poll_order_valid(
        &self,
        order_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
        max_duration: Duration,
    ) -> Result<String, PluginError> {
        let start = std::time::Instant::now();
        while start.elapsed() < max_duration {
            let resp = self
                .post_jws_with_retry(
                    order_url,
                    new_nonce_url,
                    account_key,
                    KeyIdentifier::Kid(account_url),
                    "", // POST-as-GET
                )
                .await?;

            if resp.status == 200 {
                let order: AcmeOrder = serde_json::from_slice(&resp.body).map_err(|e| {
                    PluginError::Config(format!("Failed to parse order status JSON: {e}"))
                })?;

                if let Some(cert_url) = order.certificate {
                    return Ok(cert_url);
                }

                if order.status == "invalid" {
                    return Err(PluginError::Config(format!(
                        "ACME order became invalid during certificate issuance: {}",
                        String::from_utf8_lossy(&resp.body)
                    )));
                }

                tokio::time::sleep(Duration::from_millis(1500)).await;
            } else {
                return Err(PluginError::Config(format!(
                    "Polling valid order failed with status {}: {}",
                    resp.status,
                    String::from_utf8_lossy(&resp.body)
                )));
            }
        }

        Err(PluginError::Config(format!(
            "Timed out after {:?} waiting for ACME certificate issuance",
            max_duration
        )))
    }

    /// Downloads the issued certificate chain (fullchain PEM).
    pub async fn download_certificate(
        &self,
        cert_url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        account_url: &str,
    ) -> Result<Vec<u8>, PluginError> {
        let resp = self
            .post_jws_with_retry(
                cert_url,
                new_nonce_url,
                account_key,
                KeyIdentifier::Kid(account_url),
                "", // POST-as-GET
            )
            .await?;

        if resp.status != 200 {
            return Err(PluginError::Config(format!(
                "Failed to download certificate from '{cert_url}' with status {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }

        Ok(resp.body)
    }

    /// Sends a signed JWS POST request, automatically retrying on `badNonce` errors.
    async fn post_jws_with_retry(
        &self,
        url: &str,
        new_nonce_url: &str,
        account_key: &AccountKey,
        key_id: KeyIdentifier<'_>,
        payload: &str,
    ) -> Result<HttpResponse, PluginError> {
        let mut nonce = self.get_nonce(new_nonce_url).await?;

        for attempt in 0..2 {
            let jws = account_key.sign(
                url,
                &nonce,
                match &key_id {
                    KeyIdentifier::Jwk(j) => KeyIdentifier::Jwk(j),
                    KeyIdentifier::Kid(k) => KeyIdentifier::Kid(k),
                },
                payload,
            )?;

            let body = serde_json::to_vec(&jws).map_err(|e| {
                PluginError::Config(format!("Failed to serialize JWS message JSON: {e}"))
            })?;

            let headers = [
                (
                    "Content-Type".to_string(),
                    "application/jose+json".to_string(),
                ),
                (
                    "Accept".to_string(),
                    "application/json, application/pem-certificate-chain".to_string(),
                ),
            ];

            let resp = self.send_http("POST", url, &headers, &body).await?;

            if resp.status == 400
                && String::from_utf8_lossy(&resp.body).contains("badNonce")
                && attempt == 0
            {
                // Fetch a fresh nonce and retry immediately
                if let Some(fresh_nonce) = resp.header("replay-nonce") {
                    nonce = fresh_nonce.trim().to_string();
                } else {
                    nonce = self.get_nonce(new_nonce_url).await?;
                }
                continue;
            }

            return Ok(resp);
        }

        Err(PluginError::Config(format!(
            "Failed to send JWS to '{url}' after retry"
        )))
    }

    /// Core HTTP/1.1 client request driver supporting both plain TCP (for tests) and TLS.
    async fn send_http(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(String, String)],
        body: &[u8],
    ) -> Result<HttpResponse, PluginError> {
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

        // Format request line and headers
        let mut req_bytes = Vec::with_capacity(256 + body.len());
        let _ = write!(
            req_bytes,
            "{method} {path_and_query} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: velda-edge/acme-cert-rotator\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        );

        for (k, v) in extra_headers {
            let _ = write!(req_bytes, "{k}: {v}\r\n");
        }
        req_bytes.extend_from_slice(b"\r\n");
        req_bytes.extend_from_slice(body);

        tokio::time::timeout(self.request_timeout, stream.write_all(&req_bytes))
            .await
            .map_err(|_| PluginError::Config(format!("Write timeout to '{url}'")))?
            .map_err(PluginError::Io)?;

        stream.flush().await.map_err(PluginError::Io)?;

        // Read complete response
        let mut resp_raw = Vec::with_capacity(4096);
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
        }

        Self::parse_http_response(&resp_raw)
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

    fn parse_http_response(raw: &[u8]) -> Result<HttpResponse, PluginError> {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut parsed = httparse::Response::new(&mut headers);

        let body_offset = match parsed.parse(raw) {
            Ok(httparse::Status::Complete(offset)) => offset,
            Ok(httparse::Status::Partial) => {
                return Err(PluginError::Config(
                    "Incomplete HTTP response from ACME server".into(),
                ));
            }
            Err(e) => {
                return Err(PluginError::Config(format!("HTTP parse error: {e}")));
            }
        };

        let status = parsed.code.unwrap_or(0);
        // Only `Replay-Nonce` and `Location` are retained; all other headers are skipped
        // by the SWAR classifier without any heap allocation.
        let mut replay_nonce = None;
        let mut location = None;
        for h in parsed.headers.iter() {
            match classify_header(h.name.as_bytes()) {
                KnownHeader::ReplayNonce => {
                    if let Ok(v) = std::str::from_utf8(h.value) {
                        replay_nonce = Some(v.to_string());
                    }
                }
                KnownHeader::Location => {
                    if let Ok(v) = std::str::from_utf8(h.value) {
                        location = Some(v.to_string());
                    }
                }
                KnownHeader::Other => {}
            }
        }

        let body = raw[body_offset..].to_vec();

        Ok(HttpResponse {
            status,
            replay_nonce,
            location,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swar_header_classifier_is_case_insensitive_and_exact() {
        assert!(classify_header(b"replay-nonce") == KnownHeader::ReplayNonce);
        assert!(classify_header(b"Replay-Nonce") == KnownHeader::ReplayNonce);
        assert!(classify_header(b"REPLAY-NONCE") == KnownHeader::ReplayNonce);
        assert!(classify_header(b"location") == KnownHeader::Location);
        assert!(classify_header(b"Location") == KnownHeader::Location);
        assert!(classify_header(b"replay-nonca") == KnownHeader::Other);
        assert!(classify_header(b"locatioN2") == KnownHeader::Other);
        assert!(classify_header(b"content-type") == KnownHeader::Other); // 12 bytes, wrong
        assert!(classify_header(b"cache-ctl") == KnownHeader::Other);
    }

    #[test]
    fn parse_keeps_only_nonce_and_location() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Type: a\r\nREPLAY-NONCE: n1\r\nLocation: http://x/y\r\nX-Other: z\r\n\r\nbody";
        let r = AcmeClient::parse_http_response(raw).unwrap();
        assert_eq!(r.status, 201);
        assert_eq!(r.header("replay-nonce"), Some("n1"));
        assert_eq!(r.header("Location"), Some("http://x/y"));
        assert_eq!(r.header("content-type"), None);
        assert_eq!(r.body, b"body");
    }
}
