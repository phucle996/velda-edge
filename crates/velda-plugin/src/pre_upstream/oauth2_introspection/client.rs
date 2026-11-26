//! Non-Blocking Async HTTP/HTTPS Keep-Alive RFC 7662 Client (`oauth2_introspection::client`).
//!
//! ```text
//!                      ASYNC RFC 7662 ID-P INTROSPECTION CLIENT
//!
//!   introspection_url (`http://` | `https://`, IPv4 / [IPv6] / Hostname)
//!         │
//!         ▼
//!   1. Endpoint Resolution (`velda-discovery`):
//!        • Explicit IP literal ──► SocketAddr directly (0 DNS calls)
//!        • Hostname            ──► DnsResolverProvider (DnsCache -> /etc/hosts -> UDP DNS + LKG)
//!         │
//!         ▼
//!   2. Acquire Async Keep-Alive Stream (`tokio::sync::Mutex` idle pool, `coarse_sec()` expiry):
//!        • Plain `http://`     ──► tokio::net::TcpStream
//!        • TLS   `https://`    ──► tokio::net::TcpStream + velda_tls::client::connect
//!         │
//!         ▼
//!   3. Async HTTP/1.1 POST `application/x-www-form-urlencoded` (Basic or Post client creds)
//!         │
//!         ▼
//!   4. Bounded Read (<= 64 KiB, Content-Length & Transfer-Encoding: chunked) -> IntrospectionResponse
//! ```

use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::prelude::*;
use http::Uri;
use rustc_hash::FxHashMap;
use rustls::ClientConfig;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::client::TlsStream;
use velda_discovery::{
    DnsResolverConfig, DnsResolverProvider, HostsFileSource, ResolvConfServerProvider,
    UdpDnsTransport,
};

use super::validation::Oauth2IntrospectionConfig;
use crate::error::PluginError;

/// Maximum allowed IdP response payload size (64 KiB) to prevent resource exhaustion attacks.
pub const MAX_IDP_RESPONSE_SIZE: usize = 64 * 1024;

/// RFC 7662 Introspection Response Payload.
#[derive(Debug, Clone, Deserialize)]
pub struct IntrospectionResponse {
    pub active: bool,
    #[serde(default)]
    pub exp: Option<serde_json::Value>,
    #[serde(default)]
    pub nbf: Option<serde_json::Value>,
    #[serde(default)]
    pub sub: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(flatten)]
    pub raw_claims: FxHashMap<String, serde_json::Value>,
}

impl IntrospectionResponse {
    /// Parses RFC 7519 `exp` NumericDate (integer or finite float seconds), rounding down.
    pub fn parsed_exp_epoch(&self) -> Result<Option<u64>, PluginError> {
        match self.exp.as_ref().filter(|v| !v.is_null()) {
            None => Ok(None),
            Some(v) => parse_numeric_date_floor(v).map(Some),
        }
    }

    /// Parses RFC 7519 `nbf` NumericDate (integer or finite float seconds), rounding up.
    pub fn parsed_nbf_epoch(&self) -> Result<Option<u64>, PluginError> {
        match self.nbf.as_ref().filter(|v| !v.is_null()) {
            None => Ok(None),
            Some(v) => parse_numeric_date_ceil(v).map(Some),
        }
    }

    /// Resolves a claim key or nested dot-separated path (e.g. `"sub"` or `"profile.tenant"`).
    pub fn resolve_claim_str(&self, claim_path: &str) -> Option<String> {
        if !claim_path.contains('.') {
            match claim_path {
                "sub" => return self.sub.clone(),
                "client_id" => return self.client_id.clone(),
                "scope" => return self.scope.clone(),
                _ => {
                    return self
                        .raw_claims
                        .get(claim_path)
                        .and_then(value_to_header_string);
                }
            }
        }
        let mut segs = claim_path.split('.');
        let first = segs.next()?;
        let mut cur = self.raw_claims.get(first)?;
        for seg in segs {
            cur = cur.as_object()?.get(seg)?;
        }
        value_to_header_string(cur)
    }

    /// Extracts flattened top-level claims into a string map.
    pub fn string_claims(&self) -> FxHashMap<String, String> {
        let mut map = FxHashMap::default();
        if let Some(ref sub) = self.sub {
            map.insert("sub".to_string(), sub.clone());
        }
        if let Some(ref cid) = self.client_id {
            map.insert("client_id".to_string(), cid.clone());
        }
        if let Some(ref scope) = self.scope {
            map.insert("scope".to_string(), scope.clone());
        }
        for (k, v) in &self.raw_claims {
            if matches!(
                k.as_str(),
                "active" | "exp" | "nbf" | "sub" | "client_id" | "scope"
            ) {
                continue;
            }
            if let Some(s) = value_to_header_string(v) {
                map.insert(k.clone(), s);
            }
        }
        map
    }
}

fn value_to_header_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(if *b { "true".into() } else { "false".into() }),
        serde_json::Value::Array(arr) => {
            let mut out = String::new();
            for item in arr {
                let piece = match item {
                    serde_json::Value::String(s) => s.as_str(),
                    _ => continue,
                };
                if !out.is_empty() {
                    out.push_str(", ");
                }
                out.push_str(piece);
            }
            if out.is_empty() { None } else { Some(out) }
        }
        _ => None,
    }
}

fn parse_numeric_date_floor(v: &serde_json::Value) -> Result<u64, PluginError> {
    let Some(n) = v.as_number() else {
        return Err(PluginError::Http(
            "Invalid non-numeric exp claim from IdP".into(),
        ));
    };
    if let Some(u) = n.as_u64() {
        return Ok(u);
    }
    if let Some(i) = n.as_i64() {
        return Ok(if i <= 0 { 0 } else { i as u64 });
    }
    if let Some(f) = n.as_f64()
        && f.is_finite()
    {
        if f <= 0.0 {
            return Ok(0);
        }
        if f >= u64::MAX as f64 {
            return Ok(u64::MAX);
        }
        return Ok(f.floor() as u64);
    }
    Err(PluginError::Http(
        "Invalid numeric exp claim from IdP".into(),
    ))
}

fn parse_numeric_date_ceil(v: &serde_json::Value) -> Result<u64, PluginError> {
    let Some(n) = v.as_number() else {
        return Err(PluginError::Http(
            "Invalid non-numeric nbf claim from IdP".into(),
        ));
    };
    if let Some(u) = n.as_u64() {
        return Ok(u);
    }
    if let Some(i) = n.as_i64() {
        return Ok(if i <= 0 { 0 } else { i as u64 });
    }
    if let Some(f) = n.as_f64()
        && f.is_finite()
    {
        if f <= 0.0 {
            return Ok(0);
        }
        if f >= u64::MAX as f64 {
            return Ok(u64::MAX);
        }
        return Ok(f.ceil() as u64);
    }
    Err(PluginError::Http(
        "Invalid numeric nbf claim from IdP".into(),
    ))
}

enum IdpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for IdpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for IdpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

struct PooledStream {
    stream: IdpStream,
    last_used_sec: u64,
}

enum EndpointResolver {
    Static(SocketAddr),
    Discovery {
        host: String,
        port: u16,
        resolver: Arc<DnsResolverProvider<ResolvConfServerProvider, UdpDnsTransport>>,
    },
}

/// Non-blocking async HTTP/HTTPS Keep-Alive RFC 7662 Token Introspection client.
pub struct IdpKeepAliveClient {
    endpoint: EndpointResolver,
    is_tls: bool,
    tls_host: String,
    tls_config: Option<Arc<ClientConfig>>,
    pub host_header: String,
    pub uri_path: String,
    basic_auth_header: Option<String>,
    client_id: String,
    client_secret: String,
    auth_method: String,
    token_type_hint: String,
    pool_max_idle: usize,
    keepalive_timeout_secs: u64,
    request_timeout: Duration,
    idle_pool: Mutex<Vec<PooledStream>>,
}

impl IdpKeepAliveClient {
    /// Constructs a new `IdpKeepAliveClient` backed by system `/etc/hosts` + `/etc/resolv.conf` `velda-discovery`.
    pub fn new(config: &Oauth2IntrospectionConfig) -> Result<Self, PluginError> {
        Self::new_with_hosts(config, HostsFileSource::load_system())
    }

    /// Constructs a new `IdpKeepAliveClient` with an explicit [`HostsFileSource`].
    pub fn new_with_hosts(
        config: &Oauth2IntrospectionConfig,
        hosts: HostsFileSource,
    ) -> Result<Self, PluginError> {
        let uri: Uri = config.introspection_url.trim().parse().map_err(|e| {
            PluginError::Config(format!(
                "Invalid introspection_url '{}': {e}",
                config.introspection_url
            ))
        })?;

        let scheme = uri.scheme_str().unwrap_or("");
        let is_tls = match scheme {
            "http" => false,
            "https" => true,
            other => {
                return Err(PluginError::Config(format!(
                    "Invalid URL scheme '{other}' in '{}'; expected http or https",
                    config.introspection_url
                )));
            }
        };

        let authority = uri.authority().ok_or_else(|| {
            PluginError::Config(format!(
                "Missing authority in introspection_url '{}'",
                config.introspection_url
            ))
        })?;
        let raw_host = authority.host();
        let clean_host = raw_host.trim_matches(|c| c == '[' || c == ']').to_string();
        let port = authority
            .port_u16()
            .unwrap_or(if is_tls { 443 } else { 80 });
        let host_header = authority.as_str().to_string();
        let uri_path = uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());

        let endpoint = if let Ok(ip) = clean_host.parse::<IpAddr>() {
            EndpointResolver::Static(SocketAddr::new(ip, port))
        } else {
            let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
                ResolvConfServerProvider::load_system(),
                hosts,
                UdpDnsTransport::new(),
                DnsResolverConfig::default(),
            ));
            EndpointResolver::Discovery {
                host: clean_host.clone(),
                port,
                resolver,
            }
        };

        let tls_config = if is_tls {
            Some(config.tls.build_client_config(&["http/1.1"])?)
        } else {
            None
        };

        let basic_auth_header = if config.auth_method == "client_secret_basic" {
            let cred = format!("{}:{}", config.client_id, config.client_secret);
            Some(format!("Basic {}", BASE64_STANDARD.encode(cred.as_bytes())))
        } else {
            None
        };

        Ok(Self {
            endpoint,
            is_tls,
            tls_host: clean_host,
            tls_config,
            host_header,
            uri_path,
            basic_auth_header,
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
            auth_method: config.auth_method.clone(),
            token_type_hint: config.token_type_hint.clone(),
            pool_max_idle: config.pool_max_idle_connections,
            keepalive_timeout_secs: config.keepalive_timeout_secs,
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            idle_pool: Mutex::new(Vec::with_capacity(config.pool_max_idle_connections)),
        })
    }

    /// Resolves target socket address asynchronously via `velda-discovery` (or static IP literal).
    async fn resolve_target(&self) -> Result<SocketAddr, PluginError> {
        match &self.endpoint {
            EndpointResolver::Static(addr) => Ok(*addr),
            EndpointResolver::Discovery {
                host,
                port,
                resolver,
            } => {
                let endpoints = resolver.resolve(host, *port).await.map_err(|e| {
                    PluginError::Http(format!("Discovery DNS failed for '{host}': {e}"))
                })?;
                let ep = endpoints.first().ok_or_else(|| {
                    PluginError::Http(format!("Discovery returned 0 endpoints for '{host}'"))
                })?;
                let port_host = u16::from_be(ep.port_be);
                let ip = if let Some(v4) = velda_core::lut::ip::is_ipv4_mapped_u128(ep.ip) {
                    IpAddr::V4(std::net::Ipv4Addr::from(v4.to_be_bytes()))
                } else {
                    IpAddr::V6(std::net::Ipv6Addr::from(ep.ip.to_be_bytes()))
                };
                Ok(SocketAddr::new(ip, port_host))
            }
        }
    }

    async fn connect_fresh(&self) -> Result<IdpStream, PluginError> {
        let addr = self.resolve_target().await?;
        let tcp = tokio::time::timeout(self.request_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| PluginError::Http(format!("IdP TCP connect timeout to {addr}")))?
            .map_err(|e| PluginError::Http(format!("IdP TCP connect error to {addr}: {e}")))?;

        let _ = tcp.set_nodelay(true);

        if self.is_tls {
            let cfg = self.tls_config.as_ref().ok_or_else(|| {
                PluginError::Tls("Missing TLS client config for HTTPS IdP".into())
            })?;
            let tls = tokio::time::timeout(
                self.request_timeout,
                velda_tls::client::connect(tcp, cfg, &self.tls_host),
            )
            .await
            .map_err(|_| PluginError::Http(format!("IdP TLS handshake timeout to {addr}")))?
            .map_err(|e| PluginError::Tls(format!("IdP TLS handshake error: {e}")))?;
            Ok(IdpStream::Tls(Box::new(tls)))
        } else {
            Ok(IdpStream::Plain(tcp))
        }
    }

    async fn acquire_stream(&self) -> Result<(IdpStream, bool), PluginError> {
        let now_sec = velda_core::clock::coarse_sec();
        {
            let mut pool = self.idle_pool.lock().await;
            while let Some(pooled) = pool.pop() {
                if now_sec.saturating_sub(pooled.last_used_sec) <= self.keepalive_timeout_secs {
                    return Ok((pooled.stream, true));
                }
            }
        }
        let stream = self.connect_fresh().await?;
        Ok((stream, false))
    }

    async fn release_stream(&self, stream: IdpStream) {
        let now_sec = velda_core::clock::coarse_sec();
        let mut pool = self.idle_pool.lock().await;
        if pool.len() < self.pool_max_idle {
            pool.push(PooledStream {
                stream,
                last_used_sec: now_sec,
            });
        }
    }

    fn format_request(&self, token: &str) -> Vec<u8> {
        let mut body = format!("token={}", url_encode(token));
        if self.token_type_hint != "none" {
            body.push_str("&token_type_hint=");
            body.push_str(&self.token_type_hint);
        }
        if self.auth_method == "client_secret_post" {
            body.push_str("&client_id=");
            body.push_str(&url_encode(&self.client_id));
            body.push_str("&client_secret=");
            body.push_str(&url_encode(&self.client_secret));
        }

        let mut req_str = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
            self.uri_path,
            self.host_header,
            body.len()
        );
        if let Some(ref auth) = self.basic_auth_header {
            req_str.push_str("Authorization: ");
            req_str.push_str(auth);
            req_str.push_str("\r\n");
        }
        req_str.push_str("\r\n");
        req_str.push_str(&body);
        req_str.into_bytes()
    }

    /// Executes an asynchronous RFC 7662 introspection request using the Keep-Alive pool.
    pub async fn introspect(&self, token: &str) -> Result<IntrospectionResponse, PluginError> {
        let req_bytes = self.format_request(token);
        let (mut stream, is_pooled) = self.acquire_stream().await?;

        match self.do_introspect(&mut stream, &req_bytes).await {
            Ok((resp, keep_alive)) => {
                if keep_alive {
                    self.release_stream(stream).await;
                }
                Ok(resp)
            }
            Err(_) if is_pooled => {
                let mut fresh = self.connect_fresh().await?;
                let (resp, keep_alive) = self.do_introspect(&mut fresh, &req_bytes).await?;
                if keep_alive {
                    self.release_stream(fresh).await;
                }
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    async fn do_introspect(
        &self,
        stream: &mut IdpStream,
        req_bytes: &[u8],
    ) -> Result<(IntrospectionResponse, bool), PluginError> {
        tokio::time::timeout(self.request_timeout, stream.write_all(req_bytes))
            .await
            .map_err(|_| PluginError::Http("IdP request write timeout".into()))?
            .map_err(|e| PluginError::Http(format!("IdP request write failed: {e}")))?;

        tokio::time::timeout(self.request_timeout, stream.flush())
            .await
            .map_err(|_| PluginError::Http("IdP request flush timeout".into()))?
            .map_err(|e| PluginError::Http(format!("IdP request flush failed: {e}")))?;

        let mut resp_buf = Vec::with_capacity(4096);
        let mut scratch = [0u8; 4096];
        let mut body_start = 0usize;
        let mut headers_parsed = false;
        let mut content_length: Option<usize> = None;
        let mut is_chunked = false;
        let mut connection_close = false;

        loop {
            let n = tokio::time::timeout(self.request_timeout, stream.read(&mut scratch))
                .await
                .map_err(|_| PluginError::Http("IdP response read timeout".into()))?
                .map_err(|e| PluginError::Http(format!("IdP response read failed: {e}")))?;

            if n == 0 {
                break;
            }
            resp_buf.extend_from_slice(&scratch[..n]);
            if resp_buf.len() > MAX_IDP_RESPONSE_SIZE {
                return Err(PluginError::Http(
                    "IdP response exceeded maximum 64 KiB limit".into(),
                ));
            }

            if !headers_parsed {
                let mut headers = [httparse::EMPTY_HEADER; 64];
                let mut parsed = httparse::Response::new(&mut headers);
                match parsed.parse(&resp_buf) {
                    Ok(httparse::Status::Complete(offset)) => {
                        headers_parsed = true;
                        body_start = offset;
                        if parsed.code != Some(200) {
                            return Err(PluginError::Http(format!(
                                "IdP returned HTTP status {:?}",
                                parsed.code
                            )));
                        }
                        for h in parsed.headers.iter() {
                            if h.name.eq_ignore_ascii_case("content-length") {
                                if let Ok(s) = std::str::from_utf8(h.value) {
                                    let len = s.trim().parse::<usize>().map_err(|_| {
                                        PluginError::Http(
                                            "Invalid IdP Content-Length header".into(),
                                        )
                                    })?;
                                    if len > MAX_IDP_RESPONSE_SIZE {
                                        return Err(PluginError::Http(
                                            "IdP Content-Length exceeds 64 KiB limit".into(),
                                        ));
                                    }
                                    content_length = Some(len);
                                }
                            } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
                                if let Ok(s) = std::str::from_utf8(h.value)
                                    && s.split(',')
                                        .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
                                {
                                    is_chunked = true;
                                }
                            } else if h.name.eq_ignore_ascii_case("connection")
                                && let Ok(s) = std::str::from_utf8(h.value)
                                && s.trim().eq_ignore_ascii_case("close")
                            {
                                connection_close = true;
                            }
                        }
                    }
                    Ok(httparse::Status::Partial) => continue,
                    Err(e) => {
                        return Err(PluginError::Http(format!(
                            "Failed to parse HTTP response from IdP: {e}"
                        )));
                    }
                }
            }

            if headers_parsed {
                let body_slice = &resp_buf[body_start..];
                if is_chunked {
                    if let Some(decoded) = try_decode_chunked_body(body_slice)? {
                        let resp_json: IntrospectionResponse = serde_json::from_slice(&decoded)
                            .map_err(|e| {
                                PluginError::Http(format!("Failed to parse chunked IdP JSON: {e}"))
                            })?;
                        return Ok((resp_json, !connection_close));
                    }
                } else if let Some(cl) = content_length
                    && body_slice.len() >= cl
                {
                    let resp_json: IntrospectionResponse =
                        serde_json::from_slice(&body_slice[..cl]).map_err(|e| {
                            PluginError::Http(format!("Failed to parse IdP JSON: {e}"))
                        })?;
                    return Ok((resp_json, !connection_close));
                }
            }
        }

        if !headers_parsed {
            return Err(PluginError::Http(
                "IdP closed connection before sending HTTP headers".into(),
            ));
        }

        let body_slice = &resp_buf[body_start..];
        let body_bytes = if is_chunked {
            try_decode_chunked_body(body_slice)?
                .ok_or_else(|| PluginError::Http("Truncated chunked HTTP body from IdP".into()))?
        } else if let Some(cl) = content_length {
            if body_slice.len() < cl {
                return Err(PluginError::Http(format!(
                    "Truncated IdP response body: expected {cl} bytes, got {}",
                    body_slice.len()
                )));
            }
            body_slice[..cl].to_vec()
        } else {
            body_slice.to_vec()
        };

        let resp_json: IntrospectionResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| PluginError::Http(format!("Failed to parse IdP JSON response: {e}")))?;
        Ok((resp_json, false))
    }
}

/// Attempts RFC 9112 chunked decoding, returning `Ok(None)` when additional TCP reads are needed.
fn try_decode_chunked_body(mut input: &[u8]) -> Result<Option<Vec<u8>>, PluginError> {
    let mut out = Vec::with_capacity(input.len());
    loop {
        let Some(crlf_pos) = input.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let size_line = std::str::from_utf8(&input[..crlf_pos])
            .map_err(|_| PluginError::Http("Invalid non-ASCII chunk size from IdP".into()))?;
        let hex_part = size_line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|_| {
            PluginError::Http(format!("Invalid hex chunk size '{hex_part}' from IdP"))
        })?;

        input = &input[crlf_pos + 2..];
        if chunk_size == 0 {
            if input.windows(2).any(|w| w == b"\r\n") || input.is_empty() {
                return Ok(Some(out));
            }
            return Ok(None);
        }

        if input.len() < chunk_size + 2 {
            return Ok(None);
        }
        if &input[chunk_size..chunk_size + 2] != b"\r\n" {
            return Err(PluginError::Http("Malformed chunked frame from IdP".into()));
        }

        out.extend_from_slice(&input[..chunk_size]);
        input = &input[chunk_size + 2..];
    }
}

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_decode_incremental_and_complete() {
        let partial = b"5\r\n{\"act";
        assert!(try_decode_chunked_body(partial).unwrap().is_none());

        let complete = b"7\r\n{\"activ\r\n8;foo=1\r\ne\":true}\r\n0\r\n\r\n";
        let decoded = try_decode_chunked_body(complete).unwrap().unwrap();
        assert_eq!(decoded, br#"{"active":true}"#);
    }

    #[test]
    fn test_numeric_date_float_and_nested_claims() {
        let resp: IntrospectionResponse = serde_json::from_str(
            r#"{"active":true,"exp":2500000000.9,"nbf":1600000000.2,"sub":"u1","profile":{"tenant":"acme"}}"#,
        )
        .unwrap();
        assert_eq!(resp.parsed_exp_epoch().unwrap(), Some(2500000000));
        assert_eq!(resp.parsed_nbf_epoch().unwrap(), Some(1600000001));
        assert_eq!(resp.resolve_claim_str("sub").as_deref(), Some("u1"));
        assert_eq!(
            resp.resolve_claim_str("profile.tenant").as_deref(),
            Some("acme")
        );
    }
}
