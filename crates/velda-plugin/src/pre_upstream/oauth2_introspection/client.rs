//! Persistent HTTP Keep-Alive Introspection Client (`oauth2_introspection::client`).
//!
//! Maintains a persistent connection pool with kernel `SO_KEEPALIVE` and HTTP/1.1
//! `Connection: keep-alive` reuse to Authorization Servers (RFC 7662).
//! Eliminates repeated TCP 3-way handshakes and TLS overhead.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::prelude::*;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use velda_sync::Oauth2IntrospectionConfig;

use crate::error::PluginError;

/// Maximum allowed IdP response payload size (64KB) to prevent resource exhaustion attacks.
pub const MAX_IDP_RESPONSE_SIZE: usize = 64 * 1024;

/// RFC 7662 Introspection Response Payload.
#[derive(Debug, Clone, Deserialize)]
pub struct IntrospectionResponse {
    pub active: bool,
    pub exp: Option<u64>,
    pub sub: Option<String>,
    pub client_id: Option<String>,
    pub scope: Option<String>,
    #[serde(flatten)]
    pub raw_claims: FxHashMap<String, serde_json::Value>,
}

impl IntrospectionResponse {
    /// Extracts flattened string-valued claims for upstream header injection.
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
            if k == "active" || k == "exp" || k == "sub" || k == "client_id" || k == "scope" {
                continue;
            }
            if let Some(s) = v.as_str() {
                map.insert(k.clone(), s.to_string());
            } else if let Some(n) = v.as_i64() {
                map.insert(k.clone(), n.to_string());
            } else if let Some(b) = v.as_bool() {
                map.insert(k.clone(), b.to_string());
            }
        }
        map
    }
}

struct PooledStream {
    stream: TcpStream,
    last_used: Instant,
}

/// Persistent HTTP Keep-Alive client communicating with an RFC 7662 Introspection endpoint.
pub struct IdpKeepAliveClient {
    pub target_addr: SocketAddr,
    pub host_header: String,
    pub uri_path: String,
    basic_auth_header: Option<String>,
    client_id: String,
    client_secret: String,
    auth_method: String,
    token_type_hint: String,
    pool_max_idle: usize,
    keepalive_timeout: Duration,
    request_timeout: Duration,
    idle_pool: Mutex<Vec<PooledStream>>,
}

impl IdpKeepAliveClient {
    /// Constructs a new `IdpKeepAliveClient` with parsed endpoint targets.
    pub fn new(config: &Oauth2IntrospectionConfig) -> Result<Self, PluginError> {
        let introspection_url = &config.introspection_url;
        let client_id = &config.client_id;
        let client_secret = &config.client_secret;
        let auth_method = &config.auth_method;
        let token_type_hint = &config.token_type_hint;
        let keepalive_timeout_secs = config.keepalive_timeout_secs;
        let pool_max_idle = config.pool_max_idle_connections;
        let request_timeout_ms = config.request_timeout_ms;

        // Parse URL scheme, host, port, and path
        let stripped = introspection_url
            .strip_prefix("http://")
            .or_else(|| introspection_url.strip_prefix("https://"))
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Invalid URL scheme in '{introspection_url}'; must start with http:// or https://"
                ))
            })?;

        let (host_port_str, uri_path) = match stripped.split_once('/') {
            Some((hp, path)) => (hp, format!("/{path}")),
            None => (stripped, "/".to_string()),
        };

        let (host_name, port) = match host_port_str.split_once(':') {
            Some((h, p)) => {
                let parsed_p = p.parse::<u16>().map_err(|e| {
                    PluginError::Config(format!("Invalid port in '{host_port_str}': {e}"))
                })?;
                (h, parsed_p)
            }
            None => (host_port_str, 80),
        };

        let target_addr = format!("{host_name}:{port}")
            .to_socket_addrs()
            .map_err(|e| {
                PluginError::Config(format!(
                    "Failed to resolve IdP address '{host_name}:{port}': {e}"
                ))
            })?
            .next()
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "DNS resolution returned no addresses for '{host_name}'"
                ))
            })?;

        let basic_auth_header = if auth_method == "client_secret_basic" {
            let cred = format!("{client_id}:{client_secret}");
            let encoded = BASE64_STANDARD.encode(cred.as_bytes());
            Some(format!("Basic {encoded}"))
        } else {
            None
        };

        Ok(Self {
            target_addr,
            host_header: host_port_str.to_string(),
            uri_path,
            basic_auth_header,
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            auth_method: auth_method.to_string(),
            token_type_hint: token_type_hint.to_string(),
            pool_max_idle,
            keepalive_timeout: Duration::from_secs(keepalive_timeout_secs),
            request_timeout: Duration::from_millis(request_timeout_ms),
            idle_pool: Mutex::new(Vec::with_capacity(pool_max_idle)),
        })
    }

    /// Establishes a brand-new TCP stream configured with timeouts and nodelay.
    fn connect_fresh(&self) -> Result<TcpStream, PluginError> {
        let stream =
            TcpStream::connect_timeout(&self.target_addr, self.request_timeout).map_err(|e| {
                PluginError::Http(format!(
                    "Failed to connect to IdP at {}: {e}",
                    self.target_addr
                ))
            })?;

        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(self.request_timeout));
        let _ = stream.set_write_timeout(Some(self.request_timeout));
        Ok(stream)
    }

    /// Acquires an active Keep-Alive TCP stream from the idle pool or establishes a fresh one.
    /// Returns `(stream, is_pooled)`.
    fn acquire_stream(&self) -> Result<(TcpStream, bool), PluginError> {
        let now = Instant::now();
        if let Ok(mut pool) = self.idle_pool.lock() {
            while let Some(pooled) = pool.pop() {
                if now.duration_since(pooled.last_used) <= self.keepalive_timeout {
                    return Ok((pooled.stream, true));
                }
            }
        }

        let stream = self.connect_fresh()?;
        Ok((stream, false))
    }

    /// Releases a healthy Keep-Alive stream back into the idle pool.
    fn release_stream(&self, stream: TcpStream) {
        if let Ok(mut pool) = self.idle_pool.lock()
            && pool.len() < self.pool_max_idle
        {
            pool.push(PooledStream {
                stream,
                last_used: Instant::now(),
            });
        }
    }

    /// Builds the wire HTTP request body and headers for RFC 7662 introspection.
    fn format_request(&self, token: &str) -> Vec<u8> {
        let mut body = format!("token={}", url_encode(token));
        if self.token_type_hint != "none" {
            body.push_str(&format!("&token_type_hint={}", self.token_type_hint));
        }
        if self.auth_method == "client_secret_post" {
            body.push_str(&format!(
                "&client_id={}&client_secret={}",
                url_encode(&self.client_id),
                url_encode(&self.client_secret)
            ));
        }

        let mut req_str = format!(
            "POST {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Type: application/x-www-form-urlencoded\r\n\
             Content-Length: {}\r\n\
             Connection: keep-alive\r\n",
            self.uri_path,
            self.host_header,
            body.len()
        );

        if let Some(ref auth) = self.basic_auth_header {
            req_str.push_str(&format!("Authorization: {auth}\r\n"));
        }
        req_str.push_str("\r\n");
        req_str.push_str(&body);

        req_str.into_bytes()
    }

    /// Executes an RFC 7662 Token Introspection call over a persistent Keep-Alive connection.
    /// Transparently retries once with a fresh socket if a pooled connection had been closed by IdP.
    pub fn introspect(&self, token: &str) -> Result<IntrospectionResponse, PluginError> {
        let req_bytes = self.format_request(token);
        let (mut stream, is_pooled) = self.acquire_stream()?;

        match self.do_introspect(&mut stream, &req_bytes) {
            Ok((resp, keep_alive)) => {
                if keep_alive {
                    self.release_stream(stream);
                }
                Ok(resp)
            }
            Err(_e) if is_pooled => {
                // Pooled connection likely timed out or was closed by server; retry once with a fresh socket
                let mut fresh_stream = self.connect_fresh()?;
                let (resp, keep_alive) = self.do_introspect(&mut fresh_stream, &req_bytes)?;
                if keep_alive {
                    self.release_stream(fresh_stream);
                }
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    /// Performs the HTTP write/read/parse cycle on a single TCP stream.
    fn do_introspect(
        &self,
        stream: &mut TcpStream,
        req_bytes: &[u8],
    ) -> Result<(IntrospectionResponse, bool), PluginError> {
        stream
            .write_all(req_bytes)
            .map_err(|e| PluginError::Http(format!("Failed to write to IdP stream: {e}")))?;

        let mut resp_buf = vec![0u8; 4096];
        let mut total_read = 0;
        let mut headers_parsed = false;
        let mut body_start = 0;
        let mut content_length: Option<usize> = None;
        let mut connection_close = false;

        loop {
            let n = match stream.read(&mut resp_buf[total_read..]) {
                Ok(0) => break,
                Ok(bytes) => bytes,
                Err(e) => {
                    return Err(PluginError::Http(format!(
                        "Failed to read response from IdP: {e}"
                    )));
                }
            };
            total_read += n;

            if !headers_parsed {
                let mut headers = [httparse::EMPTY_HEADER; 64];
                let mut resp = httparse::Response::new(&mut headers);
                match resp.parse(&resp_buf[..total_read]) {
                    Ok(httparse::Status::Complete(offset)) => {
                        headers_parsed = true;
                        body_start = offset;

                        if resp.code != Some(200) {
                            return Err(PluginError::Http(format!(
                                "IdP returned unexpected HTTP status code: {:?}",
                                resp.code
                            )));
                        }

                        for h in resp.headers {
                            if h.name.eq_ignore_ascii_case("content-length") {
                                if let Ok(s) = std::str::from_utf8(h.value) {
                                    let cl = s.trim().parse::<usize>().ok();
                                    if let Some(len) = cl
                                        && len > MAX_IDP_RESPONSE_SIZE
                                    {
                                        return Err(PluginError::Http(
                                            "IdP Content-Length exceeds maximum 64KB limit".into(),
                                        ));
                                    }
                                    content_length = cl;
                                }
                            } else if h.name.eq_ignore_ascii_case("connection")
                                && let Ok(s) = std::str::from_utf8(h.value)
                                && s.trim().eq_ignore_ascii_case("close")
                            {
                                connection_close = true;
                            }
                        }
                    }
                    Ok(httparse::Status::Partial) => {
                        if total_read > MAX_IDP_RESPONSE_SIZE {
                            return Err(PluginError::Http(
                                "IdP headers exceed maximum 64KB limit".into(),
                            ));
                        }
                    }
                    Err(e) => {
                        return Err(PluginError::Http(format!(
                            "Failed to parse HTTP response from IdP: {e}"
                        )));
                    }
                }
            }

            if headers_parsed
                && let Some(cl) = content_length
                && total_read >= body_start + cl
            {
                break;
            }

            if total_read > MAX_IDP_RESPONSE_SIZE {
                return Err(PluginError::Http(
                    "IdP response body exceeds maximum 64KB limit".into(),
                ));
            }

            if total_read == resp_buf.len() {
                resp_buf.resize(resp_buf.len() * 2, 0);
            }
        }

        if !headers_parsed {
            return Err(PluginError::Http(
                "IdP closed connection before sending complete HTTP headers".into(),
            ));
        }

        let body_end = if let Some(cl) = content_length {
            (body_start + cl).min(total_read)
        } else {
            total_read
        };

        let body_bytes = &resp_buf[body_start..body_end];
        let resp_json: IntrospectionResponse = serde_json::from_slice(body_bytes)
            .map_err(|e| PluginError::Http(format!("Failed to parse IdP JSON response: {e}")))?;

        Ok((resp_json, !connection_close))
    }
}

/// Percent-encodes characters for application/x-www-form-urlencoded payloads.
fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
