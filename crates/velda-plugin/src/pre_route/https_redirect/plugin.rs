//! `https_redirect` Pre-Route Plugin.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     HTTPS REDIRECT HOT-PATH EXECUTION                                   │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Inbound L7Request                                                                      │
//! │    │                                                                                    │
//! │    ├── x-forwarded-proto == "https" OR uri.scheme == "https" ──► Action::Continue       │
//! │    │                                                                                    │
//! │    └── Plaintext HTTP                                                                   │
//! │          ├── Extract Host / Authority (strip port, reject CRLF/control chars)           │
//! │          ├── Stack-format: "https://<host>[:port]<path_and_query>"                      │
//! │          └── Action::Respond(301 | 308, Location, Content-Length: 0)                    │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use http::header::{CONTENT_LENGTH, HOST, LOCATION};
use http::{HeaderName, HeaderValue, StatusCode};
use velda_core::{Action, Body, HeaderWorkspace, L7Request, L7Response, RejectReason};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;
use crate::pre_route::https_redirect::validation::validate_https_redirect_config;

const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

/// Stack buffer capacity for zero-heap-allocation `Location` header assembly.
const STACK_LOCATION_CAP: usize = 2048;

/// Pre-route plugin enforcing HTTPS upgrades at the edge boundary via 301/308 redirects.
#[derive(Debug, Clone)]
pub struct HttpsRedirectPlugin {
    id: String,
    status: StatusCode,
    port_suffix: Option<String>,
}

impl HttpsRedirectPlugin {
    /// Creates a new `HttpsRedirectPlugin` with explicit status code and target HTTPS port.
    pub fn new(
        id: impl Into<String>,
        status_code: u16,
        https_port: u16,
    ) -> Result<Self, PluginError> {
        let id = id.into();
        let status = match status_code {
            301 => StatusCode::MOVED_PERMANENTLY,
            308 => StatusCode::PERMANENT_REDIRECT,
            other => {
                return Err(PluginError::Config(format!(
                    "Plugin '{id}' (https_redirect) unsupported status_code {other}: expected 301 or 308"
                )));
            }
        };
        if https_port == 0 {
            return Err(PluginError::Config(format!(
                "Plugin '{id}' (https_redirect) invalid https_port 0"
            )));
        }

        let port_suffix = if https_port == 443 {
            None
        } else {
            Some(format!(":{https_port}"))
        };

        Ok(Self {
            id,
            status,
            port_suffix,
        })
    }

    /// Instantiates `HttpsRedirectPlugin` from a declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_https_redirect_config(&config.id, &config.config)?;
        Self::new(config.id.clone(), cfg.status_code, cfg.https_port)
    }
}

/// Strips an optional `:port` suffix from a Host/Authority string while preserving IPv6 brackets.
#[inline(always)]
fn strip_port(authority: &str) -> Option<&str> {
    let bytes = authority.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    if bytes[0] == b'[' {
        let close = bytes.iter().position(|&b| b == b']')?;
        let hostname = &authority[..=close];
        // Remainder after ']' must be empty or ':<digits>'
        let rest = &bytes[close + 1..];
        if !rest.is_empty() && rest[0] != b':' {
            return None;
        }
        Some(hostname)
    } else {
        let hostname = match bytes.iter().position(|&b| b == b':') {
            Some(idx) => &authority[..idx],
            None => authority,
        };
        if hostname.is_empty() {
            return None;
        }
        Some(hostname)
    }
}

#[inline(always)]
fn has_unsafe_header_bytes(s: &str) -> bool {
    s.bytes().any(|b| b <= 0x20 || b == 0x7f)
}

impl HttpsRedirectPlugin {
    /// Out-of-line redirect builder so the HTTPS fast path stays register-only with zero stack frame.
    #[cold]
    #[inline(never)]
    fn build_redirect_response(&self, req: &L7Request) -> Action {
        let raw_host = match req.uri.authority() {
            Some(auth) => auth.as_str(),
            None => match req.headers.get(&HOST).and_then(|v| v.to_str().ok()) {
                Some(h) => h,
                None => return Action::Reject(RejectReason::BadRequest),
            },
        };

        let Some(hostname) = strip_port(raw_host) else {
            return Action::Reject(RejectReason::BadRequest);
        };
        if has_unsafe_header_bytes(hostname) {
            return Action::Reject(RejectReason::BadRequest);
        }

        let pq = req.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
        if has_unsafe_header_bytes(pq) {
            return Action::Reject(RejectReason::BadRequest);
        }

        let port_str = self.port_suffix.as_deref().unwrap_or("");
        let total_len = 8 + hostname.len() + port_str.len() + pq.len();

        let location_val = if total_len <= STACK_LOCATION_CAP {
            let mut buf = [0u8; STACK_LOCATION_CAP];
            let mut pos = 0usize;
            buf[pos..pos + 8].copy_from_slice(b"https://");
            pos += 8;
            buf[pos..pos + hostname.len()].copy_from_slice(hostname.as_bytes());
            pos += hostname.len();
            if !port_str.is_empty() {
                buf[pos..pos + port_str.len()].copy_from_slice(port_str.as_bytes());
                pos += port_str.len();
            }
            buf[pos..pos + pq.len()].copy_from_slice(pq.as_bytes());
            pos += pq.len();

            match HeaderValue::from_bytes(&buf[..pos]) {
                Ok(v) => v,
                Err(_) => return Action::Reject(RejectReason::BadRequest),
            }
        } else {
            let mut loc = String::with_capacity(total_len);
            loc.push_str("https://");
            loc.push_str(hostname);
            loc.push_str(port_str);
            loc.push_str(pq);
            match HeaderValue::from_str(&loc) {
                Ok(v) => v,
                Err(_) => return Action::Reject(RejectReason::BadRequest),
            }
        };

        let mut headers = HeaderWorkspace::default();
        headers.insert(LOCATION, location_val);
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));

        Action::Respond(L7Response::new(
            self.status,
            req.version,
            headers,
            Body::Empty,
        ))
    }
}

impl PreRoutePlugin for HttpsRedirectPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline]
    fn on_request(&self, req: &mut L7Request) -> Action {
        // Fast path: already HTTPS -> zero stack frame, immediate Continue.
        if let Some(proto) = req.headers.get(&X_FORWARDED_PROTO)
            && proto.as_bytes().eq_ignore_ascii_case(b"https")
        {
            return Action::Continue;
        }
        if let Some(scheme) = req.uri.scheme_str()
            && scheme.eq_ignore_ascii_case("https")
        {
            return Action::Continue;
        }

        self.build_redirect_response(req)
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Uri, Version};
    use serde_json::json;

    fn make_req(
        uri: &'static str,
        host: Option<&'static str>,
        proto: Option<&'static str>,
    ) -> L7Request {
        let mut headers = HeaderWorkspace::default();
        if let Some(h) = host {
            headers.insert(HOST, HeaderValue::from_static(h));
        }
        if let Some(p) = proto {
            headers.insert(
                http::header::HeaderName::from_static("x-forwarded-proto"),
                HeaderValue::from_static(p),
            );
        }
        L7Request::new(
            Method::GET,
            Uri::from_static(uri),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_https_redirect_passes_when_already_https() {
        let plugin = HttpsRedirectPlugin::new("https-redir", 308, 443).unwrap();

        let mut req_hdr = make_req("/login?next=/home", Some("example.com"), Some("https"));
        assert!(matches!(plugin.on_request(&mut req_hdr), Action::Continue));

        let mut req_uri = make_req("https://example.com/api", None, None);
        assert!(matches!(plugin.on_request(&mut req_uri), Action::Continue));
    }

    #[test]
    fn test_https_redirect_upgrades_http_to_https_308_and_301() {
        let plugin_308 = HttpsRedirectPlugin::new("redir-308", 308, 443).unwrap();
        let mut req = make_req(
            "/v1/users?active=true",
            Some("api.example.com:80"),
            Some("http"),
        );
        match plugin_308.on_request(&mut req) {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::PERMANENT_REDIRECT);
                assert_eq!(
                    resp.headers.get(LOCATION).unwrap(),
                    "https://api.example.com/v1/users?active=true"
                );
                assert_eq!(resp.headers.get(CONTENT_LENGTH).unwrap(), "0");
            }
            other => panic!("Expected Action::Respond, got {other:?}"),
        }

        let plugin_301 = HttpsRedirectPlugin::new("redir-301", 301, 8443).unwrap();
        let mut req_v6 = make_req("/status", Some("[2001:db8::1]:8080"), Some("http"));
        match plugin_301.on_request(&mut req_v6) {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::MOVED_PERMANENTLY);
                assert_eq!(
                    resp.headers.get(LOCATION).unwrap(),
                    "https://[2001:db8::1]:8443/status"
                );
            }
            other => panic!("Expected Action::Respond, got {other:?}"),
        }
    }

    #[test]
    fn test_https_redirect_rejects_missing_host_and_invalid_config() {
        let plugin = HttpsRedirectPlugin::new("redir", 308, 443).unwrap();
        let mut req = make_req("/health", None, Some("http"));
        assert!(matches!(
            plugin.on_request(&mut req),
            Action::Reject(RejectReason::BadRequest)
        ));

        let bad_cfg = PluginConfig {
            id: "https_redirect".to_string(),
            config: json!({ "status_code": 302 }),
        };
        assert!(HttpsRedirectPlugin::from_config(&bad_cfg).is_err());
    }
}
