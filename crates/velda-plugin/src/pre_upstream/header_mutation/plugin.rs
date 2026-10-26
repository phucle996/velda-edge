//! Header Mutation Pre-Upstream Plugin (`header_mutation`).
//!
//! Enforces edge hygiene and request transformation before upstream backend forwarding:
//! - Strips RFC 9110 / RFC 7230 hop-by-hop headers while defending against CVE-2022-26143 abuse.
//! - Removes sensitive, untrusted, or unneeded client headers.
//! - Renames headers in-place on the stack with anti-shadowing protection.
//! - Evaluates and interpolates dynamic runtime variables (`$client_ip`, `$request_id`, etc.).
//! - Appends values to existing headers (e.g. multi-value `X-Forwarded-For`).

use std::sync::Arc;

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, HeaderWorkspace, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

// Pre-parsed static hop-by-hop header names to avoid dynamic string parsing on the hot path.
const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");
const PROXY_CONNECTION: HeaderName = HeaderName::from_static("proxy-connection");

// ── Dynamic Variables & Value Specifications ─────────────────────────

/// Canonical dynamic runtime variables interpolatable into upstream request headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicVar {
    /// Client IP address (authoritatively resolved from edge `x-real-ip` first, falling back to nearest proxy).
    ClientIp,
    /// Monotonic W3C request identifier (zero-allocation reference from Slot 0 of `HeaderWorkspace`).
    RequestId,
    /// Downstream request scheme (`"http"` or `"https"`).
    Scheme,
    /// Host authority header or URI authority.
    Host,
    /// HTTP request method (`"GET"`, `"POST"`, etc.).
    Method,
    /// Request URI path.
    Path,
}

/// Header value specification: pre-compiled static value or dynamic request variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderValueSpec {
    /// Static header value pre-computed at configuration compile time.
    Static(HeaderValue),
    /// Dynamic variable resolved per request on the hot path.
    Variable(DynamicVar),
}

impl HeaderValueSpec {
    /// Parses a header value specification from string.
    /// Supports `$$` to escape a literal `$`.
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        if let Some(rest) = s.strip_prefix('$') {
            if rest == "$" {
                return Ok(Self::Static(HeaderValue::from_static("$")));
            }
            let var = match rest {
                "client_ip" => DynamicVar::ClientIp,
                "request_id" => DynamicVar::RequestId,
                "scheme" => DynamicVar::Scheme,
                "host" => DynamicVar::Host,
                "method" => DynamicVar::Method,
                "path" => DynamicVar::Path,
                _ => {
                    return Err(PluginError::Config(format!(
                        "Unknown dynamic variable '{s}'; supported variables are: $client_ip, $request_id, $scheme, $host, $method, $path (use '$$' to escape literal '$')"
                    )));
                }
            };
            Ok(Self::Variable(var))
        } else {
            let val = HeaderValue::from_str(s).map_err(|e| {
                PluginError::Config(format!("Invalid header value string '{s}': {e}"))
            })?;
            Ok(Self::Static(val))
        }
    }

    /// Resolves the header value against the incoming request and authoritative target upstream.
    #[inline]
    pub fn resolve(&self, req: &L7Request, _target_upstream: &str) -> Option<HeaderValue> {
        match self {
            Self::Static(val) => Some(val.clone()),
            Self::Variable(var) => match var {
                DynamicVar::ClientIp => {
                    // Security Invariant: Prefer edge-authoritative x-real-ip (injected from physical socket peer)
                    // over untrusted client-supplied x-forwarded-for to eliminate IP spoofing attacks.
                    if let Some(real_ip) = req.headers.get("x-real-ip") {
                        Some(real_ip.clone())
                    } else if let Some(xff) = req
                        .headers
                        .get("x-forwarded-for")
                        .and_then(|v| v.to_str().ok())
                    {
                        // Fall back to nearest proxy (right-most IP)
                        let nearest = xff
                            .rsplit(',')
                            .next()
                            .map(|s| s.trim())
                            .unwrap_or("127.0.0.1");
                        HeaderValue::from_str(nearest).ok()
                    } else {
                        Some(HeaderValue::from_static("127.0.0.1"))
                    }
                }
                DynamicVar::RequestId => {
                    // Always O(1) zero-allocation from Slot 0 authoritative W3C ID
                    Some(req.headers.request_id().clone())
                }
                DynamicVar::Scheme => {
                    let is_https = req.uri.scheme_str() == Some("https")
                        || req
                            .headers
                            .get("x-forwarded-proto")
                            .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"https"));
                    if is_https {
                        Some(HeaderValue::from_static("https"))
                    } else {
                        Some(HeaderValue::from_static("http"))
                    }
                }
                DynamicVar::Host => req
                    .headers
                    .get(http::header::HOST)
                    .cloned()
                    .or_else(|| req.uri.host().and_then(|h| HeaderValue::from_str(h).ok())),
                DynamicVar::Method => match req.method {
                    http::Method::GET => Some(HeaderValue::from_static("GET")),
                    http::Method::POST => Some(HeaderValue::from_static("POST")),
                    http::Method::PUT => Some(HeaderValue::from_static("PUT")),
                    http::Method::DELETE => Some(HeaderValue::from_static("DELETE")),
                    http::Method::HEAD => Some(HeaderValue::from_static("HEAD")),
                    http::Method::OPTIONS => Some(HeaderValue::from_static("OPTIONS")),
                    http::Method::PATCH => Some(HeaderValue::from_static("PATCH")),
                    ref m => HeaderValue::from_str(m.as_str()).ok(),
                },
                DynamicVar::Path => HeaderValue::from_str(req.uri.path()).ok(),
            },
        }
    }
}

// ── Hop-by-Hop Stripping & Anti-Abuse Protection ─────────────────────

/// Checks whether a header name is a protected edge/protocol header that must NEVER
/// be stripped via client-supplied `Connection` tokens (defends against CVE-2022-26143 / Hop-by-Hop Abuse).
/// Uses zero-allocation length-bucket dispatch for hot-path efficiency.
#[inline]
pub fn is_protected_from_connection_strip(name: &HeaderName) -> bool {
    let s = name.as_str();
    if s.starts_with(':') {
        return true;
    }
    match s.len() {
        4 => s == "host",
        6 => s == "cookie",
        9 => s == "x-real-ip",
        10 => s == "set-cookie",
        12 => s == "content-type" || s == "x-request-id",
        13 => s == "authorization",
        14 => s == "content-length",
        15 => s == "x-forwarded-for",
        16 => s == "x-forwarded-host" || s == "x-forwarded-port",
        17 => s == "x-forwarded-proto",
        _ => false,
    }
}

/// Strips RFC 9110 / RFC 7230 hop-by-hop headers from a mutable headers map.
/// Scans ALL Connection header lines (supports RFC 9110 §5.3 header splitting / multi-line headers).
#[inline]
pub fn strip_hop_by_hop_headers(headers: &mut HeaderWorkspace) {
    // 1. Scan and extract tokens across ALL Connection header lines
    if headers.contains_key(http::header::CONNECTION) {
        let mut custom_tokens = [const { None }; 16];
        let mut count = 0;
        for val in headers.get_all(http::header::CONNECTION) {
            if let Ok(raw) = val.to_str() {
                for token in raw.split(',') {
                    let trimmed = token.trim();
                    if !trimmed.is_empty()
                        && !trimmed.eq_ignore_ascii_case("close")
                        && !trimmed.eq_ignore_ascii_case("keep-alive")
                        && let Ok(name) = HeaderName::from_bytes(trimmed.as_bytes())
                        && !is_protected_from_connection_strip(&name)
                        && count < custom_tokens.len()
                    {
                        custom_tokens[count] = Some(name);
                        count += 1;
                    }
                }
            }
        }
        headers.remove(http::header::CONNECTION);
        for name in custom_tokens.into_iter().flatten() {
            headers.remove(name);
        }
    }

    // 2. Standard RFC Hop-by-Hop headers
    headers.remove(&KEEP_ALIVE);
    headers.remove(&PROXY_CONNECTION);
    headers.remove(http::header::PROXY_AUTHENTICATE);
    headers.remove(http::header::PROXY_AUTHORIZATION);
    headers.remove(http::header::TE);
    headers.remove(http::header::TRAILER);
    headers.remove(http::header::TRANSFER_ENCODING);
    headers.remove(http::header::UPGRADE);
}

// ── Plugin Definition & Implementation ───────────────────────────────

/// Configuration settings for the header mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderMutationConfig {
    /// Strips RFC 9110 / RFC 7230 hop-by-hop headers before upstream forwarding.
    pub strip_hop_by_hop: bool,
    /// Headers to be explicitly stripped before upstream dispatch.
    pub remove: Vec<HeaderName>,
    /// Headers to be renamed from `(source_name, target_name)`.
    pub rename: Vec<(HeaderName, HeaderName)>,
    /// Headers to be set or overwritten.
    pub set: Vec<(HeaderName, HeaderValueSpec)>,
    /// Headers to be appended to existing values.
    pub append: Vec<(HeaderName, HeaderValueSpec)>,
}

impl Default for HeaderMutationConfig {
    fn default() -> Self {
        Self {
            strip_hop_by_hop: true,
            remove: Vec::new(),
            rename: Vec::new(),
            set: Vec::new(),
            append: Vec::new(),
        }
    }
}

/// Pre-Upstream Header Mutation Plugin.
#[derive(Debug, Clone)]
pub struct HeaderMutationPlugin {
    pub id: String,
    pub config: HeaderMutationConfig,
    static_sets: Vec<(HeaderName, HeaderValue)>,
    dynamic_sets: Vec<(HeaderName, DynamicVar)>,
    static_appends: Vec<(HeaderName, HeaderValue)>,
    dynamic_appends: Vec<(HeaderName, DynamicVar)>,
}

impl HeaderMutationPlugin {
    pub fn new(id: impl Into<String>, config: HeaderMutationConfig) -> Self {
        let mut static_sets = Vec::new();
        let mut dynamic_sets = Vec::new();
        for (name, spec) in &config.set {
            match spec {
                HeaderValueSpec::Static(val) => static_sets.push((name.clone(), val.clone())),
                HeaderValueSpec::Variable(var) => dynamic_sets.push((name.clone(), *var)),
            }
        }

        let mut static_appends = Vec::new();
        let mut dynamic_appends = Vec::new();
        for (name, spec) in &config.append {
            match spec {
                HeaderValueSpec::Static(val) => static_appends.push((name.clone(), val.clone())),
                HeaderValueSpec::Variable(var) => dynamic_appends.push((name.clone(), *var)),
            }
        }

        Self {
            id: id.into(),
            config,
            static_sets,
            dynamic_sets,
            static_appends,
            dynamic_appends,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_header_mutation_config(&config.id, &config.config).map_err(
            |e| {
                PluginError::Config(format!(
                    "Invalid header_mutation configuration for '{}': {e}",
                    config.id
                ))
            },
        )?;

        let mut remove = Vec::with_capacity(cfg.remove_headers.len());
        for s in &cfg.remove_headers {
            let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid remove header name '{s}': {e}"))
            })?;
            remove.push(name);
        }

        let mut set = Vec::with_capacity(cfg.set_headers.len());
        for (k, v) in &cfg.set_headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid set header name '{k}': {e}")))?;
            let spec = HeaderValueSpec::parse(v)?;
            set.push((name, spec));
        }

        let mut append = Vec::with_capacity(cfg.append_headers.len());
        for (k, v) in &cfg.append_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid append header name '{k}': {e}"))
            })?;
            let spec = HeaderValueSpec::parse(v)?;
            append.push((name, spec));
        }

        let mut rename = Vec::with_capacity(cfg.rename_headers.len());
        for (from_k, to_v) in &cfg.rename_headers {
            let from_name = HeaderName::from_bytes(from_k.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid rename source header '{from_k}': {e}"))
            })?;
            let to_name = HeaderName::from_bytes(to_v.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid rename target header '{to_v}': {e}"))
            })?;
            rename.push((from_name, to_name));
        }

        Ok(Self::new(
            &config.id,
            HeaderMutationConfig {
                strip_hop_by_hop: cfg.strip_hop_by_hop,
                remove,
                rename,
                set,
                append,
            },
        ))
    }
}

#[inline]
fn resolve_dynamic_var_cached(
    var: DynamicVar,
    req: &L7Request,
    cached_client_ip: &mut Option<HeaderValue>,
    cached_scheme: &mut Option<HeaderValue>,
) -> Option<HeaderValue> {
    match var {
        DynamicVar::ClientIp => {
            if let Some(ip) = cached_client_ip.as_ref() {
                return Some(ip.clone());
            }
            let ip = if let Some(real_ip) = req.headers.get("x-real-ip") {
                real_ip.clone()
            } else if let Some(xff) = req
                .headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
            {
                let nearest = xff
                    .rsplit(',')
                    .next()
                    .map(|s| s.trim())
                    .unwrap_or("127.0.0.1");
                HeaderValue::from_str(nearest)
                    .unwrap_or_else(|_| HeaderValue::from_static("127.0.0.1"))
            } else {
                HeaderValue::from_static("127.0.0.1")
            };
            *cached_client_ip = Some(ip.clone());
            Some(ip)
        }
        DynamicVar::Scheme => {
            if let Some(s) = cached_scheme.as_ref() {
                return Some(s.clone());
            }
            let is_https = req.uri.scheme_str() == Some("https")
                || req
                    .headers
                    .get("x-forwarded-proto")
                    .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"https"));
            let val = if is_https {
                HeaderValue::from_static("https")
            } else {
                HeaderValue::from_static("http")
            };
            *cached_scheme = Some(val.clone());
            Some(val)
        }
        DynamicVar::RequestId => Some(req.headers.request_id().clone()),
        DynamicVar::Host => req
            .headers
            .get(http::header::HOST)
            .cloned()
            .or_else(|| req.uri.host().and_then(|h| HeaderValue::from_str(h).ok())),
        DynamicVar::Method => match req.method {
            http::Method::GET => Some(HeaderValue::from_static("GET")),
            http::Method::POST => Some(HeaderValue::from_static("POST")),
            http::Method::PUT => Some(HeaderValue::from_static("PUT")),
            http::Method::DELETE => Some(HeaderValue::from_static("DELETE")),
            http::Method::HEAD => Some(HeaderValue::from_static("HEAD")),
            http::Method::OPTIONS => Some(HeaderValue::from_static("OPTIONS")),
            http::Method::PATCH => Some(HeaderValue::from_static("PATCH")),
            ref m => HeaderValue::from_str(m.as_str()).ok(),
        },
        DynamicVar::Path => HeaderValue::from_str(req.uri.path()).ok(),
    }
}

impl PreUpstreamPlugin for HeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        // Step 1: Strip RFC Hop-by-hop headers if configured
        if self.config.strip_hop_by_hop {
            strip_hop_by_hop_headers(&mut req.headers);
        }

        // Step 2: Remove blacklisted / unneeded headers
        for name in &self.config.remove {
            req.headers.remove(name);
        }

        // Step 3: Rename headers
        // Single-value fast path avoids get_all count traversal when there is at most 1 value (99.99% of requests)
        for (from_name, to_name) in &self.config.rename {
            if from_name != to_name {
                let mut vals = req.headers.get_all(from_name).iter();
                if let Some(first) = vals.next() {
                    if let Some(second) = vals.next() {
                        let mut stack_vals = [const { None }; 8];
                        stack_vals[0] = Some(first.clone());
                        stack_vals[1] = Some(second.clone());
                        let mut count = 2;
                        for v in vals {
                            if count < stack_vals.len() {
                                stack_vals[count] = Some(v.clone());
                                count += 1;
                            }
                        }
                        req.headers.remove(from_name);
                        req.headers.remove(to_name);
                        for item in stack_vals.into_iter().flatten() {
                            req.headers.append(to_name.clone(), item);
                        }
                    } else {
                        let val = first.clone();
                        req.headers.remove(from_name);
                        req.headers.remove(to_name);
                        req.headers.append(to_name.clone(), val);
                    }
                }
            }
        }

        // Step 4: Set / overwrite static headers (100% direct, 0 dynamic branches)
        for (name, val) in &self.static_sets {
            req.headers.insert(name.clone(), val.clone());
        }

        // Step 5: Append static headers
        for (name, val) in &self.static_appends {
            req.headers.append(name.clone(), val.clone());
        }

        // Step 6: Dynamic mutations with stack-cached context (only if dynamic vars configured)
        if !self.dynamic_sets.is_empty() || !self.dynamic_appends.is_empty() {
            let mut cached_client_ip: Option<HeaderValue> = None;
            let mut cached_scheme: Option<HeaderValue> = None;

            for (name, var) in &self.dynamic_sets {
                if let Some(val) =
                    resolve_dynamic_var_cached(*var, req, &mut cached_client_ip, &mut cached_scheme)
                {
                    req.headers.insert(name.clone(), val);
                }
            }

            for (name, var) in &self.dynamic_appends {
                if let Some(val) =
                    resolve_dynamic_var_cached(*var, req, &mut cached_client_ip, &mut cached_scheme)
                {
                    req.headers.append(name.clone(), val);
                }
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP | SupportedProtocols::GRPC
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_strip_hop_by_hop_and_cve_protection() {
        let mut headers = HeaderWorkspace::default();
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive, Host, Content-Length, X-Real-IP, X-Custom-Strip"),
        );
        headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("api.example.com"),
        );
        headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from_static("123"),
        );
        headers.insert(
            HeaderName::from_static("x-real-ip"),
            HeaderValue::from_static("203.0.113.1"),
        );
        headers.insert(
            HeaderName::from_static("x-custom-strip"),
            HeaderValue::from_static("should-be-removed"),
        );

        strip_hop_by_hop_headers(&mut headers);

        // Standard hop-by-hop headers are removed
        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key(http::header::UPGRADE));
        assert!(!headers.contains_key("x-custom-strip"));

        // Protected headers are preserved against CVE-2022-26143 connection strip attack
        assert_eq!(headers.get(http::header::HOST).unwrap(), "api.example.com");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "123");
        assert_eq!(headers.get("x-real-ip").unwrap(), "203.0.113.1");
    }

    #[test]
    fn test_multiline_connection_hop_by_hop_stripping() {
        let mut headers = HeaderWorkspace::new(16);
        // Simulates RFC 9110 §5.3 multi-line Connection headers
        headers.append(
            http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        headers.append(
            http::header::CONNECTION,
            HeaderValue::from_static("x-secret-leak, x-secondary-hop"),
        );
        headers.insert(
            HeaderName::from_static("x-secret-leak"),
            HeaderValue::from_static("super-confidential"),
        );
        headers.insert(
            HeaderName::from_static("x-secondary-hop"),
            HeaderValue::from_static("must-be-stripped"),
        );
        headers.insert(
            HeaderName::from_static("x-keep-this"),
            HeaderValue::from_static("safe-data"),
        );

        strip_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key("x-secret-leak"));
        assert!(!headers.contains_key("x-secondary-hop"));
        assert_eq!(headers.get("x-keep-this").unwrap(), "safe-data");
    }

    #[test]
    fn test_rename_with_anti_shadowing() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: vec![(
                HeaderName::from_static("x-legacy-token"),
                HeaderName::from_static("authorization"),
            )],
            set: Vec::new(),
            append: Vec::new(),
        };
        let plugin = HeaderMutationPlugin::new("hm-rename", config);

        let mut headers = HeaderMap::new();
        // Client attempted to pre-inject malicious target header
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("malicious-injected-token"),
        );
        headers.insert(
            HeaderName::from_static("x-legacy-token"),
            HeaderValue::from_static("Bearer abc123xyz"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        plugin.on_upstream_request(&mut req, &mut target);

        assert!(!req.headers.contains_key("x-legacy-token"));
        // Anti-shadowing: destination header was wiped clean before inserting valid renamed value
        let auth_vals: Vec<_> = req.headers.get_all("authorization").iter().collect();
        assert_eq!(auth_vals.len(), 1);
        assert_eq!(auth_vals[0], "Bearer abc123xyz");
    }

    #[test]
    fn test_dynamic_variables_integration() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: Vec::new(),
            set: vec![
                (
                    HeaderName::from_static("x-client-ip"),
                    HeaderValueSpec::parse("$client_ip").unwrap(),
                ),
                (
                    HeaderName::from_static("x-req-id"),
                    HeaderValueSpec::parse("$request_id").unwrap(),
                ),
                (
                    HeaderName::from_static("x-method"),
                    HeaderValueSpec::parse("$method").unwrap(),
                ),
                (
                    HeaderName::from_static("x-path"),
                    HeaderValueSpec::parse("$path").unwrap(),
                ),
                (
                    HeaderName::from_static("x-req-scheme"),
                    HeaderValueSpec::parse("$scheme").unwrap(),
                ),
            ],
            append: vec![(
                HeaderName::from_static("x-forwarded-for"),
                HeaderValueSpec::parse("$client_ip").unwrap(),
            )],
        };
        let plugin = HeaderMutationPlugin::new("hm-dynamic", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-real-ip"),
            HeaderValue::from_static("198.51.100.25"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("192.0.2.1"),
        );

        let mut req = L7Request::new(
            Method::PUT,
            Uri::from_static("https://example.com/v1/resources/99"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("billing-backend-cluster");
        plugin.on_upstream_request(&mut req, &mut target);

        // Edge x-real-ip takes precedence over untrusted xff prefix
        assert_eq!(req.headers.get("x-client-ip").unwrap(), "198.51.100.25");
        assert_eq!(req.headers.get("x-method").unwrap(), "PUT");
        assert_eq!(req.headers.get("x-path").unwrap(), "/v1/resources/99");
        assert_eq!(req.headers.get("x-req-scheme").unwrap(), "https");
        assert!(req.headers.contains_key("x-req-id"));

        let xff: Vec<_> = req.headers.get_all("x-forwarded-for").iter().collect();
        assert_eq!(xff.len(), 2);
        assert_eq!(xff[0], "192.0.2.1");
        assert_eq!(xff[1], "198.51.100.25");
    }

    #[test]
    fn test_set_append_and_remove() {
        let set = vec![(
            HeaderName::from_static("x-custom-gateway"),
            HeaderValueSpec::Static(HeaderValue::from_static("velda")),
        )];
        let remove = vec![HeaderName::from_static("x-sensitive")];
        let config = HeaderMutationConfig {
            strip_hop_by_hop: true,
            remove,
            rename: Vec::new(),
            set,
            append: Vec::new(),
        };
        let plugin = HeaderMutationPlugin::new("hm-1", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-sensitive"),
            HeaderValue::from_static("secret"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-sensitive"));
        assert_eq!(req.headers.get("x-custom-gateway").unwrap(), "velda");
    }

    #[test]
    fn test_from_config_complete() {
        let json_cfg = serde_json::json!({
            "strip_hop_by_hop": true,
            "remove_headers": ["x-internal-secret"],
            "rename_headers": {
                "x-old-id": "x-new-id"
            },
            "set_headers": {
                "x-forwarded-proto": "https"
            },
            "append_headers": {
                "x-forwarded-for": "$client_ip"
            }
        });
        let config = PluginConfig {
            id: "header_mutation".into(),
            phase: String::new(),
            config: json_cfg,
        };
        let plugin = HeaderMutationPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "header_mutation");
        assert!(plugin.config.strip_hop_by_hop);
        assert_eq!(plugin.config.remove.len(), 1);
        assert_eq!(plugin.config.rename.len(), 1);
        assert_eq!(plugin.config.set.len(), 1);
        assert_eq!(plugin.config.append.len(), 1);

        let bad_config = PluginConfig {
            id: "header_mutation".into(),
            phase: String::new(),
            config: serde_json::json!({}),
        };
        assert!(HeaderMutationPlugin::from_config(&bad_config).is_err());
    }
}
