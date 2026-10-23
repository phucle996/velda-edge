//! Canary Traffic Splitting Pre-Upstream Plugin (`canary`).
//!
//! Provides controlled, progressive traffic steering between baseline and canary upstreams
//! based on:
//! 1. **Rule-Based Overrides**: Direct routing based on exact, prefix, suffix, contains,
//!    or exists header conditions evaluated on raw byte slices (Zero DFA/NFA Regex overhead).
//! 2. **Flexible Consistent Session Hashing**: Multi-strategy session stickiness (`priority`,
//!    `composite`, or `random`) across user-defined sources (`header:<name>`, `cookie:<name>`,
//!    `query:<name>`, `client_ip`, `uri_path`). Zero hardcoded default header/cookie names.
//! 3. **Weight-Based Percentage**: Configurable percentage (0% to 100%) for gradual rollout.
//! 4. **Spoof-Resistant Backend Marker**: Injects an informational header when routed to canary,
//!    and strictly purges any client-supplied marker on the baseline path to prevent telemetry/flag spoofing.

use http::header::{COOKIE, HeaderName, HeaderValue};
use std::cell::Cell;
use std::hash::Hasher;
use std::sync::Arc;
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Fast thread-local pseudo-random percentage generator (0..100).
///
/// Uses Xorshift64* to eliminate atomic MESI cache-line bouncing and lock contention
/// across worker threads while maintaining uniform distribution.
#[inline]
pub fn fast_prng_percent() -> u32 {
    thread_local! {
        static RNG_STATE: Cell<u64> = Cell::new({
            let ptr = &RNG_STATE as *const _ as usize as u64;
            let time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x517cc1b727220a95);
            ptr ^ time ^ 0x9e3779b97f4a7c15
        });
    }

    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            x = 0x853c49e6748fea9b;
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        let rand_val = x.wrapping_mul(0x2545f4914f6cdd1d);
        (rand_val % 100) as u32
    })
}

/// Matching operator for header override rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleOperator {
    /// Exact match (case-insensitive ASCII comparison).
    Equals(Box<[u8]>),
    /// Prefix match (case-insensitive ASCII comparison).
    Prefix(Box<[u8]>),
    /// Suffix match (case-insensitive ASCII comparison).
    Suffix(Box<[u8]>),
    /// Substring match (case-insensitive ASCII comparison).
    Contains(Box<[u8]>),
    /// Checks for the presence of the header regardless of value.
    Exists,
}

/// Rule matching against request headers to force immediate canary routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderRule {
    /// Pre-compiled header name.
    pub header: HeaderName,
    /// Matching operator and pre-compiled byte target.
    pub operator: RuleOperator,
}

impl HeaderRule {
    /// Evaluates this rule against incoming request headers directly on byte slices.
    #[inline]
    pub fn matches(&self, req: &L7Request) -> bool {
        for val in req.headers.get_all(&self.header) {
            let b = val.as_bytes();
            match &self.operator {
                RuleOperator::Exists => return true,
                RuleOperator::Equals(expected) => {
                    if b.eq_ignore_ascii_case(expected) {
                        return true;
                    }
                }
                RuleOperator::Prefix(prefix) => {
                    if b.len() >= prefix.len() && b[..prefix.len()].eq_ignore_ascii_case(prefix) {
                        return true;
                    }
                }
                RuleOperator::Suffix(suffix) => {
                    if b.len() >= suffix.len()
                        && b[b.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
                    {
                        return true;
                    }
                }
                RuleOperator::Contains(needle) => {
                    if !needle.is_empty()
                        && b.len() >= needle.len()
                        && b.windows(needle.len())
                            .any(|window| window.eq_ignore_ascii_case(needle))
                    {
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// Strategy for evaluating consistent session hashing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashStrategy {
    /// Evaluates `hash_sources` in priority order; first non-empty source is hashed (Default).
    #[default]
    Priority,
    /// Concatenates all specified sources into a compound hash key.
    Composite,
    /// Pure uniform pseudo-random percentage without key hashing.
    Random,
}

impl HashStrategy {
    /// Parses a hash strategy identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "composite" | "combine" => Self::Composite,
            "random" => Self::Random,
            _ => Self::Priority,
        }
    }
}

/// Source attribute used for consistent hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashSource {
    /// Downstream client IP (authoritative `x-real-ip` or rightmost `x-forwarded-for`).
    ClientIp,
    /// Full request URI path (`req.uri.path()`).
    UriPath,
    /// Pre-compiled header name (user-specified, e.g. `header:x-user-id`).
    Header(HeaderName),
    /// Cookie name (user-specified, e.g. `cookie:session_id`).
    Cookie(Box<str>),
    /// URL query parameter name (user-specified, e.g. `query:tenant_id`).
    Query(Box<str>),
}

impl HashSource {
    /// Extracts raw byte representation for this hash source directly without heap allocations.
    #[inline]
    pub fn extract_bytes<'a>(&'a self, req: &'a L7Request) -> Option<&'a [u8]> {
        match self {
            Self::ClientIp => {
                // Priority 1: x-real-ip (authoritatively overwritten by gateway from verified socket peer)
                if let Some(val) = req.headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
                    let trimmed = val.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.as_bytes());
                    }
                }
                // Priority 2: rightmost entry in x-forwarded-for (where gateway appends verified socket peer)
                if let Some(val) = req
                    .headers
                    .get("x-forwarded-for")
                    .and_then(|v| v.to_str().ok())
                    && let Some(last) = val.rsplit(',').next()
                {
                    let trimmed = last.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.as_bytes());
                    }
                }
                None
            }
            Self::UriPath => Some(req.uri.path().as_bytes()),
            Self::Header(name) => {
                for val in req.headers.get_all(name) {
                    let b = val.as_bytes();
                    if !b.is_empty() {
                        return Some(b);
                    }
                }
                None
            }
            Self::Cookie(target_name) => {
                for val in req.headers.get_all(COOKIE) {
                    if let Ok(s) = val.to_str() {
                        for pair in s.split(';') {
                            let trimmed = pair.trim();
                            if let Some((name, val)) = trimmed.split_once('=')
                                && name.trim().eq_ignore_ascii_case(target_name)
                            {
                                let v = val.trim();
                                if !v.is_empty() {
                                    return Some(v.as_bytes());
                                }
                            }
                        }
                    }
                }
                None
            }
            Self::Query(target_param) => {
                let query_str = req.uri.query()?;
                for pair in query_str.split('&') {
                    if let Some((name, val)) = pair.split_once('=') {
                        if name.eq_ignore_ascii_case(target_param) {
                            return Some(val.as_bytes());
                        }
                    } else if pair.eq_ignore_ascii_case(target_param) {
                        return Some(b"");
                    }
                }
                None
            }
        }
    }
}

/// Configuration settings for the Canary Traffic Splitting plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryConfig {
    /// Name of the target canary upstream (e.g., `"orders-v2"`).
    pub canary_upstream: String,
    /// Percentage of traffic directed to the canary upstream (0 to 100).
    pub weight: u32,
    /// Explicit header rules that force canary routing when matched.
    pub rules: Vec<HeaderRule>,
    /// Consistent hashing strategy.
    pub hash_strategy: HashStrategy,
    /// List of sources used for consistent session hashing.
    pub hash_sources: Vec<HashSource>,
    /// Header name injected into upstream request to mark canary status (or `"none"`).
    pub canary_header: String,
    /// Header value injected (or `"none"`).
    pub canary_header_value: String,
}

/// Canary Traffic Splitting Pre-Upstream Plugin.
#[derive(Debug, Clone)]
pub struct CanaryPlugin {
    pub id: String,
    pub canary_upstream: Arc<str>,
    pub weight: u32,
    pub rules: Vec<HeaderRule>,
    pub hash_strategy: HashStrategy,
    pub hash_sources: Vec<HashSource>,
    pub canary_marker: Option<(HeaderName, HeaderValue)>,
}

impl CanaryPlugin {
    /// Constructs a new canary plugin with full configuration.
    pub fn new(id: impl Into<String>, config: CanaryConfig) -> Result<Self, PluginError> {
        let canary_upstream: Arc<str> = Arc::from(config.canary_upstream.as_str());

        let canary_marker = if config.canary_header.trim().eq_ignore_ascii_case("none") {
            None
        } else {
            let name = HeaderName::from_bytes(config.canary_header.trim().as_bytes())
                .map_err(|e| PluginError::Config(format!("Invalid canary_header: {e}")))?;
            let val = HeaderValue::from_str(config.canary_header_value.trim())
                .map_err(|e| PluginError::Config(format!("Invalid canary_header_value: {e}")))?;
            Some((name, val))
        };

        Ok(Self {
            id: id.into(),
            canary_upstream,
            weight: config.weight,
            rules: config.rules,
            hash_strategy: config.hash_strategy,
            hash_sources: config.hash_sources,
            canary_marker,
        })
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_canary_config(&config.id, &config.config).map_err(|e| {
            PluginError::Config(format!(
                "Invalid canary configuration for '{}': {e}",
                config.id
            ))
        })?;

        let mut rules = Vec::with_capacity(cfg.rules.len());
        for r in &cfg.rules {
            let name = HeaderName::from_bytes(r.name.trim().as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid header rule name '{}': {e}", r.name))
            })?;

            let operator = match r.operator.trim().to_ascii_lowercase().as_str() {
                "equals" => RuleOperator::Equals(r.value.trim().as_bytes().into()),
                "prefix" => RuleOperator::Prefix(r.value.trim().as_bytes().into()),
                "suffix" => RuleOperator::Suffix(r.value.trim().as_bytes().into()),
                "contains" => RuleOperator::Contains(r.value.trim().as_bytes().into()),
                "exists" => RuleOperator::Exists,
                other => {
                    return Err(PluginError::Config(format!(
                        "Unsupported canary rule operator '{other}'"
                    )));
                }
            };

            rules.push(HeaderRule {
                header: name,
                operator,
            });
        }

        let hash_strategy = HashStrategy::parse(&cfg.hash_strategy);
        let mut hash_sources = Vec::with_capacity(cfg.hash_sources.len());

        for src in &cfg.hash_sources {
            let src_trim = src.trim();
            let src_lower = src_trim.to_ascii_lowercase();
            if src_lower == "client_ip" || src_lower == "ip" {
                hash_sources.push(HashSource::ClientIp);
            } else if src_lower == "uri_path" || src_lower == "path" {
                hash_sources.push(HashSource::UriPath);
            } else if let Some(hdr) = src_trim.strip_prefix("header:") {
                let name = HeaderName::from_bytes(hdr.trim().as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid hash_source header '{hdr}': {e}"))
                })?;
                hash_sources.push(HashSource::Header(name));
            } else if let Some(cookie) = src_trim.strip_prefix("cookie:") {
                hash_sources.push(HashSource::Cookie(cookie.trim().into()));
            } else if let Some(query) = src_trim.strip_prefix("query:") {
                hash_sources.push(HashSource::Query(query.trim().into()));
            } else {
                return Err(PluginError::Config(format!(
                    "Unsupported hash_source '{src_trim}' for canary plugin '{}'",
                    config.id
                )));
            }
        }

        Self::new(
            &config.id,
            CanaryConfig {
                canary_upstream: cfg.canary_upstream,
                weight: cfg.weight,
                rules,
                hash_strategy,
                hash_sources,
                canary_header: cfg.canary_header,
                canary_header_value: cfg.canary_header_value,
            },
        )
    }

    /// Evaluates whether the incoming request should be routed to the canary upstream.
    #[inline]
    pub fn should_route_to_canary(&self, req: &L7Request) -> bool {
        // Step 1: Rule-based overrides (highest priority)
        for rule in &self.rules {
            if rule.matches(req) {
                return true;
            }
        }

        // Step 2: Weight boundaries
        if self.weight == 0 {
            return false;
        }
        if self.weight >= 100 {
            return true;
        }

        // Step 3: Consistent Hashing / Weight Distribution
        self.evaluate_percentage(req) < self.weight
    }

    /// Evaluates consistent percentage (0..100) based on configured strategy and sources.
    #[inline]
    pub fn evaluate_percentage(&self, req: &L7Request) -> u32 {
        match self.hash_strategy {
            HashStrategy::Random => fast_prng_percent(),
            HashStrategy::Priority => {
                for src in &self.hash_sources {
                    if let Some(bytes) = src.extract_bytes(req) {
                        let mut hasher = rustc_hash::FxHasher::default();
                        hasher.write(bytes);
                        return (hasher.finish() % 100) as u32;
                    }
                }
                fast_prng_percent()
            }
            HashStrategy::Composite => {
                let mut hasher = rustc_hash::FxHasher::default();
                let mut matched_any = false;
                for src in &self.hash_sources {
                    if let Some(bytes) = src.extract_bytes(req) {
                        hasher.write(bytes);
                        matched_any = true;
                    } else {
                        hasher.write(b"\0");
                    }
                    hasher.write(b":");
                }
                if matched_any {
                    (hasher.finish() % 100) as u32
                } else {
                    fast_prng_percent()
                }
            }
        }
    }
}

impl PreUpstreamPlugin for CanaryPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, target_upstream: &mut Arc<str>) -> Action {
        if self.should_route_to_canary(req) {
            // 1. Inject informational marker header for backend telemetry/sandbox (if configured)
            if let Some((ref name, ref val)) = self.canary_marker {
                req.headers.insert(name.clone(), val.clone());
            }

            // 2. Direct in-memory target update without touching HTTP headers
            *target_upstream = Arc::clone(&self.canary_upstream);
        } else {
            // Baseline Path: Strip client-supplied canary marker to prevent telemetry/flag spoofing
            if let Some((ref name, _)) = self.canary_marker {
                req.headers.remove(name);
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
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_canary_rule_based_override_and_operators() {
        let config = CanaryConfig {
            canary_upstream: "payments-v2".into(),
            weight: 0,
            rules: vec![
                HeaderRule {
                    header: HeaderName::from_static("x-canary"),
                    operator: RuleOperator::Equals(b"always".to_vec().into_boxed_slice()),
                },
                HeaderRule {
                    header: HeaderName::from_static("x-user-email"),
                    operator: RuleOperator::Suffix(b"@velda.io".to_vec().into_boxed_slice()),
                },
                HeaderRule {
                    header: HeaderName::from_static("x-client-version"),
                    operator: RuleOperator::Prefix(b"v2-".to_vec().into_boxed_slice()),
                },
                HeaderRule {
                    header: HeaderName::from_static("user-agent"),
                    operator: RuleOperator::Contains(b"mobile".to_vec().into_boxed_slice()),
                },
                HeaderRule {
                    header: HeaderName::from_static("x-debug"),
                    operator: RuleOperator::Exists,
                },
            ],
            hash_strategy: HashStrategy::Random,
            hash_sources: Vec::new(),
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        };

        let plugin = CanaryPlugin::new("canary-test", config).unwrap();

        // 1. Equals match
        let mut req1 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-canary", HeaderValue::from_static("always"));
                h
            },
            Body::Empty,
        );
        let mut target1 = Arc::from("payments-v1");
        plugin.on_upstream_request(&mut req1, &mut target1);
        assert_eq!(target1.as_ref(), "payments-v2");
        assert_eq!(req1.headers.get("x-velda-canary").unwrap(), "true");

        // 2. Suffix match
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-user-email", HeaderValue::from_static("dev@velda.io"));
                h
            },
            Body::Empty,
        );
        let mut target2 = Arc::from("payments-v1");
        plugin.on_upstream_request(&mut req2, &mut target2);
        assert_eq!(target2.as_ref(), "payments-v2");

        // 3. Prefix match
        let mut req3 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-client-version", HeaderValue::from_static("v2-beta"));
                h
            },
            Body::Empty,
        );
        let mut target3 = Arc::from("payments-v1");
        plugin.on_upstream_request(&mut req3, &mut target3);
        assert_eq!(target3.as_ref(), "payments-v2");

        // 4. Contains match
        let mut req4 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert(
                    "user-agent",
                    HeaderValue::from_static("Mozilla-Mobile-Safari"),
                );
                h
            },
            Body::Empty,
        );
        let mut target4 = Arc::from("payments-v1");
        plugin.on_upstream_request(&mut req4, &mut target4);
        assert_eq!(target4.as_ref(), "payments-v2");

        // 5. Exists match
        let mut req5 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-debug", HeaderValue::from_static("any-value"));
                h
            },
            Body::Empty,
        );
        let mut target5 = Arc::from("payments-v1");
        plugin.on_upstream_request(&mut req5, &mut target5);
        assert_eq!(target5.as_ref(), "payments-v2");

        // 6. Non-matching request with spoofed canary marker -> marker MUST be stripped!
        let mut req_spoof = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-velda-canary", HeaderValue::from_static("true"));
                h
            },
            Body::Empty,
        );
        let mut target_base = Arc::from("payments-v1");
        plugin.on_upstream_request(&mut req_spoof, &mut target_base);
        assert_eq!(target_base.as_ref(), "payments-v1");
        assert!(
            !req_spoof.headers.contains_key("x-velda-canary"),
            "Spoofed canary marker must be stripped on baseline path!"
        );
    }

    #[test]
    fn test_priority_hash_sources_user_defined() {
        let config = CanaryConfig {
            canary_upstream: "service-canary".into(),
            weight: 50,
            rules: Vec::new(),
            hash_strategy: HashStrategy::Priority,
            hash_sources: vec![
                HashSource::Header(HeaderName::from_static("x-user-id")),
                HashSource::Cookie("session_token".into()),
                HashSource::Query("tenant_id".into()),
                HashSource::ClientIp,
            ],
            canary_header: "none".into(),
            canary_header_value: "none".into(),
        };
        let plugin = CanaryPlugin::new("canary-multi", config).unwrap();

        // 1. First priority: header
        let req_header = L7Request::new(
            Method::GET,
            Uri::from_static("/test?tenant_id=123"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-user-id", HeaderValue::from_static("user-999"));
                h
            },
            Body::Empty,
        );
        let pct_hdr1 = plugin.evaluate_percentage(&req_header);
        let pct_hdr2 = plugin.evaluate_percentage(&req_header);
        assert_eq!(pct_hdr1, pct_hdr2);

        // 2. Second priority: cookie
        let req_cookie = L7Request::new(
            Method::GET,
            Uri::from_static("/test?tenant_id=123"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert(
                    COOKIE,
                    HeaderValue::from_static("theme=dark; session_token=tok_456; lang=en"),
                );
                h
            },
            Body::Empty,
        );
        let pct_cook1 = plugin.evaluate_percentage(&req_cookie);
        let pct_cook2 = plugin.evaluate_percentage(&req_cookie);
        assert_eq!(pct_cook1, pct_cook2);

        // 3. Third priority: query param
        let req_query = L7Request::new(
            Method::GET,
            Uri::from_static("/test?other=1&tenant_id=corp_alpha"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let pct_q1 = plugin.evaluate_percentage(&req_query);
        let pct_q2 = plugin.evaluate_percentage(&req_query);
        assert_eq!(pct_q1, pct_q2);
    }

    #[test]
    fn test_composite_hash_consistency() {
        let config = CanaryConfig {
            canary_upstream: "service-canary".into(),
            weight: 50,
            rules: Vec::new(),
            hash_strategy: HashStrategy::Composite,
            hash_sources: vec![
                HashSource::Header(HeaderName::from_static("x-tenant-id")),
                HashSource::ClientIp,
            ],
            canary_header: "x-canary-route".into(),
            canary_header_value: "yes".into(),
        };
        let plugin = CanaryPlugin::new("canary-comp", config).unwrap();

        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            {
                let mut h = HeaderMap::new();
                h.insert("x-tenant-id", HeaderValue::from_static("tenant-42"));
                h.insert("x-real-ip", HeaderValue::from_static("192.168.1.100"));
                h
            },
            Body::Empty,
        );
        let p1 = plugin.evaluate_percentage(&req);
        let p2 = plugin.evaluate_percentage(&req);
        assert_eq!(p1, p2, "Composite hash must be deterministic");
    }

    #[test]
    fn test_canary_from_config_multi_sources() {
        let json_cfg = serde_json::json!({
            "canary_upstream": "catalog-v2",
            "weight": 20,
            "rules": [
                {
                    "name": "x-canary",
                    "operator": "equals",
                    "value": "always"
                }
            ],
            "hash_strategy": "priority",
            "hash_sources": [
                "header:x-device-id",
                "client_ip"
            ],
            "canary_header": "x-velda-canary",
            "canary_header_value": "true"
        });
        let config = PluginConfig {
            id: "canary".into(),
            phase: String::new(),
            config: json_cfg,
        };
        let plugin = CanaryPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "canary");
        assert_eq!(plugin.canary_upstream.as_ref(), "catalog-v2");
        assert_eq!(plugin.weight, 20);
        assert_eq!(plugin.rules.len(), 1);
        assert_eq!(plugin.hash_sources.len(), 2);
    }
}
