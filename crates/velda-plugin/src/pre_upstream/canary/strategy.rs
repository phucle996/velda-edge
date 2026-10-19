//! Canary Hash Keys and Conditional Header Rules.
//!
//! Provides consistent session hashing and explicit header matching strategies
//! for progressive canary rollouts.

use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};

use http::header::COOKIE;
use rustc_hash::FxHasher;
use velda_core::L7Request;

static CANARY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key source used for consistent hashing to ensure session stickiness.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HashSource {
    /// Uniform pseudo-random distribution per request (default).
    #[default]
    Random,
    /// Hashes client IP address (resolved from `x-forwarded-for` or `x-real-ip`).
    ClientIp,
    /// Hashes a specific request header (e.g., `x-user-id`, `authorization`, `x-account-id`).
    Header(String),
    /// Hashes a specific cookie value (e.g., `session_id`, `user_token`).
    Cookie(String),
}

impl HashSource {
    /// Parses a hash source definition from configuration format.
    pub fn parse(source: &str, key: Option<&str>) -> Self {
        match source.to_ascii_lowercase().as_str() {
            "client_ip" | "ip" => Self::ClientIp,
            "header" => Self::Header(key.unwrap_or("x-user-id").to_string()),
            "cookie" => Self::Cookie(key.unwrap_or("session_id").to_string()),
            _ => Self::Random,
        }
    }

    /// Evaluates consistent hash value (0..100) for the given request.
    #[inline]
    pub fn evaluate_percentage(&self, req: &L7Request) -> u32 {
        match self {
            Self::Random => {
                let count = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
                (count % 100) as u32
            }
            Self::ClientIp => {
                let ip_str = req
                    .headers
                    .get("x-forwarded-for")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.split(',').next())
                    .map(|s| s.trim())
                    .or_else(|| {
                        req.headers
                            .get("x-real-ip")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.trim())
                    })
                    .unwrap_or("127.0.0.1");

                hash_str_to_percent(ip_str)
            }
            Self::Header(header_name) => {
                if let Some(val) = req.headers.get(header_name).and_then(|v| v.to_str().ok()) {
                    hash_str_to_percent(val)
                } else {
                    // Fall back to random if header is missing
                    let count = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
                    (count % 100) as u32
                }
            }
            Self::Cookie(cookie_name) => {
                if let Some(val) = extract_cookie(req, cookie_name) {
                    hash_str_to_percent(val)
                } else {
                    // Fall back to random if cookie is missing
                    let count = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
                    (count % 100) as u32
                }
            }
        }
    }
}

/// Conditional rule matching against request headers to force canary routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderRule {
    /// Target header name to evaluate (case-insensitive).
    pub header: String,
    /// Exact value match (e.g., `exact: "beta"` or `exact: "always"`).
    pub exact: Option<String>,
    /// Prefix value match (e.g., `prefix: "internal-"`).
    pub prefix: Option<String>,
    /// Triggers if the header exists regardless of value.
    pub exists: bool,
}

impl HeaderRule {
    /// Evaluates this rule against the request headers.
    #[inline]
    pub fn matches(&self, req: &L7Request) -> bool {
        let val = req.headers.get(&self.header).and_then(|v| v.to_str().ok());

        match val {
            Some(v) => {
                if self.exists {
                    return true;
                }
                if let Some(ref exact_val) = self.exact
                    && v.eq_ignore_ascii_case(exact_val)
                {
                    return true;
                }
                if let Some(ref prefix_val) = self.prefix
                    && v.starts_with(prefix_val)
                {
                    return true;
                }
                false
            }
            None => false,
        }
    }
}

/// Computes consistent percentage (0..100) from string slice via FxHasher.
#[inline]
fn hash_str_to_percent(s: &str) -> u32 {
    let mut hasher = FxHasher::default();
    hasher.write(s.as_bytes());
    (hasher.finish() % 100) as u32
}

/// Extracts cookie value by name from the `Cookie` header.
#[inline]
fn extract_cookie<'a>(req: &'a L7Request, target_name: &str) -> Option<&'a str> {
    let raw_cookies = req.headers.get(COOKIE).and_then(|v| v.to_str().ok())?;
    for pair in raw_cookies.split(';') {
        let trimmed = pair.trim();
        if let Some((name, val)) = trimmed.split_once('=')
            && name.trim().eq_ignore_ascii_case(target_name)
        {
            return Some(val.trim());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_header_rule_matching() {
        let rule_exact = HeaderRule {
            header: "x-user-role".into(),
            exact: Some("beta-tester".into()),
            prefix: None,
            exists: false,
        };

        let mut headers = HeaderMap::new();
        headers.insert("x-user-role", HeaderValue::from_static("beta-tester"));
        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        assert!(rule_exact.matches(&req));

        let rule_prefix = HeaderRule {
            header: "x-client-id".into(),
            exact: None,
            prefix: Some("corp-".into()),
            exists: false,
        };

        let mut headers2 = HeaderMap::new();
        headers2.insert("x-client-id", HeaderValue::from_static("corp-dev-99"));
        let req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers2,
            Body::Empty,
        );
        assert!(rule_prefix.matches(&req2));
    }

    #[test]
    fn test_consistent_hashing() {
        let hash_source = HashSource::Header("x-user-id".into());

        let mut headers = HeaderMap::new();
        headers.insert("x-user-id", HeaderValue::from_static("usr_998877"));
        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let pct1 = hash_source.evaluate_percentage(&req);
        let pct2 = hash_source.evaluate_percentage(&req);
        assert_eq!(
            pct1, pct2,
            "Consistent hash must yield identical percentage for same key"
        );
        assert!(pct1 < 100);
    }
}
