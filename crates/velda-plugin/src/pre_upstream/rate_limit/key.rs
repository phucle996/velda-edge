//! Rate Limit Key Resolution and Client Identification.
//!
//! Extracts and hashes client identification keys (headers, client IP, composite keys)
//! for lock-striped rate limiting.

use std::hash::Hasher;

use velda_core::L7Request;

/// Key resolution strategy for client identification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyStrategy {
    /// Evaluates `key_headers` in priority order; first non-empty header/IP is selected (Default).
    #[default]
    Priority,
    /// Concatenates all specified headers and client IP into a multi-dimensional compound key.
    Composite,
}

impl KeyStrategy {
    /// Parses a key strategy identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "composite" | "combine" => Self::Composite,
            _ => Self::Priority,
        }
    }

    /// Resolves rate-limiting client identifier key based on this strategy.
    #[inline]
    pub fn resolve_key(&self, headers_list: &[String], req: &L7Request) -> String {
        match self {
            Self::Priority => {
                for hdr_name in headers_list {
                    if hdr_name == "client_ip" || hdr_name == "ip" {
                        if let Some(ip) = extract_client_ip(req) {
                            let (buf, len) = velda_core::lut::ip::format_ip_bytes(ip);
                            let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("");
                            return format!("ip:{ip_str}");
                        }
                    } else if let Some(val) = req.headers.get(hdr_name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            return format!("{hdr_name}:{trimmed}");
                        }
                    }
                }
                // Fallback if none matched
                if let Some(ip) = extract_client_ip(req) {
                    let (buf, len) = velda_core::lut::ip::format_ip_bytes(ip);
                    let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("");
                    format!("ip:{ip_str}")
                } else {
                    "global:default".to_string()
                }
            }
            Self::Composite => {
                let mut parts = Vec::with_capacity(headers_list.len());
                for hdr_name in headers_list {
                    if hdr_name == "client_ip" || hdr_name == "ip" {
                        if let Some(ip) = extract_client_ip(req) {
                            let (buf, len) = velda_core::lut::ip::format_ip_bytes(ip);
                            let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("unknown");
                            parts.push(ip_str.to_string());
                        } else {
                            parts.push("unknown".to_string());
                        }
                    } else if let Some(val) = req.headers.get(hdr_name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            parts.push(trimmed.to_string());
                        } else {
                            parts.push("none".to_string());
                        }
                    } else {
                        parts.push("none".to_string());
                    }
                }
                parts.join(":")
            }
        }
    }

    /// Optimization: Resolves and hashes the rate-limiting client identifier directly into a 64-bit integer
    /// with ZERO heap allocations (eliminating String formatting allocations on the serving hot path).
    #[inline]
    pub fn resolve_key_hash(&self, headers_list: &[String], req: &L7Request) -> u64 {
        let mut hasher = rustc_hash::FxHasher::default();
        match self {
            Self::Priority => {
                for hdr_name in headers_list {
                    if hdr_name == "client_ip" || hdr_name == "ip" {
                        if let Some(ip) = extract_client_ip(req) {
                            hasher.write(b"ip:");
                            hasher.write_u128(ip);
                            return hasher.finish();
                        }
                    } else if let Some(val) = req.headers.get(hdr_name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            hasher.write(hdr_name.as_bytes());
                            hasher.write(b":");
                            hasher.write(trimmed.as_bytes());
                            return hasher.finish();
                        }
                    }
                }
                // Fallback if none matched
                if let Some(ip) = extract_client_ip(req) {
                    hasher.write(b"ip:");
                    hasher.write_u128(ip);
                    hasher.finish()
                } else {
                    hasher.write(b"global:default");
                    hasher.finish()
                }
            }
            Self::Composite => {
                for hdr_name in headers_list {
                    if hdr_name == "client_ip" || hdr_name == "ip" {
                        if let Some(ip) = extract_client_ip(req) {
                            hasher.write_u128(ip);
                        } else {
                            hasher.write(b"unknown");
                        }
                    } else if let Some(val) = req.headers.get(hdr_name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            hasher.write(trimmed.as_bytes());
                        } else {
                            hasher.write(b"none");
                        }
                    } else {
                        hasher.write(b"none");
                    }
                    hasher.write(b":");
                }
                hasher.finish()
            }
        }
    }
}

/// Resolves client IP address as an RFC 4291 `u128` integer from request state or `HeaderWorkspace`.
#[inline]
pub fn extract_client_ip(req: &L7Request) -> Option<u128> {
    if req.state.client_ip != 0 {
        return Some(req.state.client_ip);
    }
    req.headers
        .get_ip_u128("x-forwarded-for")
        .or_else(|| req.headers.get_ip_u128("x-real-ip"))
}

/// Hashes the key string into a 64-bit integer for zero-alloc sharded table lookup.
// Optimization: Uses FxHasher instead of cryptographic DefaultHasher for 5-10x faster hashing.
#[inline(always)]
pub fn hash_key(key: &str) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    hasher.write(key.as_bytes());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    #[test]
    fn test_priority_key_resolution() {
        let strategy = KeyStrategy::Priority;
        let keys = vec!["x-api-key".to_string(), "client_ip".to_string()];

        let mut headers = HeaderWorkspace::empty(4);
        headers.insert("x-api-key", HeaderValue::from_static("key-12345"));
        headers.insert("x-forwarded-for", HeaderValue::from_static("1.2.3.4"));

        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        assert_eq!(strategy.resolve_key(&keys, &req), "x-api-key:key-12345");
    }

    #[test]
    fn test_composite_key_resolution() {
        let strategy = KeyStrategy::Composite;
        let keys = vec!["x-user-id".to_string(), "client_ip".to_string()];

        let mut headers = HeaderWorkspace::empty(4);
        headers.insert("x-user-id", HeaderValue::from_static("user_42"));
        headers.insert("x-forwarded-for", HeaderValue::from_static("1.2.3.4"));

        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        assert_eq!(strategy.resolve_key(&keys, &req), "user_42:1.2.3.4");
    }
}
