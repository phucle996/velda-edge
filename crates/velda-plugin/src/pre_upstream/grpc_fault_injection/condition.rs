//! gRPC Condition Evaluation, Disjoint Interval IP Matching, and RPC Target Filtering.
//!
//! # Architecture & Performance Invariants
//! - **Disjoint Interval IP Matching**: All IPv4 and IPv6 CIDR prefixes are converted at
//!   instantiation time into sorted, non-overlapping `[start, end]` integer ranges (`u32` for IPv4,
//!   `u128` for IPv6). Matching uses $O(\log N)$ binary search (`partition_point`) with zero
//!   heap allocations and zero linear scanning.
//! - **RPC Target Matcher**: Matches against gRPC service and method paths (`/{package}.{service}/{method}`).
//! - **Dynamic Overrides**: Evaluates `x-grpc-fault-delay-ms` and `x-grpc-fault-abort-code` (0..=16)
//!   only when `allow_header_override` is explicitly set to `true`. Max delay is strictly clamped
//!   to `max_override_delay_ms` to prevent Slowloris attacks.

use std::net::IpAddr;

use http::header::HeaderName;
use velda_core::L7Request;

/// Strongly typed matcher for candidate downstream IP addresses using disjoint integer intervals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IpRangeMatcher {
    /// Sorted, non-overlapping IPv4 intervals `(start_u32, end_u32)` inclusive.
    v4_ranges: Box<[(u32, u32)]>,
    /// Sorted, non-overlapping IPv6 intervals `(start_u128, end_u128)` inclusive.
    v6_ranges: Box<[(u128, u128)]>,
}

impl IpRangeMatcher {
    /// Parses a list of IP addresses or CIDR notations and compiles them into sorted intervals.
    pub fn from_cidrs(cidrs: &[String]) -> Result<Self, String> {
        let mut v4_intervals = Vec::new();
        let mut v6_intervals = Vec::new();

        for raw in cidrs {
            let s = raw.trim();
            if s.is_empty() {
                continue;
            }

            if let Some((ip_str, prefix_str)) = s.split_once('/') {
                let ip: IpAddr = ip_str
                    .trim()
                    .parse()
                    .map_err(|e| format!("Invalid IP address '{ip_str}': {e}"))?;
                let prefix: u8 = prefix_str
                    .trim()
                    .parse()
                    .map_err(|e| format!("Invalid CIDR prefix '{prefix_str}': {e}"))?;

                match ip {
                    IpAddr::V4(v4) => {
                        if prefix > 32 {
                            return Err(format!("IPv4 prefix cannot exceed 32, got {prefix}"));
                        }
                        let mask = if prefix == 0 {
                            0u32
                        } else {
                            !0u32 << (32 - prefix)
                        };
                        let start = v4.to_bits() & mask;
                        let end = start | !mask;
                        v4_intervals.push((start, end));
                    }
                    IpAddr::V6(v6) => {
                        if prefix > 128 {
                            return Err(format!("IPv6 prefix cannot exceed 128, got {prefix}"));
                        }
                        let mask = if prefix == 0 {
                            0u128
                        } else {
                            !0u128 << (128 - prefix)
                        };
                        let start = v6.to_bits() & mask;
                        let end = start | !mask;
                        v6_intervals.push((start, end));
                    }
                }
            } else {
                let ip: IpAddr = s
                    .parse()
                    .map_err(|e| format!("Invalid IP address '{s}': {e}"))?;
                match ip {
                    IpAddr::V4(v4) => {
                        let bits = v4.to_bits();
                        v4_intervals.push((bits, bits));
                    }
                    IpAddr::V6(v6) => {
                        let bits = v6.to_bits();
                        v6_intervals.push((bits, bits));
                    }
                }
            }
        }

        Ok(Self {
            v4_ranges: merge_intervals_u32(v4_intervals).into_boxed_slice(),
            v6_ranges: merge_intervals_u128(v6_intervals).into_boxed_slice(),
        })
    }

    /// Tests whether an [`IpAddr`] falls within any configured network prefix.
    #[inline]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => {
                let bits = v4.to_bits();
                match self.v4_ranges.len() {
                    0 => false,
                    1 => bits >= self.v4_ranges[0].0 && bits <= self.v4_ranges[0].1,
                    _ => {
                        let idx = self.v4_ranges.partition_point(|&(_, end)| end < bits);
                        if idx < self.v4_ranges.len() {
                            bits >= self.v4_ranges[idx].0 && bits <= self.v4_ranges[idx].1
                        } else {
                            false
                        }
                    }
                }
            }
            IpAddr::V6(v6) => {
                let bits = v6.to_bits();
                match self.v6_ranges.len() {
                    0 => false,
                    1 => bits >= self.v6_ranges[0].0 && bits <= self.v6_ranges[0].1,
                    _ => {
                        let idx = self.v6_ranges.partition_point(|&(_, end)| end < bits);
                        if idx < self.v6_ranges.len() {
                            bits >= self.v6_ranges[idx].0 && bits <= self.v6_ranges[idx].1
                        } else {
                            false
                        }
                    }
                }
            }
        }
    }

    /// Returns `true` if no IP ranges were configured.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.v4_ranges.is_empty() && self.v6_ranges.is_empty()
    }
}

/// Merges overlapping and contiguous `(u32, u32)` intervals.
fn merge_intervals_u32(mut intervals: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    if intervals.is_empty() {
        return intervals;
    }
    intervals.sort_unstable_by_key(|&(s, _)| s);
    let mut merged: Vec<(u32, u32)> = Vec::with_capacity(intervals.len());
    for (s, e) in intervals {
        if let Some(last) = merged.last_mut()
            && s <= last.1.saturating_add(1)
        {
            last.1 = last.1.max(e);
            continue;
        }
        merged.push((s, e));
    }
    merged
}

/// Merges overlapping and contiguous `(u128, u128)` intervals.
fn merge_intervals_u128(mut intervals: Vec<(u128, u128)>) -> Vec<(u128, u128)> {
    if intervals.is_empty() {
        return intervals;
    }
    intervals.sort_unstable_by_key(|&(s, _)| s);
    let mut merged: Vec<(u128, u128)> = Vec::with_capacity(intervals.len());
    for (s, e) in intervals {
        if let Some(last) = merged.last_mut()
            && s <= last.1.saturating_add(1)
        {
            last.1 = last.1.max(e);
            continue;
        }
        merged.push((s, e));
    }
    merged
}

/// Rule for matching incoming gRPC metadata headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderMatchRule {
    /// Matches if metadata header is present, regardless of value.
    Present(HeaderName),
    /// Matches if metadata header is present and its string value equals `expected`.
    Exact(HeaderName, String),
}

impl HeaderMatchRule {
    /// Evaluates this rule against request metadata headers.
    #[inline]
    pub fn matches(&self, headers: &http::HeaderMap) -> bool {
        match self {
            Self::Present(name) => headers.contains_key(name),
            Self::Exact(name, expected) => headers
                .get(name)
                .and_then(|val| val.to_str().ok())
                .is_some_and(|val| val == expected.as_str()),
        }
    }
}

/// Rule for matching gRPC RPC service or method paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrpcTargetMatcher {
    /// Matches any RPC path starting with a service prefix (e.g. `/payment.v1.PaymentService/`).
    ServicePrefix(String),
    /// Matches an exact gRPC method path (e.g. `/payment.v1.PaymentService/Charge`).
    ExactPath(String),
}

impl GrpcTargetMatcher {
    /// Evaluates whether the request URI path matches this target rule.
    #[inline]
    pub fn matches(&self, path: &str) -> bool {
        match self {
            Self::ServicePrefix(prefix) => path.starts_with(prefix.as_str()),
            Self::ExactPath(exact) => path == exact.as_str(),
        }
    }
}

/// Overall targeting condition and dynamic override settings for gRPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcFaultCondition {
    /// IP range matcher. If configured, client IP must fall within one of the ranges.
    pub ip_matcher: Option<IpRangeMatcher>,
    /// Metadata header match rule.
    pub header_match: Option<HeaderMatchRule>,
    /// RPC Service/Method target matcher.
    pub target_matcher: Option<GrpcTargetMatcher>,
    /// Whether downstream clients can dynamically trigger delay or abort via headers.
    pub allow_header_override: bool,
    /// Upper bound clamp on dynamic delay override (milliseconds).
    pub max_override_delay_ms: u64,
}

impl Default for GrpcFaultCondition {
    fn default() -> Self {
        Self {
            ip_matcher: None,
            header_match: None,
            target_matcher: None,
            allow_header_override: false,
            max_override_delay_ms: 60_000,
        }
    }
}

impl GrpcFaultCondition {
    /// Returns `true` if the gRPC request meets all configured scope conditions.
    #[inline]
    pub fn matches(&self, req: &L7Request) -> bool {
        // 1. IP range match
        if let Some(ip_matcher) = &self.ip_matcher
            && !ip_matcher.is_empty()
        {
            let client_ip = extract_client_ip(req);
            match client_ip {
                Some(ip) if ip_matcher.contains(ip) => {}
                _ => return false,
            }
        }

        // 2. Metadata header match
        if let Some(header_rule) = &self.header_match
            && !header_rule.matches(&req.headers)
        {
            return false;
        }

        // 3. gRPC Service/Method target match
        if let Some(target_matcher) = &self.target_matcher
            && !target_matcher.matches(req.uri.path())
        {
            return false;
        }

        true
    }

    /// Extracts dynamic header overrides if `allow_header_override` is enabled.
    ///
    /// Returns `(override_delay_ms, override_abort_status_code)`.
    #[inline]
    pub fn extract_overrides(&self, req: &L7Request) -> (Option<u64>, Option<u32>) {
        if !self.allow_header_override {
            return (None, None);
        }

        // Extract delay override (e.g. x-grpc-fault-delay-ms: 300)
        let delay_override = req
            .headers
            .get("x-grpc-fault-delay-ms")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|ms| ms.min(self.max_override_delay_ms));

        // Extract canonical gRPC abort status code (0..=16)
        let abort_override = req
            .headers
            .get("x-grpc-fault-abort-code")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|&code| code <= 16);

        (delay_override, abort_override)
    }
}

/// Helper to extract client IP from headers.
#[inline]
fn extract_client_ip(req: &L7Request) -> Option<IpAddr> {
    req.headers
        .get("x-forwarded-for")
        .and_then(|val| val.to_str().ok())
        .and_then(|raw| raw.split(',').next())
        .and_then(|first| first.trim().parse::<IpAddr>().ok())
        .or_else(|| {
            req.headers
                .get("x-real-ip")
                .and_then(|val| val.to_str().ok())
                .and_then(|raw| raw.trim().parse::<IpAddr>().ok())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_grpc_target_matcher() {
        let matcher = GrpcTargetMatcher::ServicePrefix("/order.v1.OrderService/".to_string());
        assert!(matcher.matches("/order.v1.OrderService/CreateOrder"));
        assert!(matcher.matches("/order.v1.OrderService/CancelOrder"));
        assert!(!matcher.matches("/payment.v1.PaymentService/Charge"));

        let exact = GrpcTargetMatcher::ExactPath("/payment.v1.PaymentService/Charge".to_string());
        assert!(exact.matches("/payment.v1.PaymentService/Charge"));
        assert!(!exact.matches("/payment.v1.PaymentService/Refund"));
    }

    #[test]
    fn test_grpc_dynamic_overrides() {
        let condition = GrpcFaultCondition {
            allow_header_override: true,
            max_override_delay_ms: 10_000,
            ..Default::default()
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-grpc-fault-delay-ms"),
            HeaderValue::from_static("50000"), // Exceeds clamp!
        );
        headers.insert(
            HeaderName::from_static("x-grpc-fault-abort-code"),
            HeaderValue::from_static("14"), // UNAVAILABLE
        );
        let req = L7Request::new(
            Method::POST,
            Uri::from_static("/order.v1.OrderService/CreateOrder"),
            Version::HTTP_2,
            headers,
            Body::Empty,
        );

        let (delay_ms, abort_code) = condition.extract_overrides(&req);
        assert_eq!(delay_ms, Some(10_000)); // Clamped!
        assert_eq!(abort_code, Some(14));
    }
}
