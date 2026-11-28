//! Condition Evaluation, Disjoint Interval IP Matching, and Pre-Hashed Header Overrides.
//!
//! ```text
//!   L7Request
//!     ├─► client_ip    ──► req.state.client_ip (fallback: X-Real-IP / right-most X-Forwarded-For)
//!     │                     └─► IpRangeMatcher::contains (1 range: 2 cmp; N ranges: O(log N))
//!     ├─► header_match ──► pre-hashed HeaderKey lookup (zero runtime hashing)
//!     └─► overrides    ──► x-fault-delay-ms (clamped) & x-fault-abort-status (InternalStatus)
//! ```

use http::header::HeaderName;
use velda_core::{HeaderKey, InternalStatus, L7Request};

const KEY_X_REAL_IP: HeaderKey = HeaderKey::from_static("x-real-ip");
const KEY_X_FORWARDED_FOR: HeaderKey = HeaderKey::from_static("x-forwarded-for");
pub(super) const KEY_FAULT_DELAY_MS: HeaderKey = HeaderKey::from_static("x-fault-delay-ms");
pub(super) const KEY_FAULT_ABORT_STATUS: HeaderKey = HeaderKey::from_static("x-fault-abort-status");

/// Strongly typed matcher for candidate downstream IP addresses using disjoint integer intervals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IpRangeMatcher {
    /// Sorted, non-overlapping IPv4 intervals `(start_u32, end_u32)` inclusive.
    v4_ranges: Box<[(u32, u32)]>,
    /// Sorted, non-overlapping IPv6 intervals `(start_u128, end_u128)` inclusive.
    v6_ranges: Box<[(u128, u128)]>,
}

impl IpRangeMatcher {
    /// Parses a list of IP addresses or CIDR notations (e.g. `10.0.0.0/8`, `192.168.1.1`, `2001:db8::/32`)
    /// and compiles them into sorted, non-overlapping numeric intervals.
    pub fn from_cidrs(cidrs: &[String]) -> Result<Self, String> {
        let mut v4_intervals = Vec::new();
        let mut v6_intervals = Vec::new();

        for raw in cidrs {
            let s = raw.trim();
            if s.is_empty() {
                continue;
            }

            if let Some((ip_str, prefix_str)) = s.split_once('/') {
                let ip_trimmed = ip_str.trim();
                let prefix_trimmed = prefix_str.trim();
                let prefix: u8 = prefix_trimmed
                    .parse()
                    .map_err(|e| format!("Invalid CIDR prefix '{prefix_trimmed}': {e}"))?;

                if let Some(v4_bits) = velda_core::lut::ip::parse_ipv4_bytes(ip_trimmed.as_bytes())
                {
                    if prefix > 32 {
                        return Err(format!("IPv4 prefix cannot exceed 32, got {prefix}"));
                    }
                    let mask = if prefix == 0 {
                        0u32
                    } else {
                        !0u32 << (32 - prefix)
                    };
                    let start = v4_bits & mask;
                    let end = start | !mask;
                    v4_intervals.push((start, end));
                } else if let Some(v6_bits) =
                    velda_core::lut::ip::parse_ipv6_u128(ip_trimmed.as_bytes())
                {
                    if prefix > 128 {
                        return Err(format!("IPv6 prefix cannot exceed 128, got {prefix}"));
                    }
                    let mask = if prefix == 0 {
                        0u128
                    } else {
                        !0u128 << (128 - prefix)
                    };
                    let start = v6_bits & mask;
                    let end = start | !mask;
                    v6_intervals.push((start, end));
                } else {
                    return Err(format!("Invalid IP address '{ip_trimmed}'"));
                }
            } else if let Some(v4_bits) = velda_core::lut::ip::parse_ipv4_bytes(s.as_bytes()) {
                v4_intervals.push((v4_bits, v4_bits));
            } else if let Some(v6_bits) = velda_core::lut::ip::parse_ipv6_u128(s.as_bytes()) {
                v6_intervals.push((v6_bits, v6_bits));
            } else {
                return Err(format!("Invalid IP address '{s}'"));
            }
        }

        Ok(Self {
            v4_ranges: merge_intervals_u32(v4_intervals).into_boxed_slice(),
            v6_ranges: merge_intervals_u128(v6_intervals).into_boxed_slice(),
        })
    }

    /// Tests whether an RFC 4291 `u128` IP address falls within any configured network prefix.
    #[inline]
    pub fn contains(&self, ip: u128) -> bool {
        if let Some(bits) = velda_core::lut::ip::is_ipv4_mapped_u128(ip) {
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
        } else {
            match self.v6_ranges.len() {
                0 => false,
                1 => ip >= self.v6_ranges[0].0 && ip <= self.v6_ranges[0].1,
                _ => {
                    let idx = self.v6_ranges.partition_point(|&(_, end)| end < ip);
                    if idx < self.v6_ranges.len() {
                        ip >= self.v6_ranges[idx].0 && ip <= self.v6_ranges[idx].1
                    } else {
                        false
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

/// Merges overlapping and contiguous `(u32, u32)` intervals into a sorted disjoint vector.
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

/// Merges overlapping and contiguous `(u128, u128)` intervals into a sorted disjoint vector.
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

/// Rule for matching incoming request headers using pre-hashed [`HeaderKey`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultHeaderMatchRule {
    /// Matches if header is present, regardless of value.
    Present(HeaderKey),
    /// Matches if header is present and its UTF-8 value matches `expected`.
    Exact(HeaderKey, String),
}

impl FaultHeaderMatchRule {
    /// Constructs a presence rule from a [`HeaderName`], pre-computing its SIMD tag once.
    #[inline]
    pub fn present(name: HeaderName) -> Self {
        Self::Present(HeaderKey::from_name(name))
    }

    /// Constructs an exact-value rule from a [`HeaderName`], pre-computing its SIMD tag once.
    #[inline]
    pub fn exact(name: HeaderName, expected: impl Into<String>) -> Self {
        Self::Exact(HeaderKey::from_name(name), expected.into())
    }

    /// Evaluates this rule against request headers with zero runtime hashing.
    #[inline]
    pub fn matches(&self, headers: &velda_core::HeaderWorkspace) -> bool {
        match self {
            Self::Present(key) => headers.contains_prehashed(key),
            Self::Exact(key, expected) => headers
                .get_prehashed(key)
                .and_then(|val| val.to_str().ok())
                .is_some_and(|val| val == expected.as_str()),
        }
    }
}

/// Overall targeting condition and dynamic override settings for fault injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultCondition {
    /// IP range matcher. If configured, client IP must fall within one of the ranges.
    pub ip_matcher: Option<IpRangeMatcher>,
    /// Header match rule. If configured, request headers must satisfy the rule.
    pub header_match: Option<FaultHeaderMatchRule>,
    /// Whether scoped clients can dynamically trigger delay or abort via headers.
    pub allow_header_override: bool,
    /// Upper bound clamp on dynamic delay override in milliseconds.
    pub max_override_delay_ms: u64,
}

impl Default for FaultCondition {
    fn default() -> Self {
        Self {
            ip_matcher: None,
            header_match: None,
            allow_header_override: false,
            max_override_delay_ms: 5_000,
        }
    }
}

impl FaultCondition {
    /// Returns `true` if the request meets the scope conditions.
    #[inline]
    pub fn matches(&self, req: &L7Request) -> bool {
        if let Some(ip_matcher) = &self.ip_matcher
            && !ip_matcher.is_empty()
        {
            let client_ip = extract_client_ip(req);
            match client_ip {
                Some(ip) if ip_matcher.contains(ip) => {}
                _ => return false,
            }
        }

        if let Some(header_rule) = &self.header_match
            && !header_rule.matches(&req.headers)
        {
            return false;
        }

        true
    }

    /// Extracts dynamic header overrides if `allow_header_override` is enabled.
    ///
    /// Returns `(override_delay_ms, override_abort_status)`.
    #[inline]
    pub fn extract_overrides(&self, req: &L7Request) -> (Option<u64>, Option<InternalStatus>) {
        if !self.allow_header_override {
            return (None, None);
        }

        let delay_override = req
            .headers
            .get_prehashed(&KEY_FAULT_DELAY_MS)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(|ms| ms.min(self.max_override_delay_ms));

        let abort_override = req
            .headers
            .get_prehashed(&KEY_FAULT_ABORT_STATUS)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| InternalStatus::from_name(s.trim()))
            .filter(|s| s.is_aborted());

        (delay_override, abort_override)
    }
}

/// Extracts authoritative client IP as RFC 4291 `u128` from `req.state.client_ip`,
/// falling back to `X-Real-IP` or the right-most `X-Forwarded-For` entry.
#[inline]
fn extract_client_ip(req: &L7Request) -> Option<u128> {
    if req.state.client_ip != 0 {
        return Some(req.state.client_ip);
    }
    if let Some(val) = req.headers.get_prehashed(&KEY_X_REAL_IP) {
        let first = match val.as_bytes().iter().position(|&b| b == b',') {
            Some(idx) => &val.as_bytes()[..idx],
            None => val.as_bytes(),
        };
        if let Some(real_ip) = velda_core::lut::ip::parse_ip_u128(first.trim_ascii()) {
            return Some(real_ip);
        }
    }

    let val = req.headers.get_prehashed(&KEY_X_FORWARDED_FOR)?;
    let bytes = val.as_bytes();
    let last = match bytes.iter().rposition(|&b| b == b',') {
        Some(idx) => &bytes[idx + 1..],
        None => bytes,
    };
    velda_core::lut::ip::parse_ip_u128(last.trim_ascii())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace};

    #[test]
    fn test_ip_range_matcher_single_and_merged_cidrs() {
        let single = IpRangeMatcher::from_cidrs(&["10.0.0.0/8".to_string()]).unwrap();
        assert!(single.contains(velda_core::lut::ip::parse_ip_u128(b"10.1.2.3").unwrap()));
        assert!(!single.contains(velda_core::lut::ip::parse_ip_u128(b"11.0.0.1").unwrap()));

        let merged = IpRangeMatcher::from_cidrs(&[
            "192.168.1.0/24".to_string(),
            "192.168.2.0/24".to_string(),
            "10.5.0.1".to_string(),
        ])
        .unwrap();
        assert_eq!(merged.v4_ranges.len(), 2);
        assert!(merged.contains(velda_core::lut::ip::parse_ip_u128(b"192.168.2.200").unwrap()));
        assert!(merged.contains(velda_core::lut::ip::parse_ip_u128(b"10.5.0.1").unwrap()));
        assert!(!merged.contains(velda_core::lut::ip::parse_ip_u128(b"192.168.3.1").unwrap()));
    }

    #[test]
    fn test_extract_overrides_clamps_delay_and_validates_abort_status() {
        let cond = FaultCondition {
            allow_header_override: true,
            max_override_delay_ms: 1500,
            ..Default::default()
        };

        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(
            HeaderName::from_static("x-fault-delay-ms"),
            HeaderValue::from_static("9999"),
        );
        headers.insert(
            HeaderName::from_static("x-fault-abort-status"),
            HeaderValue::from_static("upstream_timeout"),
        );
        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        assert_eq!(
            cond.extract_overrides(&req),
            (Some(1500), Some(InternalStatus::UpstreamTimeout))
        );

        // Non-abort status ("completed") and unknown names must be ignored
        let mut bad_headers = HeaderWorkspace::empty(2);
        bad_headers.insert(
            HeaderName::from_static("x-fault-abort-status"),
            HeaderValue::from_static("completed"),
        );
        let bad_req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            bad_headers,
            Body::Empty,
        );
        assert_eq!(cond.extract_overrides(&bad_req), (None, None));
    }
}
