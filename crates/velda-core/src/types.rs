//! Shared strongly typed identifiers used across the Velda data plane.
//!
//! These types intentionally wrap primitive integers instead of using
//! raw `u64`/`u32` values throughout the codebase. This prevents
//! accidentally mixing identifiers belonging to different domains.
//!
//! Using `#[repr(transparent)]` guarantees zero ABI overhead over raw integers,
//! allowing the compiler to pass IDs directly in CPU registers.

use std::fmt;

/// Unique 128-bit identifier for an L7 request conforming to W3C TraceContext specification.
///
/// Encodes a 64-bit Unix Epoch timestamp in the high bits and a 64-bit worker/sequence entropy
/// in the low bits. Formats as 32 lowercase hex characters (`{:032x}`).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RequestId(pub u128);

impl RequestId {
    /// Creates a new 128-bit request identifier.
    #[inline(always)]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// Creates a request identifier from a 64-bit integer (useful for tests and backward compatibility).
    #[inline(always)]
    pub const fn from_u64(value: u64) -> Self {
        Self(value as u128)
    }

    /// Returns the underlying 128-bit identifier value.
    #[inline(always)]
    pub const fn value(self) -> u128 {
        self.0
    }

    /// Returns the high 64 bits (Unix Epoch Timestamp).
    #[inline(always)]
    pub const fn timestamp_secs(self) -> u64 {
        (self.0 >> 64) as u64
    }

    /// Returns the low 64 bits (Worker ID + Monotonic Sequence Entropy).
    #[inline(always)]
    pub const fn entropy(self) -> u64 {
        self.0 as u64
    }

    /// Formats the RequestId as a canonical 32-character lowercase hex string.
    #[inline(always)]
    pub fn to_hex_bytes(self) -> [u8; 32] {
        let mut hex = [0u8; 32];
        let high: &mut [u8; 16] = (&mut hex[0..16]).try_into().unwrap();
        crate::request_id::format_hex_16(self.timestamp_secs(), high);
        let low: &mut [u8; 16] = (&mut hex[16..32]).try_into().unwrap();
        crate::request_id::format_hex_16(self.entropy(), low);
        hex
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Unique identifier for an L4 connection.
///
/// A connection ID identifies a single physical transport-level connection
/// accepted by Velda, which may serve multiple requests over its lifetime.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

impl ConnectionId {
    /// Creates a new connection identifier.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying identifier value.
    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Canonical 32-bit FNV-1a deterministic hash for compiling string names into `u32` IDs.
///
/// Offset basis: `0x811c9dc5`, Prime: `0x01000193`.
#[inline]
pub const fn fnv1a_hash_32(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

/// Identifier of a configured ingress listener.
///
/// Deterministically derived from the listener configuration name at sync/compile time.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ListenerId(pub u32);

impl ListenerId {
    /// Creates a new listener identifier.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Deterministically computes a `ListenerId` from a listener name using 32-bit FNV-1a.
    #[inline]
    pub const fn from_name(name: &str) -> Self {
        Self(fnv1a_hash_32(name))
    }

    /// Returns the underlying identifier value.
    #[inline]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ListenerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for ListenerId {
    #[inline]
    fn from(name: &str) -> Self {
        Self::from_name(name)
    }
}

impl From<&String> for ListenerId {
    #[inline]
    fn from(name: &String) -> Self {
        Self::from_name(name.as_str())
    }
}

impl From<String> for ListenerId {
    #[inline]
    fn from(name: String) -> Self {
        Self::from_name(name.as_str())
    }
}

impl From<&std::sync::Arc<str>> for ListenerId {
    #[inline]
    fn from(name: &std::sync::Arc<str>) -> Self {
        Self::from_name(name)
    }
}

impl From<std::sync::Arc<str>> for ListenerId {
    #[inline]
    fn from(name: std::sync::Arc<str>) -> Self {
        Self::from_name(&name)
    }
}

/// Identifier of a configured route.
///
/// A route ID is assigned by the routing/configuration subsystem and
/// remains independent from the actual route matching algorithm.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct RouteId(pub u32);

impl RouteId {
    /// Creates a new route identifier.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Deterministically computes a `RouteId` from a route name using 32-bit FNV-1a.
    #[inline]
    pub const fn from_name(name: &str) -> Self {
        Self(fnv1a_hash_32(name))
    }

    /// Returns the underlying identifier value.
    #[inline]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl fmt::Display for RouteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for RouteId {
    #[inline]
    fn from(name: &str) -> Self {
        Self::from_name(name)
    }
}

impl From<&String> for RouteId {
    #[inline]
    fn from(name: &String) -> Self {
        Self::from_name(name.as_str())
    }
}

impl From<String> for RouteId {
    #[inline]
    fn from(name: String) -> Self {
        Self::from_name(name.as_str())
    }
}

/// Identifier of a configured upstream.
///
/// The ID identifies an upstream definition selected for the request.
/// It does not represent a concrete TCP connection.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct UpstreamId(pub u32);

impl UpstreamId {
    /// Creates a new upstream identifier.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Deterministically computes an `UpstreamId` from an upstream name using 32-bit FNV-1a.
    #[inline]
    pub const fn from_name(name: &str) -> Self {
        Self(fnv1a_hash_32(name))
    }

    /// Returns the underlying identifier value.
    #[inline]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl fmt::Display for UpstreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for UpstreamId {
    #[inline]
    fn from(name: &str) -> Self {
        Self::from_name(name)
    }
}

impl From<&String> for UpstreamId {
    #[inline]
    fn from(name: &String) -> Self {
        Self::from_name(name.as_str())
    }
}

impl From<String> for UpstreamId {
    #[inline]
    fn from(name: String) -> Self {
        Self::from_name(name.as_str())
    }
}

impl From<&std::sync::Arc<str>> for UpstreamId {
    #[inline]
    fn from(name: &std::sync::Arc<str>) -> Self {
        Self::from_name(name)
    }
}

impl From<std::sync::Arc<str>> for UpstreamId {
    #[inline]
    fn from(name: std::sync::Arc<str>) -> Self {
        Self::from_name(&name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strongly_typed_ids() {
        let req = RequestId::new(1001);
        let conn = ConnectionId::new(2002);
        let listener = ListenerId::new(2505);
        let route = RouteId::new(3003);
        let up = UpstreamId::new(4004);

        assert_eq!(req.value(), 1001);
        assert_eq!(conn.value(), 2002);
        assert_eq!(listener.value(), 2505);
        assert_eq!(route.value(), 3003);
        assert_eq!(up.value(), 4004);

        assert_eq!(format!("{req}"), "000000000000000000000000000003e9");
        assert_eq!(format!("{conn}"), "2002");
        assert_eq!(format!("{listener}"), "2505");
        assert_eq!(format!("{route}"), "3003");
        assert_eq!(format!("{up}"), "4004");
    }

    #[test]
    fn test_deterministic_fnv1a_ids() {
        let l1 = ListenerId::from_name("http-80");
        let l2 = ListenerId::from("http-80");
        assert_eq!(l1, l2);
        assert_ne!(l1, ListenerId::from_name("http-443"));

        let r1 = RouteId::from_name("route-api");
        let r2 = RouteId::from("route-api");
        assert_eq!(r1, r2);

        let u1 = UpstreamId::from_name("backend-v1");
        let u2 = UpstreamId::from("backend-v1");
        assert_eq!(u1, u2);
    }
}
