//! Shared strongly typed identifiers used across the Velda data plane.
//!
//! These types intentionally wrap primitive integers instead of using
//! raw `u64`/`u32` values throughout the codebase. This prevents
//! accidentally mixing identifiers belonging to different domains.
//!
//! Using `#[repr(transparent)]` guarantees zero ABI overhead over raw integers,
//! allowing the compiler to pass IDs directly in CPU registers.

use std::fmt;

/// Unique identifier for a request.
///
/// A request ID identifies a single L7 request processed by Velda.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(pub u64);

impl RequestId {
    /// Creates a new request identifier.
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

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
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

/// Identifier of a configured route.
///
/// A route ID is assigned by the routing/configuration subsystem and
/// remains independent from the actual route matching algorithm.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteId(pub u32);

impl RouteId {
    /// Creates a new route identifier.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
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

/// Identifier of a configured upstream.
///
/// The ID identifies an upstream definition selected for the request.
/// It does not represent a concrete TCP connection.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpstreamId(pub u32);

impl UpstreamId {
    /// Creates a new upstream identifier.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strongly_typed_ids() {
        let req = RequestId::new(1001);
        let conn = ConnectionId::new(2002);
        let route = RouteId::new(3003);
        let up = UpstreamId::new(4004);

        assert_eq!(req.value(), 1001);
        assert_eq!(conn.value(), 2002);
        assert_eq!(route.value(), 3003);
        assert_eq!(up.value(), 4004);

        assert_eq!(format!("{req}"), "1001");
        assert_eq!(format!("{conn}"), "2002");
        assert_eq!(format!("{route}"), "3003");
        assert_eq!(format!("{up}"), "4004");
    }
}
