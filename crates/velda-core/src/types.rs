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
