//! Endpoint lifecycle states and re-exports.

pub use velda_core::{Endpoint, EndpointId};
pub use velda_discovery::EndpointSet;

/// Lifecycle state of an endpoint within the upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointState {
    /// Active and receiving new connections.
    Active,
    /// Temporarily excluded due to passive health failures.
    Unhealthy,
    /// Removed by discovery update; existing connections drain, new work excluded.
    Draining,
    /// Fully retired and decommissioned.
    Removed,
}

impl EndpointState {
    /// Returns `true` if the endpoint can accept new connection acquisitions.
    #[inline]
    pub const fn is_eligible_for_new_work(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns `true` if this endpoint is currently in draining mode.
    #[inline]
    pub const fn is_draining(&self) -> bool {
        matches!(self, Self::Draining)
    }
}
