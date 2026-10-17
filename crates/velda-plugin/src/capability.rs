//! Plugin Capability and Reverse Permission Granting System.
//!
//! # Architecture and Invariants
//! 1. **Reverse Permission Granting ("Cấp quyền ngược")**:
//!    - Plugins declare the exact access boundaries they require on [`Header`] and [`Payload`].
//!    - A plugin that never touches payload bytes (`payload: AccessLevel::None`) allows the
//!      data plane engine to activate **Zero-Copy Direct Streaming** with zero RAM allocation for bodies.
//! 2. **Hardcoded Protocol Binding**:
//!    - Protocol affinity is defined strictly in Rust code via [`SupportedProtocols`], never dynamically in JSON.
//!    - At runtime snapshot pre-compute, protocol mismatch is rejected fail-fast.
//! 3. **Orthogonal Pipe Decoupling**:
//!    - Plugins never declare pipe strategies. Compatibility is derived strictly from the plugin's
//!      [`Direction`] and required payload access against the route's streaming axes.

use bitflags::bitflags;
use velda_core::ProtocolFamily;

/// Access permission level for a specific data plane partition (Header or Payload).
///
/// Follows a strict total order: `None < Read < Write`.
/// By definition, [`AccessLevel::Write`] subsumes [`AccessLevel::Read`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
#[repr(u8)]
pub enum AccessLevel {
    /// Zero access required. The data plane completely bypasses processing or buffering.
    #[default]
    None = 0,
    /// Read-only inspection. The data plane provides zero-copy borrowed slices (`&[u8]`).
    Read = 1,
    /// Mutable access. The data plane provides mutable buffers for transformations.
    Write = 2,
}

impl AccessLevel {
    /// Returns `true` if no access is required.
    #[inline(always)]
    pub const fn is_none(self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns `true` if read access is granted (either Read or Write).
    #[inline(always)]
    pub const fn can_read(self) -> bool {
        (self as u8) >= (Self::Read as u8)
    }

    /// Returns `true` if write access is granted.
    #[inline(always)]
    pub const fn can_write(self) -> bool {
        matches!(self, Self::Write)
    }

    /// Computes the union (superset) of two access levels.
    ///
    /// The higher access level always dominates:
    /// `union(None, Read) -> Read`, `union(Read, Write) -> Write`.
    #[inline(always)]
    pub fn union(self, other: Self) -> Self {
        std::cmp::max(self, other)
    }
}

bitflags! {
    /// Bitflags representing network and application protocol capabilities.
    ///
    /// Hardcoded directly on plugin implementations to eliminate protocol sniffing
    /// and dynamic schema guessing on the hot path.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct SupportedProtocols: u8 {
        /// HTTP/1.1 protocol.
        const HTTP1   = 1 << 0;
        /// HTTP/2 multiplexed binary framing protocol.
        const HTTP2   = 1 << 1;
        /// HTTP/3 QUIC datagram transport protocol.
        const HTTP3   = 1 << 2;
        /// gRPC length-prefixed streaming protocol over HTTP/2 or QUIC.
        const GRPC    = 1 << 3;
        /// Raw TCP Layer 4 direct forwarding.
        const RAW_TCP = 1 << 4;
        /// Raw UDP Layer 4 datagram forwarding.
        const RAW_UDP = 1 << 5;

        /// All Web REST HTTP protocols (HTTP/1.1, HTTP/2, HTTP/3).
        const ALL_HTTP = Self::HTTP1.bits() | Self::HTTP2.bits() | Self::HTTP3.bits();

        /// All Layer 7 application protocols (HTTP + gRPC).
        const ALL_L7   = Self::ALL_HTTP.bits() | Self::GRPC.bits();

        /// All Raw Layer 4 transport protocols (TCP + UDP).
        const ALL_L4   = Self::RAW_TCP.bits() | Self::RAW_UDP.bits();
    }
}

impl SupportedProtocols {
    /// Checks whether this protocol set supports the canonical [`ProtocolFamily`].
    #[inline]
    pub fn supports_family(&self, family: ProtocolFamily) -> bool {
        match family {
            ProtocolFamily::Http => self.intersects(Self::ALL_HTTP),
            ProtocolFamily::Grpc => self.contains(Self::GRPC),
            ProtocolFamily::Raw => self.intersects(Self::ALL_L4),
        }
    }
}

/// Execution direction of a plugin.
///
/// Derived directly from the execution phase and trait implementation:
/// - [`PreRoutePlugin`](crate::pre_route::PreRoutePlugin) and
///   [`PreUpstreamPlugin`](crate::pre_upstream::PreUpstreamPlugin) -> [`Direction::Request`]
/// - [`PostResponsePlugin`](crate::post_response::PostResponsePlugin) -> [`Direction::Response`]
/// - [`BackgroundPlugin`](crate::background::BackgroundPlugin) -> [`Direction::Background`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Operates on the incoming request path from client towards upstream.
    Request,
    /// Operates on the returning response path from upstream towards client.
    Response,
    /// Operates as an independent background service on auxiliary supervisory cores.
    Background,
}

/// Reverse-permission capability declaration for a plugin instance.
///
/// Declares access requirements for the specific [`Direction`] the plugin owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct PluginCapability {
    /// Access level requested for headers.
    pub header: AccessLevel,
    /// Access level requested for payload bodies.
    pub payload: AccessLevel,
}

impl PluginCapability {
    /// Creates a new capability declaration.
    #[inline(always)]
    pub const fn new(header: AccessLevel, payload: AccessLevel) -> Self {
        Self { header, payload }
    }

    /// Convenience constructor for plugins that only require header access.
    #[inline(always)]
    pub const fn header_only(header: AccessLevel) -> Self {
        Self {
            header,
            payload: AccessLevel::None,
        }
    }

    /// Returns `true` if payload body is completely bypassed (allows zero-copy streaming).
    #[inline(always)]
    pub const fn is_payload_passthrough(&self) -> bool {
        self.payload.is_none()
    }

    /// Computes the union (superset) of two plugin capabilities.
    #[inline(always)]
    pub fn union(self, other: Self) -> Self {
        Self {
            header: self.header.union(other.header),
            payload: self.payload.union(other.payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_level_ordering_and_union() {
        assert!(AccessLevel::None < AccessLevel::Read);
        assert!(AccessLevel::Read < AccessLevel::Write);

        assert!(AccessLevel::None.is_none());
        assert!(!AccessLevel::Read.is_none());
        assert!(!AccessLevel::Write.is_none());

        assert!(!AccessLevel::None.can_read());
        assert!(AccessLevel::Read.can_read());
        assert!(AccessLevel::Write.can_read());

        assert!(!AccessLevel::None.can_write());
        assert!(!AccessLevel::Read.can_write());
        assert!(AccessLevel::Write.can_write());

        assert_eq!(
            AccessLevel::None.union(AccessLevel::Read),
            AccessLevel::Read
        );
        assert_eq!(
            AccessLevel::Read.union(AccessLevel::Write),
            AccessLevel::Write
        );
        assert_eq!(
            AccessLevel::Write.union(AccessLevel::Read),
            AccessLevel::Write
        );
    }

    #[test]
    fn test_supported_protocols_matching() {
        let http_only = SupportedProtocols::ALL_HTTP;
        assert!(http_only.supports_family(ProtocolFamily::Http));
        assert!(!http_only.supports_family(ProtocolFamily::Grpc));
        assert!(!http_only.supports_family(ProtocolFamily::Raw));

        let grpc_only = SupportedProtocols::GRPC;
        assert!(!grpc_only.supports_family(ProtocolFamily::Http));
        assert!(grpc_only.supports_family(ProtocolFamily::Grpc));
        assert!(!grpc_only.supports_family(ProtocolFamily::Raw));

        let l7_all = SupportedProtocols::ALL_L7;
        assert!(l7_all.supports_family(ProtocolFamily::Http));
        assert!(l7_all.supports_family(ProtocolFamily::Grpc));
        assert!(!l7_all.supports_family(ProtocolFamily::Raw));

        let raw_all = SupportedProtocols::ALL_L4;
        assert!(!raw_all.supports_family(ProtocolFamily::Http));
        assert!(!raw_all.supports_family(ProtocolFamily::Grpc));
        assert!(raw_all.supports_family(ProtocolFamily::Raw));
    }

    #[test]
    fn test_plugin_capability_union() {
        let cap1 = PluginCapability::header_only(AccessLevel::Read);
        assert!(cap1.is_payload_passthrough());

        let cap2 = PluginCapability::new(AccessLevel::Write, AccessLevel::Read);
        assert!(!cap2.is_payload_passthrough());

        let combined = cap1.union(cap2);
        assert_eq!(combined.header, AccessLevel::Write);
        assert_eq!(combined.payload, AccessLevel::Read);
        assert!(!combined.is_payload_passthrough());
    }
}
