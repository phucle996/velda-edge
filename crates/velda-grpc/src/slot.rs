//! gRPC Metadata Slot Definitions (RFC 9113 / gRPC Wire Specification).
//!
//! # Architecture & Invariants (SRP: Protocol Header Semantics Only)
//! 1. **Zero-Overhead Metadata Storage**: Maps canonical gRPC trailers and metadata keys
//!    (`grpc-status`, `grpc-message`, `grpc-timeout`) to deterministic integer slots.
//! 2. **Complete HTTP Decoupling**: Pure gRPC metadata indices operate independently of HTTP status codes.

use http::header::HeaderName;

/// Canonical gRPC metadata slot indices (`0..=8`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrpcMetadataSlot {
    Status = 0,
    Message = 1,
    Encoding = 2,
    AcceptEncoding = 3,
    Timeout = 4,
    ContentType = 5,
    Te = 6,
    PreviousRpcAttempts = 7,
    Authority = 8,
}

impl GrpcMetadataSlot {
    /// Returns the integer slot index.
    #[inline(always)]
    pub const fn index(self) -> u8 {
        self as u8
    }

    /// Resolves a gRPC metadata key name to its fast-path slot index.
    #[inline]
    pub fn from_name(name: &HeaderName) -> Option<Self> {
        match name.as_str() {
            "grpc-status" => Some(Self::Status),
            "grpc-message" => Some(Self::Message),
            "grpc-encoding" => Some(Self::Encoding),
            "grpc-accept-encoding" => Some(Self::AcceptEncoding),
            "grpc-timeout" => Some(Self::Timeout),
            "content-type" => Some(Self::ContentType),
            "te" => Some(Self::Te),
            "grpc-previous-rpc-attempts" => Some(Self::PreviousRpcAttempts),
            ":authority" | "host" => Some(Self::Authority),
            _ => None,
        }
    }

    /// Canonical wire name of this metadata field.
    #[inline(always)]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Status => "grpc-status",
            Self::Message => "grpc-message",
            Self::Encoding => "grpc-encoding",
            Self::AcceptEncoding => "grpc-accept-encoding",
            Self::Timeout => "grpc-timeout",
            Self::ContentType => "content-type",
            Self::Te => "te",
            Self::PreviousRpcAttempts => "grpc-previous-rpc-attempts",
            Self::Authority => ":authority",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_slot_mapping() {
        assert_eq!(
            GrpcMetadataSlot::from_name(&HeaderName::from_static("grpc-status")),
            Some(GrpcMetadataSlot::Status)
        );
        assert_eq!(
            GrpcMetadataSlot::from_name(&HeaderName::from_static("grpc-timeout")),
            Some(GrpcMetadataSlot::Timeout)
        );
        assert_eq!(GrpcMetadataSlot::Status.wire_name(), "grpc-status");
    }
}
