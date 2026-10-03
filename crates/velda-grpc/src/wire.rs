//! Canonical wire constants and HTTP/2 metadata headers for gRPC.
//!
//! Strictly enforces the official [gRPC over HTTP/2 Wire Specification](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
//! Encapsulated within [`GrpcWire`] to avoid namespace pollution.

use http::HeaderName;
use http::header::HeaderValue;

/// Canonical gRPC wire headers, content types, and metadata constants.
pub struct GrpcWire;

impl GrpcWire {
    /// Standard MIME Content-Type for gRPC requests and responses ("application/grpc").
    pub const CONTENT_TYPE: &'static str = "application/grpc";

    /// Standard static `HeaderValue` for `content-type: application/grpc`.
    pub const CONTENT_TYPE_VALUE: HeaderValue = HeaderValue::from_static(Self::CONTENT_TYPE);

    /// Standard MIME Content-Type for Proto-encoded gRPC requests ("application/grpc+proto").
    pub const CONTENT_TYPE_PROTO: &'static str = "application/grpc+proto";

    /// Standard static `HeaderValue` for `content-type: application/grpc+proto`.
    pub const CONTENT_TYPE_PROTO_VALUE: HeaderValue =
        HeaderValue::from_static(Self::CONTENT_TYPE_PROTO);

    /// Header/Trailer name carrying the canonical gRPC status code (`0`..`16`).
    pub const STATUS_HEADER: &'static str = "grpc-status";

    /// Static `HeaderName` for `grpc-status`.
    pub const STATUS_NAME: HeaderName = HeaderName::from_static(Self::STATUS_HEADER);

    /// Header/Trailer name carrying the human-readable error description.
    pub const MESSAGE_HEADER: &'static str = "grpc-message";

    /// Static `HeaderName` for `grpc-message`.
    pub const MESSAGE_NAME: HeaderName = HeaderName::from_static(Self::MESSAGE_HEADER);

    /// Header name specifying the message compression algorithm (e.g., "gzip", "deflate").
    pub const ENCODING_HEADER: &'static str = "grpc-encoding";

    /// Static `HeaderName` for `grpc-encoding`.
    pub const ENCODING_NAME: HeaderName = HeaderName::from_static(Self::ENCODING_HEADER);

    /// Header name advertising supported compression algorithms to the peer.
    pub const ACCEPT_ENCODING_HEADER: &'static str = "grpc-accept-encoding";

    /// Static `HeaderName` for `grpc-accept-encoding`.
    pub const ACCEPT_ENCODING_NAME: HeaderName =
        HeaderName::from_static(Self::ACCEPT_ENCODING_HEADER);

    /// Header name for specifying RPC call timeout / deadline (e.g., "10S", "500m").
    pub const TIMEOUT_HEADER: &'static str = "grpc-timeout";

    /// Static `HeaderName` for `grpc-timeout`.
    pub const TIMEOUT_NAME: HeaderName = HeaderName::from_static(Self::TIMEOUT_HEADER);

    /// Header name for HTTP/2 TE header ("te").
    pub const TE_HEADER: &'static str = "te";

    /// Required value for gRPC TE header indicating support for trailers ("trailers").
    pub const TE_TRAILERS: &'static str = "trailers";

    /// Static `HeaderValue` for `te: trailers`.
    pub const TE_TRAILERS_VALUE: HeaderValue = HeaderValue::from_static(Self::TE_TRAILERS);
}
