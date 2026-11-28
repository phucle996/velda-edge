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

    /// Correlation ID header propagated across gRPC HTTP/2 streams ("x-request-id").
    pub const REQUEST_ID_HEADER: &'static str = "x-request-id";

    /// Static `HeaderName` for `x-request-id`.
    pub const REQUEST_ID_NAME: HeaderName = HeaderName::from_static(Self::REQUEST_ID_HEADER);

    /// Parses a `grpc-timeout` header value into a standard [`std::time::Duration`].
    ///
    /// Adheres to the official [gRPC over HTTP/2 Specification](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
    /// `TimeoutValue` is 1..8 decimal digits followed by a single unit char:
    /// - `H`: Hours
    /// - `M`: Minutes
    /// - `S`: Seconds
    /// - `m`: Milliseconds
    /// - `u`: Microseconds
    /// - `n`: Nanoseconds
    pub fn parse_grpc_timeout(val: &str) -> Option<std::time::Duration> {
        let val = val.trim();
        if val.is_empty() || val.len() > 9 {
            return None;
        }
        let (num_part, unit_part) = val.split_at(val.len() - 1);
        let num: u64 = num_part.parse().ok()?;
        let unit = unit_part.chars().next()?;
        match unit {
            'H' => Some(std::time::Duration::from_secs(num.checked_mul(3600)?)),
            'M' => Some(std::time::Duration::from_secs(num.checked_mul(60)?)),
            'S' => Some(std::time::Duration::from_secs(num)),
            'm' => Some(std::time::Duration::from_millis(num)),
            'u' => Some(std::time::Duration::from_micros(num)),
            'n' => Some(std::time::Duration::from_nanos(num)),
            _ => None,
        }
    }

    /// Formats a [`std::time::Duration`] into standard `grpc-timeout` format (e.g. "500m" or "10S").
    pub fn format_grpc_timeout(duration: std::time::Duration) -> String {
        let millis = duration.as_millis();
        if millis == 0 {
            let nanos = duration.as_nanos();
            if nanos > 0 && nanos <= 99_999_999 {
                return format!("{nanos}n");
            }
            return "0m".to_string();
        }
        if millis.is_multiple_of(1000) {
            let secs = duration.as_secs();
            if secs <= 99_999_999 {
                return format!("{secs}S");
            }
        }
        if millis <= 99_999_999 {
            format!("{millis}m")
        } else {
            let secs = duration.as_secs();
            format!("{secs}S")
        }
    }

    /// Transforms a request [`velda_core::HeaderWorkspace`] in-place into gRPC Trailers-Only
    /// rejection headers, retaining only bypass headers (e.g. `x-request-id`, `x-ratelimit-*`)
    /// before stamping `content-type: application/grpc`, `grpc-status`, and optional `grpc-message`.
    #[cold]
    pub fn prepare_rejection_headers(
        headers: &mut velda_core::HeaderWorkspace,
        status: crate::GrpcStatus,
        message: Option<&str>,
    ) {
        use velda_core::HeaderKey;

        const KEY_CONTENT_TYPE: HeaderKey = HeaderKey::from_static("content-type");
        const KEY_GRPC_STATUS: HeaderKey = HeaderKey::from_static(GrpcWire::STATUS_HEADER);
        const KEY_GRPC_MESSAGE: HeaderKey = HeaderKey::from_static(GrpcWire::MESSAGE_HEADER);

        headers.retain_bypass_only();
        headers.insert_prehashed(&KEY_CONTENT_TYPE, Self::CONTENT_TYPE_VALUE);
        headers.insert_prehashed(&KEY_GRPC_STATUS, status.code_header_value());
        if let Some(msg) = message
            && let Ok(val) = HeaderValue::from_str(msg)
        {
            headers.insert_prehashed(&KEY_GRPC_MESSAGE, val);
        }
    }

    /// Maps an [`velda_core::InternalStatus`] into gRPC Trailers-Only metadata while preserving
    /// Bit 1 bypass headers (`x-request-id`, `x-velda-fault`, etc.), returning the resolved [`crate::GrpcStatus`].
    #[cold]
    pub fn prepare_status_headers(
        headers: &mut velda_core::HeaderWorkspace,
        internal: velda_core::InternalStatus,
    ) -> crate::GrpcStatus {
        let grpc_status = crate::GrpcStatus::from_internal_status(internal);
        let msg = crate::GrpcStatus::internal_status_message(internal);
        Self::prepare_rejection_headers(headers, grpc_status, Some(msg));
        grpc_status
    }
}

/// Standard correlation ID header propagated across gRPC HTTP/2 streams.
pub const X_REQUEST_ID: HeaderName = GrpcWire::REQUEST_ID_NAME;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_grpc_timeout_parsing_and_formatting() {
        assert_eq!(
            GrpcWire::parse_grpc_timeout("10S"),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            GrpcWire::parse_grpc_timeout("500m"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            GrpcWire::parse_grpc_timeout("2H"),
            Some(Duration::from_secs(7200))
        );
        assert_eq!(
            GrpcWire::parse_grpc_timeout("15M"),
            Some(Duration::from_secs(900))
        );
        assert_eq!(
            GrpcWire::parse_grpc_timeout("100u"),
            Some(Duration::from_micros(100))
        );
        assert_eq!(
            GrpcWire::parse_grpc_timeout("50n"),
            Some(Duration::from_nanos(50))
        );

        // Invalid or malformed
        assert_eq!(GrpcWire::parse_grpc_timeout(""), None);
        assert_eq!(GrpcWire::parse_grpc_timeout("invalid"), None);
        assert_eq!(GrpcWire::parse_grpc_timeout("100X"), None);

        // Formatting
        assert_eq!(
            GrpcWire::format_grpc_timeout(Duration::from_millis(500)),
            "500m"
        );
        assert_eq!(
            GrpcWire::format_grpc_timeout(Duration::from_secs(10)),
            "10S"
        );
    }

    #[test]
    fn test_prepare_rejection_headers_retains_bypass_and_stamps_grpc_trailers() {
        let mut ws = velda_core::HeaderWorkspace::new(32);
        let req_id_str = ws.request_id().to_str().unwrap().to_string();
        ws.insert(
            http::header::HOST,
            HeaderValue::from_static("grpc.example.com"),
        );
        ws.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("grpc-go/1.64"),
        );

        GrpcWire::prepare_rejection_headers(
            &mut ws,
            crate::GrpcStatus::ResourceExhausted,
            Some("quota exceeded"),
        );

        assert!(!ws.contains_key(http::header::HOST));
        assert!(!ws.contains_key(http::header::USER_AGENT));
        assert_eq!(
            ws.get(&velda_core::HeaderWorkspace::X_REQUEST_ID).unwrap(),
            req_id_str.as_str()
        );
        assert_eq!(
            ws.get(http::header::CONTENT_TYPE).unwrap(),
            "application/grpc"
        );
        assert_eq!(ws.get("grpc-status").unwrap(), "8");
        assert_eq!(ws.get("grpc-message").unwrap(), "quota exceeded");
    }
}
