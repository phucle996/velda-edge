//! Downstream gRPC Path Parsing and Security Validation over UDP (RFC 9114 / gRPC-over-HTTP3).

use http::Uri;

use crate::error::GrpcError;

/// Parses and validates a gRPC request URI path into `(service, method)` components over UDP.
///
/// Invariants:
/// - Path MUST start with `/` (e.g. `/helloworld.Greeter/SayHello`).
/// - Path MUST contain exactly one separating `/` between service and method (`/{service}/{method}`).
/// - Service and Method components MUST NOT be empty.
/// - Rejects directory traversals (`..`), backslashes (`\`), and null bytes (`\0`).
#[inline]
pub fn parse_grpc_path(path: &str) -> Result<(&str, &str), GrpcError> {
    let bytes = path.as_bytes();
    if bytes.is_empty() || bytes[0] != b'/' {
        return Err(GrpcError::Protocol(
            "gRPC path must start with leading slash '/'".into(),
        ));
    }

    if bytes.contains(&b'\0') || bytes.contains(&b'\\') {
        return Err(GrpcError::Protocol(
            "Prohibited character in gRPC path".into(),
        ));
    }

    // Strip leading slash
    let remainder = &path[1..];
    let Some(slash_idx) = remainder.find('/') else {
        return Err(GrpcError::Protocol(
            "gRPC path must contain both service and method (/{service}/{method})".into(),
        ));
    };

    let service = &remainder[..slash_idx];
    let method = &remainder[slash_idx + 1..];

    if service.is_empty() {
        return Err(GrpcError::Protocol(
            "gRPC service name cannot be empty".into(),
        ));
    }

    if method.is_empty() || method.contains('/') {
        return Err(GrpcError::Protocol(
            "gRPC method name cannot be empty or contain additional slashes".into(),
        ));
    }

    if service == ".." || method == ".." || service.contains("..") || method.contains("..") {
        return Err(GrpcError::Protocol(
            "Path traversal prohibited in gRPC path".into(),
        ));
    }

    Ok((service, method))
}

/// Validates that a URI has a well-formed gRPC path according to the gRPC specification over UDP.
#[inline]
pub fn validate_grpc_path(uri: &Uri) -> Result<(&str, &str), GrpcError> {
    parse_grpc_path(uri.path())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_grpc_udp_path() {
        let (service, method) =
            parse_grpc_path("/helloworld.Greeter/SayHello").expect("valid path");
        assert_eq!(service, "helloworld.Greeter");
        assert_eq!(method, "SayHello");
    }

    #[test]
    fn test_parse_grpc_udp_path_rejects_missing_leading_slash() {
        assert!(parse_grpc_path("helloworld.Greeter/SayHello").is_err());
    }

    #[test]
    fn test_parse_grpc_udp_path_rejects_single_segment() {
        assert!(parse_grpc_path("/helloworld.Greeter").is_err());
    }

    #[test]
    fn test_parse_grpc_udp_path_rejects_extra_slashes() {
        assert!(parse_grpc_path("/service/method/extra").is_err());
    }

    #[test]
    fn test_parse_grpc_udp_path_rejects_empty_components() {
        assert!(parse_grpc_path("//SayHello").is_err());
        assert!(parse_grpc_path("/service/").is_err());
    }

    #[test]
    fn test_parse_grpc_udp_path_rejects_null_byte_and_traversal() {
        assert!(parse_grpc_path("/service\0/method").is_err());
        assert!(parse_grpc_path("/../method").is_err());
        assert!(parse_grpc_path("/service/..").is_err());
    }
}
