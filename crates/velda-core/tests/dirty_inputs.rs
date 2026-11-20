//! Dirty, extreme, and malicious boundary inputs testing for velda-core.
//!
//! Validates how velda-core handles unexpected socket addresses, extreme integer
//! limits, huge or empty payloads, repeated state transitions, and unusual URI paths.

use std::error::Error as StdError;
use std::net::SocketAddr;

use bytes::Bytes;
use http::{HeaderMap, Method, Uri, Version};
use velda_core::l4::request::TransportProtocol;
use velda_core::l7::request::Body;
use velda_core::{Error, ErrorKind, L4Request, L7Request, RequestState};

#[test]
fn test_extreme_integer_boundaries() {
    // 0 values
    let zero_req: u128 = 0;
    let zero_route: u16 = 0;
    let zero_upstream: u16 = 0;
    let zero_conn: u64 = 0;

    assert_eq!(zero_req, 0);
    assert_eq!(zero_route, 0);
    assert_eq!(zero_upstream, 0);
    assert_eq!(zero_conn, 0);

    // Maximum possible bounds
    let max_req: u128 = u128::MAX;
    let max_route: u16 = u16::MAX;
    let max_upstream: u16 = u16::MAX;
    let max_conn: u64 = u64::MAX;

    assert_eq!(max_req, u128::MAX);
    assert_eq!(max_route, u16::MAX);
    assert_eq!(max_upstream, u16::MAX);
    assert_eq!(max_conn, u64::MAX);

    // Formatter checks for extreme numbers
    assert_eq!(
        format!("{max_req:032x}"),
        "ffffffffffffffffffffffffffffffff"
    );
    assert_eq!(max_route.to_string(), u16::MAX.to_string());
}

#[test]
fn test_dirty_and_extreme_socket_addresses() {
    let test_addrs = [
        // IPv4 boundary cases
        ("0.0.0.0:0", "0.0.0.0", 0),
        ("255.255.255.255:65535", "255.255.255.255", 65535),
        ("127.0.0.1:80", "127.0.0.1", 80),
        ("224.0.0.1:5353", "224.0.0.1", 5353),     // Multicast
        ("169.254.1.1:8080", "169.254.1.1", 8080), // Link-local
        // IPv6 boundary cases
        ("[::]:0", "::", 0),
        ("[::1]:443", "::1", 443),
        (
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            65535,
        ),
        ("[fe80::1]:80", "fe80::1", 80),
        ("[ff02::1]:1234", "ff02::1", 1234), // IPv6 Multicast
    ];

    for (raw_addr, expected_ip, expected_port) in test_addrs {
        let addr: SocketAddr = raw_addr.parse().expect("Valid socket address");
        let l4 = L4Request::new(
            1,
            TransportProtocol::Tcp,
            velda_core::lut::ip::encode_ip_u128(addr.ip()),
            addr.port().to_be(),
            velda_core::lut::ip::v4_to_u128(0x7F00_0001),
            80u16.to_be(),
        );

        let (buf, len) = velda_core::lut::ip::format_ip_bytes(l4.peer_ip);
        assert_eq!(std::str::from_utf8(&buf[..len]).unwrap(), expected_ip);
        assert_eq!(u16::from_be(l4.peer_port_be), expected_port);
        assert_eq!(l4.peer_ip, velda_core::lut::ip::encode_ip_u128(addr.ip()));
    }
}

#[test]
fn test_unusual_and_dirty_l7_requests() {
    // 1. Root and nested dirty paths
    let dirty_uris = [
        "/",
        "///double//slashes/",
        "/path/with/../traversal/attempt",
        "/path?query=1&evil=%3Cscript%3Ealert(1)%3C%2Fscript%3E&unicode=%F0%9F%A6%80",
        "/?",
        "/?empty_query=",
        "http://example.com:8080/absolute/form/uri",
    ];

    for uri_str in dirty_uris {
        let uri: Uri = uri_str.parse().expect("Parsed valid URI");
        let l7 = L7Request::new(
            Method::POST,
            uri.clone(),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        assert_eq!(l7.path(), uri.path());
        assert_eq!(l7.query(), uri.query());
        assert!(!l7.has_body());
    }

    // Malformed raw unencoded characters must be rejected by URI parser
    assert!("/path?<script>".parse::<Uri>().is_err());

    // 2. Large body payloads (1MB chunks)
    let large_bytes = Bytes::from(vec![0xAA; 1024 * 1024]);
    let l7_large = L7Request::new(
        Method::PUT,
        Uri::from_static("/upload"),
        Version::HTTP_2,
        HeaderMap::new(),
        Body::Bytes(large_bytes.clone()),
    );

    assert!(l7_large.has_body());
    assert!(l7_large.is_http2());
    assert!(!l7_large.is_http11());

    if let Body::Bytes(b) = &l7_large.body {
        assert_eq!(b.len(), 1024 * 1024);
        assert_eq!(b[0], 0xAA);
    } else {
        panic!("Expected Bytes body");
    }
}

#[test]
fn test_repeated_and_conflicting_state_mutations() {
    let mut state = RequestState::new(100);

    // Overwriting route multiple times (e.g. internal redirects / rewrites)
    state.set_route(1);
    assert_eq!(state.route_id, 1);

    state.set_route(2);
    assert_eq!(state.route_id, 2);

    // Setting upstream before or after routing
    state.set_upstream(10);
    assert_eq!(state.upstream_id, 10);

    state.set_upstream(20);
    assert_eq!(state.upstream_id, 20);

    // Out-of-order flag transitions
    state.mark_upstream_completed();
    assert!(state.upstream_completed);
    assert!(!state.upstream_started); // Completed marked without start (dirty sequence)

    state.mark_upstream_started();
    assert!(state.upstream_started);
    assert!(state.upstream_completed);
}

#[test]
fn test_error_construction_with_extreme_strings() {
    let kinds = [
        ErrorKind::InvalidRequest,
        ErrorKind::Protocol,
        ErrorKind::RouteNotFound,
        ErrorKind::RouteConfig,
        ErrorKind::UpstreamUnavailable,
        ErrorKind::UpstreamFailure,
        ErrorKind::Timeout,
        ErrorKind::Connection,
        ErrorKind::Rejected,
        ErrorKind::Canceled,
        ErrorKind::Internal,
    ];

    // 1. Empty message
    for kind in &kinds {
        let err = Error::new(*kind, "");
        assert_eq!(err.kind(), *kind);
        assert_eq!(err.message(), "");
    }

    // 2. Extremely large error message (64KB string)
    let huge_msg = "X".repeat(65536);
    let err = Error::new(ErrorKind::Internal, huge_msg.clone());
    assert_eq!(err.kind(), ErrorKind::Internal);
    assert_eq!(err.message().len(), 65536);

    // 3. Error with source
    let source_err = std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection reset by peer",
    );
    let wrap_err = Error::with_source(ErrorKind::Connection, "transport dropped", source_err);
    assert_eq!(wrap_err.kind(), ErrorKind::Connection);
    assert!(wrap_err.source().is_some());
}
