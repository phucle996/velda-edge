//! Dirty, extreme, and malicious boundary inputs testing for velda-core.
//!
//! Validates how velda-core handles unexpected socket addresses, extreme integer
//! limits, huge or empty payloads, repeated state transitions, and unusual URI paths.

use std::error::Error as StdError;
use std::net::SocketAddr;

use bytes::Bytes;
use http::{HeaderMap, Method, Uri, Version};
use velda_core::l4::request::{ConnectionId, TransportProtocol};
use velda_core::l7::request::Body;
use velda_core::{
    ConnectionContext, Error, ErrorKind, L4Request, L7Request, RequestId, RequestState, RouteId,
    UpstreamId,
};

#[test]
fn test_extreme_integer_boundaries() {
    // 0 values
    let zero_req = RequestId::new(0);
    let zero_route = RouteId::new(0);
    let zero_upstream = UpstreamId::new(0);
    let zero_conn = ConnectionId(0);

    assert_eq!(zero_req.value(), 0);
    assert_eq!(zero_route.value(), 0);
    assert_eq!(zero_upstream.value(), 0);
    assert_eq!(zero_conn.0, 0);

    // Maximum possible bounds
    let max_req = RequestId::new(u64::MAX);
    let max_route = RouteId::new(u32::MAX);
    let max_upstream = UpstreamId::new(u32::MAX);
    let max_conn = ConnectionId(u64::MAX);

    assert_eq!(max_req.value(), u64::MAX);
    assert_eq!(max_route.value(), u32::MAX);
    assert_eq!(max_upstream.value(), u32::MAX);
    assert_eq!(max_conn.0, u64::MAX);

    // Formatter checks for extreme numbers
    assert_eq!(max_req.to_string(), u64::MAX.to_string());
    assert_eq!(max_route.to_string(), u32::MAX.to_string());
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
            ConnectionId(1),
            TransportProtocol::Tcp,
            addr,
            "127.0.0.1:80".parse().unwrap(),
        );

        assert_eq!(l4.client_ip().to_string(), expected_ip);
        assert_eq!(l4.client_port(), expected_port);

        let conn = ConnectionContext::new(l4);
        assert_eq!(conn.client_ip().to_string(), expected_ip);
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
    let mut state = RequestState::new(RequestId(100));

    // Overwriting route multiple times (e.g. internal redirects / rewrites)
    state.set_route(RouteId(1));
    assert_eq!(state.route, Some(RouteId(1)));

    state.set_route(RouteId(2));
    assert_eq!(state.route, Some(RouteId(2)));

    // Setting upstream before or after routing
    state.set_upstream(UpstreamId(10));
    assert_eq!(state.upstream, Some(UpstreamId(10)));

    state.set_upstream(UpstreamId(20));
    assert_eq!(state.upstream, Some(UpstreamId(20)));

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
