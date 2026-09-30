use bytes::BytesMut;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::net::TcpListener;
use velda_core::{Body, IngressLimits, L7Request, L7Response};
use velda_http1::composer_parse::Http1ServerConnection;
use velda_http1::error::Http1Error;
use velda_http1::upstream_connector::Http1UpstreamConnector;
use velda_http1::{decode_request, decode_response, encode_response};

const TEST_LIMITS: IngressLimits = IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000);

#[tokio::test]
async fn test_http1_server_connection() {
    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, TEST_LIMITS);

    tokio::spawn(async move {
        client
            .write_all(b"GET /index.html HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
    });

    let req = conn.next_request().await.unwrap().unwrap();
    assert_eq!(req.method, Method::GET);
    assert_eq!(req.path(), "/index.html");
}

#[tokio::test]
async fn test_http1_upstream_connector() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let n = sock.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("GET /health"));
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            .await
            .unwrap();
    });

    let req = L7Request::new(
        Method::GET,
        Uri::from_static("http://localhost/health"),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Empty,
    );

    let resp = Http1UpstreamConnector::forward_request(&req, backend_addr, &TEST_LIMITS)
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn test_chunked_request_decoding() {
    let raw = b"POST /upload HTTP/1.1\r\nHost: edge.velda.io\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n6\r\npedia \r\n9\r\nin chunks\r\n0\r\n\r\n";
    let mut buf = BytesMut::from(&raw[..]);

    let req = decode_request(&mut buf, &TEST_LIMITS)
        .unwrap()
        .expect("request parsed");
    assert_eq!(req.method, Method::POST);
    assert_eq!(req.path(), "/upload");
    assert_eq!(req.body.len(), 19);
    assert_eq!(
        req.body,
        Body::Bytes(bytes::Bytes::from_static(b"Wikipedia in chunks"))
    );
    assert!(buf.is_empty(), "all wire bytes should be consumed");
}

#[test]
fn test_chunked_response_decoding() {
    let raw = b"HTTP/1.1 200 OK\r\nServer: backend-svc\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n";
    let mut buf = BytesMut::from(&raw[..]);

    let resp = decode_response(&mut buf, &TEST_LIMITS)
        .unwrap()
        .expect("response parsed");
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.len(), 11);
    assert_eq!(
        resp.body,
        Body::Bytes(bytes::Bytes::from_static(b"Hello World"))
    );
    assert!(buf.is_empty());
}

#[test]
fn test_chunked_incomplete_buffering() {
    // Missing the terminal 0\r\n\r\n chunk
    let raw = b"POST /stream HTTP/1.1\r\nHost: edge.velda.io\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n";
    let mut buf = BytesMut::from(&raw[..]);

    let res = decode_request(&mut buf, &TEST_LIMITS).unwrap();
    assert!(
        res.is_none(),
        "incomplete chunked stream must return Ok(None)"
    );
    assert_eq!(
        buf.len(),
        raw.len(),
        "incomplete stream must not advance read buffer"
    );
}

#[test]
fn test_smuggling_simultaneous_cl_te() {
    let raw = b"POST / HTTP/1.1\r\nHost: victim.com\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    let mut buf = BytesMut::from(&raw[..]);

    let err = decode_request(&mut buf, &TEST_LIMITS).unwrap_err();
    assert!(
        matches!(err, Http1Error::SmugglingDetected(_)),
        "Must fast-fail reject simultaneous CL and TE"
    );
}

#[test]
fn test_smuggling_multiple_conflicting_cl() {
    let raw = b"POST / HTTP/1.1\r\nHost: victim.com\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\n12345";
    let mut buf = BytesMut::from(&raw[..]);

    let err = decode_request(&mut buf, &TEST_LIMITS).unwrap_err();
    assert!(
        matches!(err, Http1Error::SmugglingDetected(_)),
        "Must reject conflicting multiple Content-Length headers"
    );
}

#[test]
fn test_header_too_large_rejection() {
    // Header section exceeding max_header_size (64KB) without \r\n\r\n
    let mut raw = Vec::from(&b"GET / HTTP/1.1\r\nHost: edge.velda.io\r\nX-Spam: "[..]);
    raw.resize(70 * 1024, b'a');
    let mut buf = BytesMut::from(&raw[..]);

    let err = decode_request(&mut buf, &TEST_LIMITS).unwrap_err();
    assert!(
        matches!(err, Http1Error::HeaderTooLarge(_)),
        "Headers > 64KB must be rejected with HeaderTooLarge"
    );
}

#[test]
fn test_payload_too_large_rejection() {
    // Content-Length claims 15 MB (exceeds default max_body_size = 10 MB)
    let raw = b"POST /data HTTP/1.1\r\nHost: edge.velda.io\r\nContent-Length: 15728640\r\n\r\n";
    let mut buf = BytesMut::from(&raw[..]);

    let err = decode_request(&mut buf, &TEST_LIMITS).unwrap_err();
    assert!(
        matches!(err, Http1Error::PayloadTooLarge(_)),
        "Payload > 10MB must be rejected with PayloadTooLarge"
    );
}

#[test]
fn test_too_many_headers_rejection() {
    let limits = TEST_LIMITS.with_max_headers(5);
    let mut raw = Vec::from(&b"GET / HTTP/1.1\r\nHost: localhost\r\n"[..]);
    for i in 0..10 {
        raw.extend_from_slice(format!("X-Header-{i}: value\r\n").as_bytes());
    }
    raw.extend_from_slice(b"\r\n");
    let mut buf = BytesMut::from(&raw[..]);

    let err = decode_request(&mut buf, &limits).unwrap_err();
    assert!(
        matches!(err, Http1Error::TooManyHeaders(5)),
        "Must reject when header count exceeds max_headers"
    );
}

#[tokio::test]
async fn test_http10_close_by_default() {
    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, TEST_LIMITS);

    tokio::spawn(async move {
        // Plain HTTP/1.0 without keep-alive
        client
            .write_all(b"GET /legacy HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
    });

    let req = conn.next_request().await.unwrap().unwrap();
    assert_eq!(req.version, Version::HTTP_10);
    assert!(
        conn.is_closed(),
        "HTTP/1.0 without keep-alive must set is_closed = true"
    );
}

#[tokio::test]
async fn test_http10_keep_alive_negotiated() {
    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, TEST_LIMITS);

    tokio::spawn(async move {
        // HTTP/1.0 with explicit Connection: keep-alive
        client
            .write_all(b"GET /legacy HTTP/1.0\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
    });

    let req = conn.next_request().await.unwrap().unwrap();
    assert_eq!(req.version, Version::HTTP_10);
    assert!(
        !conn.is_closed(),
        "HTTP/1.0 with keep-alive must keep connection open"
    );
}

#[test]
fn test_http10_response_encoding() {
    let resp = L7Response::new(
        StatusCode::OK,
        Version::HTTP_10,
        HeaderMap::new(),
        Body::Bytes(bytes::Bytes::from_static(b"OK")),
    );
    let mut dst = BytesMut::new();
    encode_response(&resp, &mut dst);

    let output = String::from_utf8_lossy(&dst);
    assert!(
        output.starts_with("HTTP/1.0 200 OK\r\n"),
        "HTTP/1.0 response must serialize with HTTP/1.0 status line"
    );
}

#[tokio::test]
async fn test_custom_ingress_limits() {
    // Custom limit: max body size only 50 bytes, max header size only 100 bytes
    let custom_limits = TEST_LIMITS.with_max_body_size(50).with_max_header_size(100);

    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, custom_limits);
    assert_eq!(conn.limits().max_body_size, 50);
    assert_eq!(conn.limits().max_header_size, 100);

    tokio::spawn(async move {
        // Send a request with Content-Length 100 (> 50 limit)
        client
            .write_all(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n")
            .await
            .unwrap();
    });

    let err = conn.next_request().await.unwrap_err();
    assert!(
        matches!(err, Http1Error::PayloadTooLarge(100)),
        "Must enforce custom IngressLimits on connection"
    );
}
