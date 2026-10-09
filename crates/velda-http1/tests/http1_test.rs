use bytes::BytesMut;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::net::TcpListener;
use velda_core::Body;
use velda_core::hardware::MemoryTier;
use velda_http1::error::Http1Error;
use velda_http1::{
    Http1BodyFraming, Http1Config, Http1ServerConnection, Http1ServerRequest, Http1ServerResponse,
    Http1ServerResponseHead, decode_body, decode_request, decode_request_head, decode_response,
    decode_response_head, encode_response, forward_request,
};

const TEST_CONFIG: Http1Config = Http1Config::for_tier(MemoryTier::Medium);

#[tokio::test]
async fn test_http1_server_connection() {
    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, TEST_CONFIG);

    tokio::spawn(async move {
        client
            .write_all(b"GET /index.html HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
    });

    let req = conn.next_request().await.unwrap().unwrap();
    assert_eq!(req.method, Method::GET);
    assert_eq!(req.uri.path(), "/index.html");
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

    let req = Http1ServerRequest::new(
        Method::GET,
        Uri::from_static("http://localhost/health"),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Empty,
    );

    let mut stream = tokio::net::TcpStream::connect(backend_addr).await.unwrap();
    let resp = forward_request(&req, &mut stream, &TEST_CONFIG)
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn test_chunked_request_decoding() {
    let raw = b"POST /upload HTTP/1.1\r\nHost: edge.velda.io\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n6\r\npedia \r\n9\r\nin chunks\r\n0\r\n\r\n";
    let mut buf = BytesMut::from(&raw[..]);

    let req = decode_request(&mut buf, &TEST_CONFIG)
        .unwrap()
        .expect("request parsed");
    assert_eq!(req.method, Method::POST);
    assert_eq!(req.uri.path(), "/upload");
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

    let resp = decode_response(&mut buf, &TEST_CONFIG)
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

    let res = decode_request(&mut buf, &TEST_CONFIG).unwrap();
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

    let err = decode_request(&mut buf, &TEST_CONFIG).unwrap_err();
    assert!(
        matches!(err, Http1Error::SmugglingDetected(_)),
        "Must fast-fail reject simultaneous CL and TE"
    );
}

#[test]
fn test_smuggling_multiple_conflicting_cl() {
    let raw = b"POST / HTTP/1.1\r\nHost: victim.com\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\n12345";
    let mut buf = BytesMut::from(&raw[..]);

    let err = decode_request(&mut buf, &TEST_CONFIG).unwrap_err();
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

    let err = decode_request(&mut buf, &TEST_CONFIG).unwrap_err();
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

    let err = decode_request(&mut buf, &TEST_CONFIG).unwrap_err();
    assert!(
        matches!(err, Http1Error::PayloadTooLarge(_)),
        "Payload > 10MB must be rejected with PayloadTooLarge"
    );
}

#[test]
fn test_too_many_headers_rejection() {
    let limits = TEST_CONFIG.with_max_headers(5);
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
    let mut conn = Http1ServerConnection::new(server, TEST_CONFIG);

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
    let mut conn = Http1ServerConnection::new(server, TEST_CONFIG);

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
    let resp = Http1ServerResponse::new(
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
    let custom_cfg = TEST_CONFIG.with_max_body_size(50).with_max_header_size(100);

    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, custom_cfg);
    assert_eq!(conn.config().max_body_size, 50);
    assert_eq!(conn.config().max_header_size, 100);

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
        "Must enforce custom limits on connection"
    );
}

#[tokio::test]
async fn test_phased_server_connection_head_and_body() {
    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server, TEST_CONFIG);

    tokio::spawn(async move {
        client
            .write_all(
                b"POST /api/v1/data HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nHELLO",
            )
            .await
            .unwrap();

        let mut resp_buf = [0u8; 1024];
        let n = client.read(&mut resp_buf).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp_buf[..n]);
        assert!(resp_str.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp_str.contains("WORLD"));
    });

    // Phase 1: next_request_head
    let (head, framing) = conn.next_request_head().await.unwrap().unwrap();
    assert_eq!(head.method, Method::POST);
    assert_eq!(head.uri.path(), "/api/v1/data");
    assert_eq!(framing, Http1BodyFraming::ContentLength(5));

    // Phase 2: read_body
    let body = conn.read_body(framing).await.unwrap();
    assert_eq!(body.len(), 5);

    // Egress: send response parts
    let resp_head =
        Http1ServerResponseHead::new(StatusCode::OK, Version::HTTP_11, HeaderMap::new());
    let resp_body = Body::Bytes(bytes::Bytes::from_static(b"WORLD"));
    conn.send_response_parts(&resp_head, &resp_body)
        .await
        .unwrap();
}

#[test]
fn test_phased_decode_request_head_and_body() {
    let mut buf = BytesMut::from(
        &b"GET /search?q=rust HTTP/1.1\r\nHost: example.com\r\nContent-Length: 4\r\n\r\nTEST"[..],
    );

    let (head, framing) = decode_request_head(&mut buf, &TEST_CONFIG)
        .unwrap()
        .unwrap();
    assert_eq!(head.method, Method::GET);
    assert_eq!(head.uri.path(), "/search");
    assert_eq!(framing, Http1BodyFraming::ContentLength(4));

    // Buffer now contains only body
    assert_eq!(&buf[..], b"TEST");

    let body = decode_body(&mut buf, framing, &TEST_CONFIG)
        .unwrap()
        .unwrap();
    if let Body::Bytes(b) = body {
        assert_eq!(&b[..], b"TEST");
    } else {
        panic!("Expected Body::Bytes");
    }
    assert!(buf.is_empty());
}

#[tokio::test]
async fn test_progressive_chunked_server_streaming() {
    let (mut client, server) = duplex(4096);
    let mut conn = Http1ServerConnection::new(server, TEST_CONFIG);

    tokio::spawn(async move {
        let (head, _framing) = conn.next_request_head().await.unwrap().unwrap();
        assert_eq!(head.method, Method::GET);

        let resp_head =
            Http1ServerResponseHead::new(StatusCode::OK, Version::HTTP_11, HeaderMap::new());
        conn.send_response_head_chunked(&resp_head).await.unwrap();

        conn.send_chunk(b"data: first token\n\n").await.unwrap();
        conn.send_chunk(b"data: second token\n\n").await.unwrap();
        conn.send_chunked_end().await.unwrap();
    });

    client
        .write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut read_buf = BytesMut::with_capacity(4096);
    let (head, framing) = loop {
        if let Some(parts) = decode_response_head(&mut read_buf, &TEST_CONFIG).unwrap() {
            break parts;
        }
        let n = client.read_buf(&mut read_buf).await.unwrap();
        assert!(n > 0);
    };

    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(framing, Http1BodyFraming::Chunked);

    // Read first chunk
    let chunk1 = loop {
        if let Some(chunk) = velda_http1::parse_single_chunk(&read_buf).unwrap() {
            let (wire_len, payload, is_term) = chunk;
            assert!(!is_term);
            let data = bytes::Bytes::copy_from_slice(payload);
            use bytes::Buf;
            read_buf.advance(wire_len);
            break data;
        }
        let n = client.read_buf(&mut read_buf).await.unwrap();
        assert!(n > 0);
    };
    assert_eq!(&chunk1[..], b"data: first token\n\n");

    // Read second chunk
    let chunk2 = loop {
        if let Some(chunk) = velda_http1::parse_single_chunk(&read_buf).unwrap() {
            let (wire_len, payload, is_term) = chunk;
            assert!(!is_term);
            let data = bytes::Bytes::copy_from_slice(payload);
            use bytes::Buf;
            read_buf.advance(wire_len);
            break data;
        }
        let n = client.read_buf(&mut read_buf).await.unwrap();
        assert!(n > 0);
    };
    assert_eq!(&chunk2[..], b"data: second token\n\n");

    // Read terminal chunk
    let is_done = loop {
        if let Some(chunk) = velda_http1::parse_single_chunk(&read_buf).unwrap() {
            let (wire_len, _, is_term) = chunk;
            assert!(is_term);
            use bytes::Buf;
            read_buf.advance(wire_len);
            break true;
        }
        let n = client.read_buf(&mut read_buf).await.unwrap();
        if n == 0 {
            break false;
        }
    };
    assert!(is_done);
}

#[tokio::test]
async fn test_progressive_chunked_client_upload() {
    let (mut client, server) = duplex(4096);
    let mut conn = Http1ServerConnection::new(server, TEST_CONFIG);

    tokio::spawn(async move {
        client
            .write_all(
                b"POST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n",
            )
            .await
            .unwrap();

        client.write_all(b"5\r\nhello\r\n").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        client.write_all(b"6\r\nworld!\r\n").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        client.write_all(b"0\r\n\r\n").await.unwrap();
    });

    let (head, framing) = conn.next_request_head().await.unwrap().unwrap();
    assert_eq!(head.method, Method::POST);
    assert_eq!(framing, Http1BodyFraming::Chunked);

    let chunk1 = conn.read_next_chunk().await.unwrap().unwrap();
    assert_eq!(&chunk1[..], b"hello");

    let chunk2 = conn.read_next_chunk().await.unwrap().unwrap();
    assert_eq!(&chunk2[..], b"world!");

    let chunk_term = conn.read_next_chunk().await.unwrap();
    assert!(chunk_term.is_none());
}

#[test]
fn test_chunked_body_with_trailers() {
    let raw =
        b"5\r\nhello\r\n0\r\nExpires: Wed, 21 Oct 2026 07:28:00 GMT\r\nX-Checksum: 1234\r\n\r\n";
    let (wire_len, body) = velda_http1::parse_chunked_body(raw, 1024).unwrap().unwrap();
    assert_eq!(wire_len, raw.len());
    let bytes = match body {
        velda_core::Body::Bytes(b) => b,
        _ => panic!("expected bytes body"),
    };
    assert_eq!(&bytes[..], b"hello");

    // Also test progressive parsing of terminal chunk with trailers
    let term_raw = b"0\r\nExpires: Wed, 21 Oct 2026 07:28:00 GMT\r\n\r\n";
    let (term_len, payload, is_term) = velda_http1::parse_single_chunk(term_raw).unwrap().unwrap();
    assert!(is_term);
    assert_eq!(payload.len(), 0);
    assert_eq!(term_len, term_raw.len());
}

#[tokio::test]
async fn test_http1_missing_and_duplicate_host_validation() {
    let (mut client, server_stream) = tokio::io::duplex(1024);
    let mut conn = Http1ServerConnection::new(server_stream, Http1Config::auto());

    // 1. Missing Host header in HTTP/1.1 MUST be rejected with 400
    client
        .write_all(b"GET /index.html HTTP/1.1\r\nUser-Agent: test\r\n\r\n")
        .await
        .unwrap();
    let res = conn.next_request_head().await;
    assert!(res.is_err());

    // 2. Duplicate Host header in HTTP/1.1 MUST be rejected as smuggling attempt
    let (mut client2, server_stream2) = tokio::io::duplex(1024);
    let mut conn2 = Http1ServerConnection::new(server_stream2, Http1Config::auto());
    client2
        .write_all(b"GET /index.html HTTP/1.1\r\nHost: legitimate.com\r\nHost: evil.com\r\n\r\n")
        .await
        .unwrap();
    let res2 = conn2.next_request_head().await;
    assert!(res2.is_err());
}

#[test]
fn test_chunk_size_inner_whitespace_rejected() {
    // Inner whitespace in chunk size must be rejected (RFC 9112 Section 7.1)
    let raw = b"1 0\r\n1234567890123456\r\n0\r\n\r\n";
    let res = velda_http1::parse_chunked_body(raw, 1024);
    assert!(res.is_err());
}

#[test]
fn test_sanitize_proxy_connection_and_unlimited_nominated() {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::HeaderName::from_static("proxy-connection"),
        http::HeaderValue::from_static("keep-alive"),
    );
    headers.append(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close, x-h1, x-h2, x-h3, x-h4"),
    );
    headers.append(
        http::header::CONNECTION,
        http::HeaderValue::from_static("x-h5, x-h6, x-h7, x-h8, x-h9, x-h10"),
    );
    for i in 1..=10 {
        let name = http::header::HeaderName::from_bytes(format!("x-h{i}").as_bytes()).unwrap();
        headers.insert(name, http::HeaderValue::from_static("secret"));
    }

    velda_http1::sanitize_headers(&mut headers);
    assert!(!headers.contains_key("proxy-connection"));
    assert!(!headers.contains_key("connection"));
    for i in 1..=10 {
        assert!(
            !headers.contains_key(format!("x-h{i}").as_str()),
            "x-h{i} should have been stripped"
        );
    }
}

#[test]
fn test_decode_response_skips_1xx_informational() {
    let mut buf = BytesMut::from(
        "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
    );
    let resp = decode_response(&mut buf, &TEST_CONFIG).unwrap().unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body, Body::Bytes(bytes::Bytes::from_static(b"hello")));
}

#[test]
fn test_encode_response_head_force_close() {
    let headers = HeaderMap::new();
    let mut dst = BytesMut::new();
    velda_http1::encode_response_head(
        Version::HTTP_11,
        StatusCode::OK,
        &headers,
        Some(10),
        true,
        &mut dst,
    );
    let str_val = std::str::from_utf8(&dst).unwrap();
    assert!(str_val.contains("connection: close\r\n"));
    assert!(str_val.contains("content-length: 10\r\n"));
}

#[test]
fn test_expect_100_continue_detection() {
    let raw = b"POST /upload HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue\r\nContent-Length: 10\r\n\r\n";
    let mut buf = BytesMut::from(&raw[..]);
    let (head, framing) = decode_request_head(&mut buf, &TEST_CONFIG)
        .unwrap()
        .unwrap();
    assert!(head.is_expect_100_continue());
    assert_eq!(framing, Http1BodyFraming::ContentLength(10));
}

#[test]
fn test_chunk_header_line_exceeding_4kb_rejected() {
    let mut bad_chunk = Vec::new();
    bad_chunk.extend_from_slice(b"1;ext=");
    bad_chunk.resize(5000, b'a'); // 5KB chunk line without CRLF

    let res = velda_http1::wire::parse_chunked_body(&bad_chunk, 1024 * 1024);
    assert!(res.is_err(), "Must reject chunk header line exceeding 4KB");

    let res_single = velda_http1::wire::parse_single_chunk(&bad_chunk);
    assert!(
        res_single.is_err(),
        "parse_single_chunk must reject line > 4KB"
    );
}

#[test]
fn test_enrich_headers_overwrites_forwarded_headers() {
    let mut headers = HeaderMap::new();
    let uri = http::Uri::from_static("http://example.com/api");
    let peer: std::net::SocketAddr = "1.2.3.4:12345".parse().unwrap();
    let local: std::net::SocketAddr = "10.0.0.1:80".parse().unwrap();

    headers.insert(
        http::header::HeaderName::from_static("x-forwarded-for"),
        http::HeaderValue::from_static("9.9.9.9"),
    );
    headers.insert(
        http::header::HeaderName::from_static("x-real-ip"),
        http::HeaderValue::from_static("8.8.8.8"),
    );
    headers.insert(
        http::header::HeaderName::from_static("x-forwarded-proto"),
        http::HeaderValue::from_static("fake-proto"),
    );

    velda_http1::enrich_headers(&mut headers, &uri, peer, local, false);

    assert_eq!(headers.get("x-forwarded-for").unwrap(), "1.2.3.4");
    assert_eq!(headers.get("x-real-ip").unwrap(), "1.2.3.4");
    assert_eq!(headers.get("x-forwarded-proto").unwrap(), "http");
    assert_eq!(headers.get("x-forwarded-port").unwrap(), "80");
}

#[tokio::test]
async fn test_pipe_buffered_rejects_premature_upstream_eof() {
    let (client_down, server_down) = tokio::io::duplex(4096);
    let (client_up, mut server_up) = tokio::io::duplex(4096);

    let mut conn = Http1ServerConnection::new(server_down, TEST_CONFIG);
    let req = Http1ServerRequest::new(
        http::Method::GET,
        http::Uri::from_static("/test"),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Empty,
    );

    let upstream_task = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 1024];
        let _ = server_up.read(&mut buf).await.unwrap();
        // Respond with Content-Length: 100, but only send 10 bytes and close socket!
        server_up
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
            .await
            .unwrap();
        drop(server_up);
    });

    let mut up_stream = client_up;
    let mut req = req;
    let res = velda_http1::pipe_buffered(&mut conn, &mut req, &mut up_stream, &TEST_CONFIG).await;
    let _ = upstream_task.await;
    drop(client_down);

    assert!(
        res.is_err(),
        "pipe_buffered must return error on premature upstream EOF"
    );
}
