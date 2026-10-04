use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use http::header::{ACCEPT, CONTENT_TYPE, USER_AGENT};
use http::{Method, StatusCode, Uri};
use quinn_proto::{ClientConfig, Dir, Endpoint, EndpointConfig, Event, ServerConfig, StreamEvent};
use velda_core::{Body, L7Response};
use velda_http3::Http3Engine;
use velda_http3::frame::{Http3Frame, decode_frame, decode_varint_slice, encode_frame};
use velda_http3::qpack::{decode_qpack, encode_qpack_request};

fn generate_test_crypto() -> (ServerConfig, ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
    );

    let mut rustls_server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(cert_chain, private_key)
    .unwrap();
    rustls_server.alpn_protocols = vec![b"h3".to_vec()];

    let quic_server_crypto =
        quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server)).unwrap();
    let server_config = ServerConfig::with_crypto(Arc::new(quic_server_crypto));

    // Client crypto accepting self-signed cert
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();

    let mut rustls_client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth();
    rustls_client.alpn_protocols = vec![b"h3".to_vec()];

    let quic_client_crypto =
        quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client)).unwrap();
    let client_config = ClientConfig::new(Arc::new(quic_client_crypto));

    (server_config, client_config)
}

#[test]
fn test_http3_frame_roundtrip() {
    let frame = Http3Frame::Data(Bytes::from_static(b"quic datagram payload"));
    let mut buf = bytes::BytesMut::new();
    encode_frame(&frame, &mut buf);

    let decoded = decode_frame(&mut buf).unwrap().unwrap();
    assert_eq!(decoded, frame);
    assert!(buf.is_empty());
}

#[test]
fn test_http3_engine_lifecycle() {
    let (server_config, _) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    assert_eq!(engine.connection_count(), 0);

    let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let (outgoing, requests) =
        engine.handle_datagram(Instant::now(), peer, None, b"invalid-udp-packet");

    assert_eq!(requests.len(), 0);
    let _ = outgoing;
}

#[test]
fn test_http3_full_request_response_and_leak_free() {
    let (server_config, client_config) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    let server_addr: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:55432".parse().unwrap();

    // Client endpoint
    let mut client_endpoint = Endpoint::new(Arc::new(EndpointConfig::default()), None, false, None);

    let mut now = Instant::now();
    let (_conn_handle, mut client_conn) = client_endpoint
        .connect(now, client_config, server_addr, "localhost")
        .unwrap();

    let mut client_transmit_buf = Vec::with_capacity(65535);

    // Helper: exchange packets until quiet
    let pump_packets = |engine: &mut Http3Engine,
                        client_conn: &mut quinn_proto::Connection,
                        client_endpoint: &mut Endpoint,
                        client_transmit_buf: &mut Vec<u8>,
                        now: Instant| {
        let mut server_requests = Vec::new();

        for _ in 0..10 {
            let mut progressed = false;

            // 1. Drain client packets to server
            client_transmit_buf.clear();
            while let Some(transmit) = client_conn.poll_transmit(now, 1, client_transmit_buf) {
                if transmit.size > 0 {
                    progressed = true;
                    let (server_pkts, reqs) = engine.handle_datagram(
                        now,
                        client_addr,
                        Some(server_addr.ip()),
                        &client_transmit_buf[..transmit.size],
                    );
                    server_requests.extend(reqs);

                    // Route server pkts back to client
                    for spkt in server_pkts {
                        let mut resp_buf = Vec::new();
                        let payload = BytesMut::from(&spkt.payload[..]);
                        if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ev)) =
                            client_endpoint.handle(
                                now,
                                server_addr,
                                None,
                                None,
                                payload,
                                &mut resp_buf,
                            )
                        {
                            client_conn.handle_event(ev);
                        }
                    }
                }
                client_transmit_buf.clear();
            }

            if !progressed {
                break;
            }
        }

        server_requests
    };

    // 1. Complete handshake
    let _ = pump_packets(
        &mut engine,
        &mut client_conn,
        &mut client_endpoint,
        &mut client_transmit_buf,
        now,
    );
    assert_eq!(engine.connection_count(), 1);

    // 2. Client sends HTTP/3 POST Request with QPACK headers and body
    let stream_id = client_conn.streams().open(Dir::Bi).unwrap();

    let mut req_headers = http::HeaderMap::new();
    req_headers.insert(USER_AGENT, "velda-client/1.0".parse().unwrap());
    req_headers.insert(ACCEPT, "application/json".parse().unwrap());
    req_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

    let method = Method::POST;
    let uri: Uri = "https://localhost/api/v1/orders".parse().unwrap();

    let mut qpack_buf = BytesMut::new();
    encode_qpack_request(&method, &uri, &req_headers, &mut qpack_buf);

    let header_frame = Http3Frame::Headers(qpack_buf.freeze());
    let body_bytes = Bytes::from_static(b"{\"item\": \"rust-edge\", \"quantity\": 42}");
    let data_frame = Http3Frame::Data(body_bytes.clone());

    let mut req_frames = BytesMut::new();
    encode_frame(&header_frame, &mut req_frames);
    encode_frame(&data_frame, &mut req_frames);

    client_conn
        .send_stream(stream_id)
        .write(&req_frames)
        .unwrap();
    client_conn.send_stream(stream_id).finish().unwrap();

    now += std::time::Duration::from_millis(10);

    // 3. Drive packets to server
    let reqs = pump_packets(
        &mut engine,
        &mut client_conn,
        &mut client_endpoint,
        &mut client_transmit_buf,
        now,
    );

    assert_eq!(reqs.len(), 1, "Server should receive exactly 1 request");
    let req_event = &reqs[0];
    assert_eq!(req_event.request.method, Method::POST);
    assert_eq!(req_event.request.path(), "/api/v1/orders");
    assert_eq!(
        req_event.request.headers.get("user-agent").unwrap(),
        "velda-client/1.0"
    );
    assert_eq!(
        req_event.request.headers.get("content-type").unwrap(),
        "application/json"
    );

    // Body validation
    match req_event.request.body {
        Body::Bytes(ref b) => assert_eq!(b, &body_bytes),
        _ => panic!("Expected Body::Bytes"),
    }

    // 4. Server responds with 200 OK and response headers
    let resp = L7Response::from_bytes(StatusCode::OK, b"{\"status\": \"confirmed\"}".to_vec())
        .with_header(
            CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );

    let outgoing = engine
        .send_response(now, req_event.handle, req_event.stream_id, &resp)
        .unwrap();

    for spkt in outgoing {
        let mut resp_buf = Vec::new();
        let payload = BytesMut::from(&spkt.payload[..]);
        if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ev)) =
            client_endpoint.handle(now, server_addr, None, None, payload, &mut resp_buf)
        {
            client_conn.handle_event(ev);
        }
    }

    // 5. Client reads response
    let mut client_stream_buf = BytesMut::new();
    let mut uni_streams = Vec::new();
    while let Some(event) = client_conn.poll() {
        if let Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) = event
            && let Some(id) = client_conn.streams().accept(Dir::Uni)
        {
            uni_streams.push(id);
        }
        if let Event::Stream(StreamEvent::Readable { id }) = event
            && id == stream_id
            && let Ok(mut chunks) = client_conn.recv_stream(id).read(true)
        {
            while let Some(chunk) = chunks.next(65535).ok().flatten() {
                client_stream_buf.extend_from_slice(&chunk.bytes);
            }
        }
    }

    let decoded_resp_frame = decode_frame(&mut client_stream_buf).unwrap().unwrap();
    match decoded_resp_frame {
        Http3Frame::Headers(payload) => {
            let decoded = decode_qpack(&payload).unwrap();
            assert_eq!(decoded.status, Some(StatusCode::OK));
            assert_eq!(
                decoded.headers.get("content-type").unwrap(),
                "application/json"
            );
        }
        _ => panic!("Expected HEADERS frame"),
    }

    let decoded_body_frame = decode_frame(&mut client_stream_buf).unwrap().unwrap();
    match decoded_body_frame {
        Http3Frame::Data(payload) => {
            assert_eq!(&payload[..], b"{\"status\": \"confirmed\"}");
        }
        _ => panic!("Expected DATA frame"),
    }

    // 6. Zero Memory Leak Invariant Verification:
    // Once the request is dispatched and completed, all stream buffers must be reclaimed!
    assert_eq!(
        engine.active_stream_count(),
        0,
        "Stream buffers must be completely reclaimed to prevent memory leaks"
    );
}

#[test]
fn test_http3_rfc9114_server_control_stream_settings() {
    let (server_config, client_config) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    let server_addr: SocketAddr = "127.0.0.1:4433".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:55432".parse().unwrap();

    let mut client_endpoint = Endpoint::new(Arc::new(EndpointConfig::default()), None, false, None);

    let now = Instant::now();
    let (_conn_handle, mut client_conn) = client_endpoint
        .connect(now, client_config, server_addr, "localhost")
        .unwrap();

    let mut client_transmit_buf = Vec::with_capacity(65535);

    // Exchange packets until settled
    for _ in 0..20 {
        let mut progressed = false;
        client_transmit_buf.clear();
        while let Some(transmit) = client_conn.poll_transmit(now, 1, &mut client_transmit_buf) {
            if transmit.size > 0 {
                progressed = true;
                let (server_pkts, _) = engine.handle_datagram(
                    now,
                    client_addr,
                    Some(server_addr.ip()),
                    &client_transmit_buf[..transmit.size],
                );
                for spkt in server_pkts {
                    let mut resp_buf = Vec::new();
                    let payload = BytesMut::from(&spkt.payload[..]);
                    if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ev)) =
                        client_endpoint.handle(now, server_addr, None, None, payload, &mut resp_buf)
                    {
                        client_conn.handle_event(ev);
                    }
                }
            }
            client_transmit_buf.clear();
        }
        if !progressed {
            break;
        }
    }

    // Client verifies that a unidirectional stream with Stream Type 0x00 (Control)
    // and a SETTINGS frame (Type 0x04) was received from the server!
    let mut found_control_stream = false;
    let mut found_settings_frame = false;

    let mut uni_streams = Vec::new();
    while let Some(event) = client_conn.poll() {
        if let Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) = event
            && let Some(id) = client_conn.streams().accept(Dir::Uni)
        {
            uni_streams.push(id);
        }
        if let Event::Stream(StreamEvent::Readable { id }) = event
            && id.dir() == Dir::Uni
        {
            let mut buf = BytesMut::new();
            if let Ok(mut chunks) = client_conn.recv_stream(id).read(true) {
                while let Some(chunk) = chunks.next(65535).ok().flatten() {
                    buf.extend_from_slice(&chunk.bytes);
                }
            }

            if !buf.is_empty() {
                // Check Stream Type varint
                let (stream_type, type_len) = decode_varint_slice(&buf).unwrap();
                if stream_type == 0x00 {
                    found_control_stream = true;
                    let mut frame_buf = BytesMut::from(&buf[type_len..]);
                    if let Ok(Some(Http3Frame::Settings(settings))) = decode_frame(&mut frame_buf) {
                        found_settings_frame = true;
                        assert!(
                            !settings.is_empty(),
                            "Settings frame must contain parameters"
                        );
                    }
                }
            }
        }
    }

    for id in uni_streams {
        let mut buf = BytesMut::new();
        if let Ok(mut chunks) = client_conn.recv_stream(id).read(true) {
            while let Some(chunk) = chunks.next(65535).ok().flatten() {
                buf.extend_from_slice(&chunk.bytes);
            }
        }
        if !buf.is_empty() {
            let (stream_type, type_len) = decode_varint_slice(&buf).unwrap();
            if stream_type == 0x00 {
                found_control_stream = true;
                let mut frame_buf = BytesMut::from(&buf[type_len..]);
                if let Ok(Some(Http3Frame::Settings(settings))) = decode_frame(&mut frame_buf) {
                    found_settings_frame = true;
                    assert!(
                        !settings.is_empty(),
                        "Settings frame must contain parameters"
                    );
                }
            }
        }
    }
    assert!(
        found_control_stream,
        "Server MUST open a unidirectional Control stream (RFC 9114 Section 6.2.1)"
    );
    assert!(
        found_settings_frame,
        "Server MUST send a SETTINGS frame as the first frame on the control stream (RFC 9114 Section 6.2.1)"
    );
}

#[tokio::test]
async fn test_http3_client_multiplexed_concurrent_requests() {
    let (server_config, _) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    let server_socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let server_addr = server_socket.local_addr().unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let s_socket = server_socket.clone();
    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = s_socket.recv_from(&mut buf) => {
                    let (len, remote) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let now = Instant::now();
                    let (outgoing, requests) = engine.handle_datagram(now, remote, None, &buf[..len]);
                    for out in outgoing {
                        let _ = s_socket.send_to(&out.payload, out.peer).await;
                    }
                    for req_event in requests {
                        let resp_body = match req_event.request.body {
                            Body::Bytes(ref b) => format!("echo: {}", String::from_utf8_lossy(b)),
                            _ => format!("echo: {}", req_event.request.uri),
                        };
                        let resp = L7Response::new(
                            StatusCode::OK,
                            http::Version::HTTP_3,
                            http::HeaderMap::new(),
                            Body::Bytes(Bytes::from(resp_body)),
                        );
                        if let Ok(resp_outgoing) = engine.send_response(now, req_event.handle, req_event.stream_id, &resp) {
                            for out in resp_outgoing {
                                let _ = s_socket.send_to(&out.payload, out.peer).await;
                            }
                        }
                    }
                }
            }
        }
    });

    let config = velda_http3::Http3Config::auto();
    let client = velda_http3::connect(server_addr, "localhost", &config)
        .await
        .expect("Client failed to connect to HTTP/3 server");

    assert_eq!(client.target(), server_addr);

    // Send 10 concurrent requests across multiple tasks sharing the single client
    let mut handles = Vec::new();
    for i in 0..10 {
        let client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            let req = velda_core::L7Request {
                method: Method::POST,
                uri: format!("/path-{}", i).parse::<Uri>().unwrap(),
                version: http::Version::HTTP_3,
                headers: http::HeaderMap::new(),
                body: Body::Bytes(Bytes::from(format!("request-payload-{}", i))),
            };
            let resp = client_clone
                .send_request(req)
                .await
                .expect("Request failed");
            assert_eq!(resp.status, StatusCode::OK);
            assert_eq!(resp.version, http::Version::HTTP_3);
            if let Body::Bytes(b) = resp.body {
                assert_eq!(
                    String::from_utf8_lossy(&b),
                    format!("echo: request-payload-{}", i)
                );
            } else {
                panic!("Expected Bytes body");
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    client.close().await;

    // Test forward_request single-shot helper against the same server
    let single_req = velda_core::L7Request {
        method: Method::GET,
        uri: "/single-shot".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: http::HeaderMap::new(),
        body: Body::Empty,
    };
    let single_resp = velda_http3::forward_request(&single_req, server_addr)
        .await
        .expect("Single-shot forward_request failed");
    assert_eq!(single_resp.status, StatusCode::OK);
    if let Body::Bytes(b) = single_resp.body {
        assert_eq!(String::from_utf8_lossy(&b), "echo: /single-shot");
    } else {
        panic!(
            "Expected Bytes body from single_resp, got: {:?}",
            single_resp.body
        );
    }

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test]
async fn test_http3_client_cancellation_and_recovery() {
    let (server_config, _) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    let server_socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let server_addr = server_socket.local_addr().unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let s_socket = server_socket.clone();
    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = s_socket.recv_from(&mut buf) => {
                    let (len, remote) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let now = Instant::now();
                    let (outgoing, requests) = engine.handle_datagram(now, remote, None, &buf[..len]);
                    for out in outgoing {
                        let _ = s_socket.send_to(&out.payload, out.peer).await;
                    }
                    for req_event in requests {
                        let resp = L7Response::new(
                            StatusCode::OK,
                            http::Version::HTTP_3,
                            http::HeaderMap::new(),
                            Body::Bytes(Bytes::from_static(b"normal-response")),
                        );
                        if let Ok(resp_outgoing) = engine.send_response(now, req_event.handle, req_event.stream_id, &resp) {
                            for out in resp_outgoing {
                                let _ = s_socket.send_to(&out.payload, out.peer).await;
                            }
                        }
                    }
                }
            }
        }
    });

    let config = velda_http3::Http3Config::auto();
    let client = velda_http3::connect(server_addr, "localhost", &config)
        .await
        .expect("Client failed to connect to HTTP/3 server");

    assert!(!client.is_closed());

    // 1. Drop a request task mid-flight to trigger client-side stream cancellation (0x010c)
    {
        let client_clone = client.clone();
        let cancel_task = tokio::spawn(async move {
            let req = velda_core::L7Request {
                method: Method::GET,
                uri: "/slow-path".parse::<Uri>().unwrap(),
                version: http::Version::HTTP_3,
                headers: http::HeaderMap::new(),
                body: Body::Empty,
            };
            let _ = client_clone.send_request(req).await;
        });
        // Abort task immediately before response is processed
        cancel_task.abort();
    }

    // Give driver loop time to sweep cancelled stream
    tokio::time::sleep(Duration::from_millis(30)).await;

    // 2. Connection remains healthy and multiplexes subsequent requests seamlessly (testing both by-value and by-ref)
    let req = velda_core::L7Request {
        method: Method::GET,
        uri: "/after-cancel".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: http::HeaderMap::new(),
        body: Body::Empty,
    };
    let resp = client
        .send_request_ref(&req)
        .await
        .expect("Subsequent request (by-ref) must succeed after cancellation");
    assert_eq!(resp.status, StatusCode::OK);

    let resp2 = client
        .send_request(req)
        .await
        .expect("Subsequent request (by-value) must succeed after cancellation");
    assert_eq!(resp2.status, StatusCode::OK);
    if let Body::Bytes(b) = resp.body {
        assert_eq!(&b[..], b"normal-response");
    } else {
        panic!("Expected Bytes body");
    }

    client.close().await;
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test]
async fn test_http3_post_request_with_body_roundtrip() {
    let (server_config, _) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    let server_socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let server_addr = server_socket.local_addr().unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let s_socket = server_socket.clone();
    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = s_socket.recv_from(&mut buf) => {
                    let (len, remote) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let now = Instant::now();
                    let (outgoing, requests) = engine.handle_datagram(now, remote, None, &buf[..len]);
                    for out in outgoing {
                        let _ = s_socket.send_to(&out.payload, out.peer).await;
                    }
                    for req_event in requests {
                        let received_body = match req_event.request.body {
                            Body::Bytes(b) => b,
                            Body::Empty => Bytes::new(),
                        };
                        let mut resp_body = Vec::from(b"echo: ");
                        resp_body.extend_from_slice(&received_body);

                        let resp = L7Response::new(
                            StatusCode::OK,
                            http::Version::HTTP_3,
                            http::HeaderMap::new(),
                            Body::Bytes(Bytes::from(resp_body)),
                        );
                        if let Ok(resp_outgoing) = engine.send_response(now, req_event.handle, req_event.stream_id, &resp) {
                            for out in resp_outgoing {
                                let _ = s_socket.send_to(&out.payload, out.peer).await;
                            }
                        }
                    }
                }
            }
        }
    });

    let config = velda_http3::Http3Config::auto();
    let client = velda_http3::connect(server_addr, "localhost", &config)
        .await
        .expect("Client failed to connect to HTTP/3 server");

    let payload = b"hello http3 bidirectional payload verification - 100% loss-free stream";
    let post_req = velda_core::L7Request {
        method: Method::POST,
        uri: "/submit-data".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
            h
        },
        body: Body::Bytes(Bytes::from_static(payload)),
    };

    let resp = client
        .send_request(post_req)
        .await
        .expect("POST request must succeed");

    assert_eq!(resp.status, StatusCode::OK);
    if let Body::Bytes(b) = resp.body {
        let expected = format!("echo: {}", std::str::from_utf8(payload).unwrap());
        assert_eq!(String::from_utf8_lossy(&b), expected);
    } else {
        panic!("Expected Bytes body from POST response");
    }

    client.close().await;
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[test]
fn test_http3_adversarial_malformed_datagram_flood() {
    let (server_config, _) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));

    let remote: SocketAddr = "192.0.2.1:54321".parse().unwrap();

    // Flood engine with 5,000 corrupted / fuzz datagrams
    for i in 0..5_000 {
        let mut corrupted = vec![0u8; (i % 512) + 1];
        for (idx, b) in corrupted.iter_mut().enumerate() {
            *b = ((i * 31 + idx * 17) & 0xff) as u8;
        }
        let now = Instant::now();
        let (outgoing, requests) = engine.handle_datagram(now, remote, None, &corrupted);
        assert_eq!(requests.len(), 0);
        let _ = outgoing;
    }

    assert_eq!(engine.connection_count(), 0);
    assert_eq!(engine.active_stream_count(), 0);
}

#[tokio::test]
async fn test_http3_adversarial_oversized_payload_bomb_rejected() {
    let (server_config, _) = generate_test_crypto();
    // Configure server with small max_body_size = 512 bytes
    let mut s_cfg = velda_http3::Http3Config::auto();
    s_cfg.max_body_size = 512;
    let mut engine = Http3Engine::new_with_config(Arc::new(server_config), s_cfg);

    let server_socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let server_addr = server_socket.local_addr().unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let s_socket = server_socket.clone();
    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = s_socket.recv_from(&mut buf) => {
                    let (len, remote) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let now = Instant::now();
                    let (outgoing, requests) = engine.handle_datagram(now, remote, None, &buf[..len]);
                    for out in outgoing {
                        let _ = s_socket.send_to(&out.payload, out.peer).await;
                    }
                    for req_event in requests {
                        let resp = L7Response::new(
                            StatusCode::OK,
                            http::Version::HTTP_3,
                            http::HeaderMap::new(),
                            Body::Bytes(Bytes::from_static(b"normal-ok")),
                        );
                        if let Ok(resp_outgoing) = engine.send_response(now, req_event.handle, req_event.stream_id, &resp) {
                            for out in resp_outgoing {
                                let _ = s_socket.send_to(&out.payload, out.peer).await;
                            }
                        }
                    }
                }
            }
        }
    });

    let client_cfg = velda_http3::Http3Config::auto();
    let client = velda_http3::connect(server_addr, "localhost", &client_cfg)
        .await
        .expect("Client failed to connect to HTTP/3 server");

    // 1. Send oversized bomb payload (128 KB, exceeding max_body_size + 65536)
    let bomb_payload = vec![0x42u8; 128 * 1024];
    let bomb_req = velda_core::L7Request {
        method: Method::POST,
        uri: "/payload-bomb".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: http::HeaderMap::new(),
        body: Body::Bytes(Bytes::from(bomb_payload)),
    };

    let bomb_result = client.send_request(bomb_req).await;
    // Client MUST receive an error (stream stopped / reset or flow window limit)
    assert!(bomb_result.is_err(), "Oversized bomb must not succeed");

    // 2. Subsequent legitimate request over same connection must succeed seamlessly
    let valid_req = velda_core::L7Request {
        method: Method::GET,
        uri: "/legitimate".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: http::HeaderMap::new(),
        body: Body::Empty,
    };
    let valid_resp = client
        .send_request(valid_req)
        .await
        .expect("Subsequent request must succeed after bomb rejection");
    assert_eq!(valid_resp.status, StatusCode::OK);

    client.close().await;
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
