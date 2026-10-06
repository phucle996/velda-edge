use bytes::Bytes;
use http::{Method, StatusCode};
use tokio::io::duplex;
use tokio::net::TcpListener;
use velda_core::{Body, MemoryTier};
use velda_http2::client::{self, Http2Response};
use velda_http2::config::Http2Config;
use velda_http2::error::Http2Error;
use velda_http2::server::Http2ServerConnection;

const TEST_CONFIG: Http2Config = Http2Config::for_tier(MemoryTier::Medium);

#[tokio::test]
async fn test_http2_server_and_client_roundtrip() {
    let (client_io, server_io) = duplex(64 * 1024);

    tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let req = http::Request::builder()
            .method("POST")
            .uri("https://example.com/echo")
            .body(())
            .unwrap();

        let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
        send_stream
            .send_data(Bytes::from_static(b"hello h2"), true)
            .unwrap();

        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        let chunk = body.data().await.unwrap().unwrap();
        assert_eq!(&chunk[..], b"hello h2 echo");
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();
    let (req, responder) = server_conn.accept_request().await.unwrap().unwrap();

    assert_eq!(req.method, Method::POST);
    assert_eq!(req.path(), "/echo");
    if let Body::Bytes(ref b) = req.body {
        assert_eq!(&b[..], b"hello h2");
    } else {
        panic!("expected body bytes");
    }

    let resp = velda_core::L7Response::from_bytes(StatusCode::OK, b"hello h2 echo".to_vec());
    responder.send_response(&resp).unwrap();
}

#[tokio::test]
async fn test_http2_multiplexing_concurrent_streams() {
    let (client_io, server_io) = duplex(128 * 1024);

    // Client spawns 4 concurrent streams simultaneously over the same H2 connection
    tokio::spawn(async move {
        let (client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let mut join_set = tokio::task::JoinSet::new();

        for i in 0..4 {
            let mut client = client.clone();
            join_set.spawn(async move {
                let req = http::Request::builder()
                    .method("GET")
                    .uri(format!("https://example.com/stream/{i}"))
                    .body(())
                    .unwrap();

                let (resp_fut, _) = client.send_request(req, true).unwrap();
                let (parts, mut body) = resp_fut.await.unwrap().into_parts();
                assert_eq!(parts.status, StatusCode::OK);
                let chunk = body.data().await.unwrap().unwrap();
                assert_eq!(&chunk[..], format!("response-{i}").as_bytes());
            });
        }

        while let Some(res) = join_set.join_next().await {
            res.unwrap();
        }
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    // Server accepts 4 concurrent streams and processes each in an independent task
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let (h2_req, responder) = server_conn.accept_h2_request().await.unwrap().unwrap();
        tasks.spawn(async move {
            let path = h2_req.head.path().to_string();
            let id = path.trim_start_matches("/stream/");
            let resp_bytes = format!("response-{id}").into_bytes();
            let resp = Http2Response::from_bytes(StatusCode::OK, resp_bytes);
            responder.send_h2_response(&resp).unwrap();
        });
    }

    while let Some(res) = tasks.join_next().await {
        res.unwrap();
    }
}

#[tokio::test]
async fn test_http2_payload_limit_enforcement() {
    let (client_io, server_io) = duplex(64 * 1024);

    tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let req = http::Request::builder()
            .method("POST")
            .uri("https://example.com/upload")
            .body(())
            .unwrap();

        let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
        // Send 100 bytes
        let data = Bytes::from(vec![b'A'; 100]);
        send_stream.send_data(data, true).unwrap();
        let _ = resp_fut.await;
    });

    // Limit maximum body size to 50 bytes
    let tight_config = Http2Config::for_tier(MemoryTier::Medium).with_max_body_size(50);
    let mut server_conn = Http2ServerConnection::handshake(server_io, tight_config)
        .await
        .unwrap();

    let err = server_conn.accept_h2_request().await.unwrap_err();
    match err {
        Http2Error::PayloadTooLarge(size) => {
            assert!(size > 50);
        }
        other => panic!("expected PayloadTooLarge, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_http2_upstream_connector() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut server = h2::server::handshake(sock).await.unwrap();
        while let Some(res) = server.accept().await {
            let (req, mut respond) = res.unwrap();
            assert_eq!(req.method(), Method::GET);
            assert_eq!(req.uri().path(), "/health");
            let resp = http::Response::builder().status(200).body(()).unwrap();
            let mut send = respond.send_response(resp, false).unwrap();
            send.send_data(Bytes::from_static(b"h2 alive"), true)
                .unwrap();
        }
    });

    let config = TEST_CONFIG.with_max_concurrent_streams(512);

    let mut client = client::connect(backend_addr, None, &config, None, None)
        .await
        .unwrap();
    let http_req = http::Request::builder()
        .method("GET")
        .uri("http://localhost/health")
        .body(())
        .unwrap();
    let (resp_fut, _) = client.send_request(http_req, true).unwrap();
    let (parts, mut body) = resp_fut.await.unwrap().into_parts();

    assert_eq!(parts.status, StatusCode::OK);
    let chunk = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk[..], b"h2 alive");
}

#[tokio::test]
async fn test_http2_server_streaming_sse() {
    let (client_io, server_io) = duplex(64 * 1024);

    // Client requests SSE stream and receives 3 progressive events
    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let req = http::Request::builder()
            .method("GET")
            .uri("https://example.com/events")
            .header("accept", "text/event-stream")
            .body(())
            .unwrap();

        let (resp_fut, _) = client.send_request(req, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        assert_eq!(
            parts.headers.get("content-type").unwrap(),
            "text/event-stream"
        );

        let mut received_events = Vec::new();
        while let Some(chunk_res) = body.data().await {
            let chunk = chunk_res.unwrap();
            let len = chunk.len();
            if !chunk.is_empty() {
                received_events.push(String::from_utf8(chunk.to_vec()).unwrap());
            }
            let _ = body.flow_control().release_capacity(len);
        }

        assert_eq!(received_events.len(), 3);
        assert_eq!(received_events[0], "data: token-1\n\n");
        assert_eq!(received_events[1], "data: token-2\n\n");
        assert_eq!(received_events[2], "data: token-3\n\n");
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    let server_task = tokio::spawn(async move {
        let (_req, responder) = server_conn.accept_h2_request().await.unwrap().unwrap();
        let mut sse_headers = http::HeaderMap::new();
        sse_headers.insert("content-type", "text/event-stream".parse().unwrap());
        sse_headers.insert("cache-control", "no-cache".parse().unwrap());

        let mut stream_sender = responder
            .send_stream_response(StatusCode::OK, &sse_headers)
            .unwrap();

        let stream_task = tokio::spawn(async move {
            stream_sender
                .send_chunk(Bytes::from("data: token-1\n\n"))
                .await
                .unwrap();
            stream_sender
                .send_chunk(Bytes::from("data: token-2\n\n"))
                .await
                .unwrap();
            stream_sender
                .send_chunk(Bytes::from("data: token-3\n\n"))
                .await
                .unwrap();
            stream_sender.finish().unwrap();
        });

        // Drive the connection while the stream transmits
        while let Ok(Some(_)) = server_conn.accept_h2_request().await {}
        stream_task.await.unwrap();
    });

    client_task.await.unwrap();
    server_task.abort();
}

#[tokio::test]
async fn test_http2_client_streaming_upload() {
    let (client_io, server_io) = duplex(64 * 1024);

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let req = http::Request::builder()
            .method("POST")
            .uri("https://example.com/upload")
            .header("content-type", "application/octet-stream")
            .body(())
            .unwrap();

        let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();

        send_stream
            .send_data(Bytes::from("part-1;"), false)
            .unwrap();
        send_stream
            .send_data(Bytes::from("part-2;"), false)
            .unwrap();
        send_stream.send_data(Bytes::from("part-3;"), true).unwrap();

        let (parts, _) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::CREATED);
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    let server_task = tokio::spawn(async move {
        let (head, mut receiver, responder) = server_conn
            .accept_streaming_request()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(head.method, Method::POST);
        let recv_task = tokio::spawn(async move {
            let mut accumulated = Vec::new();
            while let Some(chunk) = receiver.recv_chunk().await.unwrap() {
                accumulated.extend_from_slice(&chunk);
            }

            assert_eq!(
                String::from_utf8(accumulated).unwrap(),
                "part-1;part-2;part-3;"
            );

            let resp = Http2Response::empty(StatusCode::CREATED);
            responder.send_h2_response(&resp).unwrap();
        });

        // Drive the connection while chunks arrive
        while let Ok(Some(_)) = server_conn.accept_h2_request().await {}
        recv_task.await.unwrap();
    });

    client_task.await.unwrap();
    server_task.abort();
}

#[test]
fn test_http2_pipe_strategy_mapping() {
    use velda_core::StreamingMode;
    use velda_http2::pipe::Http2PipeStrategy;

    assert_eq!(
        Http2PipeStrategy::from_streaming(StreamingMode::DISABLED),
        Http2PipeStrategy::Buffered
    );
    assert_eq!(
        Http2PipeStrategy::from_streaming(StreamingMode::SERVER),
        Http2PipeStrategy::ServerStream
    );
    assert_eq!(
        Http2PipeStrategy::from_streaming(StreamingMode::CLIENT),
        Http2PipeStrategy::ClientStream
    );
    assert_eq!(
        Http2PipeStrategy::from_streaming(StreamingMode::DUPLEX),
        Http2PipeStrategy::Duplex
    );
}

#[tokio::test]
async fn test_empty_body_response() {
    let (client_io, server_io) = duplex(64 * 1024);

    tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let req = http::Request::builder()
            .method("GET")
            .uri("https://example.com/empty")
            .body(())
            .unwrap();

        let (resp_fut, _) = client.send_request(req, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        assert!(body.data().await.is_none());
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();
    let (_req, responder) = server_conn.accept_request().await.unwrap().unwrap();
    let resp = velda_core::L7Response::from_bytes(StatusCode::OK, vec![]);
    responder.send_response(&resp).unwrap();
}

#[tokio::test]
async fn test_head_and_no_body_status_invariants() {
    let (client_io, server_io) = duplex(64 * 1024);

    tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // 1. HEAD request
        let req_head = http::Request::builder()
            .method("HEAD")
            .uri("https://example.com/item")
            .body(())
            .unwrap();
        let (resp_fut, _) = client.send_request(req_head, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        assert_eq!(parts.headers.get("content-length").unwrap(), "1024");
        // Must receive END_STREAM immediately without DATA frame
        assert!(body.data().await.is_none());

        // 2. 204 No Content response
        let req_204 = http::Request::builder()
            .method("DELETE")
            .uri("https://example.com/item")
            .body(())
            .unwrap();
        let (resp_fut, _) = client.send_request(req_204, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::NO_CONTENT);
        assert!(body.data().await.is_none());

        // 3. 304 Not Modified response
        let req_304 = http::Request::builder()
            .method("GET")
            .uri("https://example.com/item")
            .body(())
            .unwrap();
        let (resp_fut, _) = client.send_request(req_304, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, StatusCode::NOT_MODIFIED);
        assert!(body.data().await.is_none());
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    // 1. Handle HEAD: send response with content-length but Body::Empty
    let (_req, responder) = server_conn.accept_request().await.unwrap().unwrap();
    let resp = velda_core::L7Response::new(
        StatusCode::OK,
        http::Version::HTTP_2,
        {
            let mut h = http::HeaderMap::new();
            h.insert("content-length", "1024".parse().unwrap());
            h
        },
        Body::Empty,
    );
    responder.send_response(&resp).unwrap();

    // 2. Handle 204: send response with 204 status even if Body::Bytes is provided
    let (_req, responder) = server_conn.accept_request().await.unwrap().unwrap();
    let resp =
        velda_core::L7Response::from_bytes(StatusCode::NO_CONTENT, b"must-be-ignored".to_vec());
    responder.send_response(&resp).unwrap();

    // 3. Handle 304: send response with 304 status
    let (_req, responder) = server_conn.accept_request().await.unwrap().unwrap();
    let resp = velda_core::L7Response::new(
        StatusCode::NOT_MODIFIED,
        http::Version::HTTP_2,
        http::HeaderMap::new(),
        Body::Empty,
    );
    responder.send_response(&resp).unwrap();
}
