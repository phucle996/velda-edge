use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Method, StatusCode, Uri};
use std::time::Duration;
use tokio::net::TcpListener;
use velda_core::{Body, L7Request, MemoryTier};
use velda_grpc::GrpcConfig;
use velda_grpc::frame::{decode_grpc_frame, encode_grpc_frame};
use velda_grpc::status::GrpcStatus;
use velda_grpc::tcp::client::GrpcUpstreamConnector;
use velda_grpc::tcp::server::GrpcServerConnection;

/// Tests direct Unary RPC roundtrip between GrpcUpstreamConnector and GrpcServerConnection.
#[tokio::test]
async fn test_grpc_unary_roundtrip() {
    let config = GrpcConfig::for_tier(MemoryTier::Medium);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            assert_eq!(stream.parts.uri.path(), "/test.UnaryService/EchoUnary");
            assert_eq!(stream.parts.method, Method::POST);

            let msg = stream.read_unary_message(10 * 1024 * 1024).await.unwrap();
            assert_eq!(msg, Some(Bytes::from_static(b"hello_unary")));

            stream
                .respond
                .send_unary_response(GrpcStatus::Ok, Some(b"world_unary"), None)
                .unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let mut connector = GrpcUpstreamConnector::connect(server_addr, None, &test_config, None, None)
        .await
        .unwrap();

    let mut req_body = BytesMut::new();
    encode_grpc_frame(b"hello_unary", false, &mut req_body);

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/grpc".parse().unwrap());
    headers.insert("te", "trailers".parse().unwrap());

    let req = L7Request::new(
        Method::POST,
        Uri::from_static("http://localhost/test.UnaryService/EchoUnary"),
        http::Version::HTTP_2,
        headers,
        Body::Bytes(req_body.freeze()),
    );

    let resp = connector.invoke_unary(&req, &test_config).await.unwrap();

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.headers.get("grpc-status").unwrap(), "0");

    if let Body::Bytes(body_bytes) = resp.body {
        let mut buf = BytesMut::from(&body_bytes[..]);
        let (compressed, payload) = decode_grpc_frame(&mut buf).unwrap().unwrap();
        assert!(!compressed);
        assert_eq!(&payload[..], b"world_unary");
    } else {
        panic!("Expected binary body in response");
    }

    server_task.abort();
}

/// Tests direct Trailers-Only error response (e.g. NotFound) from server.
#[tokio::test]
async fn test_grpc_server_trailers_only_response() {
    let config = GrpcConfig::for_tier(MemoryTier::Medium);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            stream
                .respond
                .send_trailers_only(GrpcStatus::NotFound, Some("custom entity not found"))
                .unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let mut connector = GrpcUpstreamConnector::connect(server_addr, None, &test_config, None, None)
        .await
        .unwrap();

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/grpc".parse().unwrap());

    let req = L7Request::new(
        Method::POST,
        Uri::from_static("http://localhost/test.UnaryService/MissingEntity"),
        http::Version::HTTP_2,
        headers,
        Body::Empty,
    );

    let resp = connector.invoke_unary(&req, &test_config).await.unwrap();

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.headers.get("grpc-status").unwrap(), "5"); // NOT_FOUND = 5
    assert_eq!(
        resp.headers.get("grpc-message").unwrap(),
        "custom entity not found"
    );

    server_task.abort();
}

/// Tests direct Server-Streaming RPC roundtrip: server streams multiple chunks + trailers to client.
#[tokio::test]
async fn test_grpc_server_streaming_roundtrip() {
    let config = GrpcConfig::for_tier(MemoryTier::Medium);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();

            let mut send = stream.respond.send_response(resp, false).unwrap();

            let mut f1 = BytesMut::new();
            encode_grpc_frame(b"server_stream_chunk_1", false, &mut f1);
            send.send_data(f1.freeze(), false).unwrap();

            let mut f2 = BytesMut::new();
            encode_grpc_frame(b"server_stream_chunk_2", false, &mut f2);
            send.send_data(f2.freeze(), false).unwrap();

            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            trailers.insert("grpc-message", "Stream complete".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let mut connector = GrpcUpstreamConnector::connect(server_addr, None, &test_config, None, None)
        .await
        .unwrap();

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/test.StreamService/FetchEvents")
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();

    let (resp_fut, _) = connector.open_stream(req, true).unwrap();
    let resp = resp_fut.await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let (_, mut body) = resp.into_parts();

    let chunk1 = body.data().await.unwrap().unwrap();
    let mut b1 = BytesMut::from(&chunk1[..]);
    let (_, p1) = decode_grpc_frame(&mut b1).unwrap().unwrap();
    assert_eq!(&p1[..], b"server_stream_chunk_1");

    let chunk2 = body.data().await.unwrap().unwrap();
    let mut b2 = BytesMut::from(&chunk2[..]);
    let (_, p2) = decode_grpc_frame(&mut b2).unwrap().unwrap();
    assert_eq!(&p2[..], b"server_stream_chunk_2");

    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    assert_eq!(trailers.get("grpc-message").unwrap(), "Stream complete");

    server_task.abort();
}

/// Tests direct Client-Streaming RPC roundtrip: client sends multiple chunks, server aggregates.
#[tokio::test]
async fn test_grpc_client_streaming_roundtrip() {
    let config = GrpcConfig::for_tier(MemoryTier::Medium);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            let mut total_chunks = 0;
            while let Some(chunk_res) = stream.recv_stream.data().await {
                let chunk = chunk_res.unwrap();
                let len = chunk.len();
                let mut b = BytesMut::from(&chunk[..]);
                let (_, _) = decode_grpc_frame(&mut b).unwrap().unwrap();
                total_chunks += 1;
                let _ = stream.recv_stream.flow_control().release_capacity(len);
            }
            assert_eq!(total_chunks, 2);

            stream
                .respond
                .send_unary_response(
                    GrpcStatus::Ok,
                    Some(format!("received:{}", total_chunks).as_bytes()),
                    None,
                )
                .unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let mut connector = GrpcUpstreamConnector::connect(server_addr, None, &test_config, None, None)
        .await
        .unwrap();

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/test.ClientStream/Upload")
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();

    let (resp_fut, mut send_stream) = connector.open_stream(req, false).unwrap();

    let mut f1 = BytesMut::new();
    encode_grpc_frame(b"chunk_one", false, &mut f1);
    send_stream.send_data(f1.freeze(), false).unwrap();

    let mut f2 = BytesMut::new();
    encode_grpc_frame(b"chunk_two", false, &mut f2);
    send_stream.send_data(f2.freeze(), true).unwrap();

    let resp = resp_fut.await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let (_, mut body) = resp.into_parts();
    let data = body.data().await.unwrap().unwrap();
    let mut b = BytesMut::from(&data[..]);
    let (_, payload) = decode_grpc_frame(&mut b).unwrap().unwrap();
    assert_eq!(&payload[..], b"received:2");

    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");

    server_task.abort();
}

/// Tests direct Bidirectional Streaming RPC roundtrip between Client and Server.
#[tokio::test]
async fn test_grpc_bidirectional_streaming_roundtrip() {
    let config = GrpcConfig::for_tier(MemoryTier::Medium);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();

            let mut send = stream.respond.send_response(resp, false).unwrap();

            // Echo each incoming frame back to client
            while let Some(chunk_res) = stream.recv_stream.data().await {
                let chunk = chunk_res.unwrap();
                let len = chunk.len();
                let mut b = BytesMut::from(&chunk[..]);
                let (_, payload) = decode_grpc_frame(&mut b).unwrap().unwrap();

                let mut echo_frame = BytesMut::new();
                let mut echo_data = b"echo:".to_vec();
                echo_data.extend_from_slice(&payload);
                encode_grpc_frame(&echo_data, false, &mut echo_frame);

                send.send_data(echo_frame.freeze(), false).unwrap();
                let _ = stream.recv_stream.flow_control().release_capacity(len);
            }

            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let mut connector = GrpcUpstreamConnector::connect(server_addr, None, &test_config, None, None)
        .await
        .unwrap();

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/test.Bidi/Chat")
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();

    let (resp_fut, mut send_stream) = connector.open_stream(req, false).unwrap();

    // Send ping_1
    let mut f1 = BytesMut::new();
    encode_grpc_frame(b"ping_1", false, &mut f1);
    send_stream.send_data(f1.freeze(), false).unwrap();

    let resp = resp_fut.await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (_, mut body) = resp.into_parts();

    let r1 = body.data().await.unwrap().unwrap();
    let mut b1 = BytesMut::from(&r1[..]);
    let (_, p1) = decode_grpc_frame(&mut b1).unwrap().unwrap();
    assert_eq!(&p1[..], b"echo:ping_1");

    // Send ping_2 and end client stream
    let mut f2 = BytesMut::new();
    encode_grpc_frame(b"ping_2", false, &mut f2);
    send_stream.send_data(f2.freeze(), true).unwrap();

    let r2 = body.data().await.unwrap().unwrap();
    let mut b2 = BytesMut::from(&r2[..]);
    let (_, p2) = decode_grpc_frame(&mut b2).unwrap().unwrap();
    assert_eq!(&p2[..], b"echo:ping_2");

    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");

    server_task.abort();
}

/// Tests that server correctly rejects payload exceeding configured message size limit.
#[tokio::test]
async fn test_grpc_server_payload_too_large() {
    let restricted_config = GrpcConfig::for_tier(MemoryTier::Constrained).with_max_message_size(32);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &restricted_config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            match stream
                .read_raw_message(restricted_config.max_message_size)
                .await
            {
                Ok(_) => {
                    stream
                        .respond
                        .send_unary_response(GrpcStatus::Ok, Some(b"ok"), None)
                        .unwrap();
                }
                Err(_) => {
                    stream
                        .respond
                        .send_trailers_only(
                            GrpcStatus::ResourceExhausted,
                            Some("request message size exceeds limit"),
                        )
                        .unwrap();
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut connector =
        GrpcUpstreamConnector::connect(server_addr, None, &restricted_config, None, None)
            .await
            .unwrap();

    // 100 bytes payload exceeds 32 bytes limit
    let large_payload = vec![42u8; 100];
    let mut req_body = BytesMut::new();
    encode_grpc_frame(&large_payload, false, &mut req_body);

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/grpc".parse().unwrap());

    let req = L7Request::new(
        Method::POST,
        Uri::from_static("http://localhost/test.Unary/TooLarge"),
        http::Version::HTTP_2,
        headers,
        Body::Bytes(req_body.freeze()),
    );

    let client_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let resp = connector.invoke_unary(&req, &client_config).await.unwrap();

    assert_eq!(resp.status, StatusCode::OK);
    // RESOURCE_EXHAUSTED = 8
    assert_eq!(resp.headers.get("grpc-status").unwrap(), "8");
    assert_eq!(
        resp.headers.get("grpc-message").unwrap(),
        "request message size exceeds limit"
    );

    server_task.abort();
}
