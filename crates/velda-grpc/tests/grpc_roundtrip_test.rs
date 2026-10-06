use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Method, StatusCode, Uri};
use std::time::Duration;
use tokio::net::TcpListener;
use velda_core::{Body, L7Request, MemoryTier};
use velda_grpc::GrpcConfig;
use velda_grpc::frame::{decode_grpc_frame, encode_grpc_frame};
use velda_grpc::status::GrpcStatus;
use velda_grpc::tcp::client::GrpcUpstreamConnector;
use velda_grpc::tcp::pipe::{GrpcPipeStrategy, pipe_grpc_stream};
use velda_grpc::tcp::server::GrpcServerConnection;

#[tokio::test]
async fn test_grpc_unary_one_way_roundtrip() {
    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let srv_config = test_config;

    // 1. Start mock gRPC server using GrpcServerConnection
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &srv_config)
            .await
            .unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            assert_eq!(stream.parts.uri.path(), "/test.UnaryService/EchoUnary");
            assert_eq!(stream.parts.method, Method::POST);

            // Read the Unary incoming message
            let msg = stream.read_unary_message(10 * 1024 * 1024).await.unwrap();
            assert_eq!(msg, Some(Bytes::from_static(b"hello_unary")));

            // Send Unary response with GrpcStatus::Ok
            stream
                .respond
                .send_unary_response(GrpcStatus::Ok, Some(b"world_unary"), None)
                .unwrap();
        }
    });

    // 2. Client calls unary RPC using GrpcUpstreamConnector
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut connector = GrpcUpstreamConnector::connect(server_addr, &test_config, None, None)
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

#[tokio::test]
async fn test_grpc_server_trailers_only_response() {
    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let srv_config = test_config;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket, &srv_config)
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

    let mut connector = GrpcUpstreamConnector::connect(server_addr, &test_config, None, None)
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

#[tokio::test]
async fn test_grpc_streaming_pipe_roundtrip() {
    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let srv_config = test_config;
    let pipe_config = test_config;

    // 1. Mock upstream gRPC backend
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (sock, _) = backend_listener.accept().await.unwrap();
        let mut server = h2::server::handshake(sock).await.unwrap();
        while let Some(res) = server.accept().await {
            let (_req, mut respond) = res.unwrap();
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();
            let mut send = respond.send_response(resp, false).unwrap();

            let mut f1 = BytesMut::new();
            encode_grpc_frame(b"stream_chunk_1", false, &mut f1);
            send.send_data(f1.freeze(), false).unwrap();

            let mut f2 = BytesMut::new();
            encode_grpc_frame(b"stream_chunk_2", false, &mut f2);
            send.send_data(f2.freeze(), false).unwrap();

            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            trailers.insert("grpc-message", "Stream finished".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        }
    });

    // 2. Mock proxy gateway using server + pipe
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (sock, _) = proxy_listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(sock, &srv_config)
            .await
            .unwrap();
        while let Some(server_stream) = conn.accept().await.unwrap() {
            let cfg = pipe_config;
            tokio::spawn(async move {
                pipe_grpc_stream(server_stream, backend_addr, GrpcPipeStrategy::Duplex, &cfg)
                    .await
                    .unwrap();
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. Client connects to proxy and receives streaming messages
    let mut connector = GrpcUpstreamConnector::connect(proxy_addr, &test_config, None, None)
        .await
        .unwrap();
    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/test.StreamService/Fetch")
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
    assert_eq!(&p1[..], b"stream_chunk_1");

    let chunk2 = body.data().await.unwrap().unwrap();
    let mut b2 = BytesMut::from(&chunk2[..]);
    let (_, p2) = decode_grpc_frame(&mut b2).unwrap().unwrap();
    assert_eq!(&p2[..], b"stream_chunk_2");

    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    assert_eq!(trailers.get("grpc-message").unwrap(), "Stream finished");

    backend_task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn test_grpc_buffered_pipe_roundtrip() {
    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let srv_config = test_config;
    let pipe_config = test_config;

    // 1. Mock upstream gRPC backend responding with 1 message
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (sock, _) = backend_listener.accept().await.unwrap();
        let mut server = h2::server::handshake(sock).await.unwrap();
        while let Some(res) = server.accept().await {
            let (_req, mut respond) = res.unwrap();
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();
            let mut send = respond.send_response(resp, false).unwrap();

            let mut f = BytesMut::new();
            encode_grpc_frame(b"buffered_reply", false, &mut f);
            send.send_data(f.freeze(), false).unwrap();

            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        }
    });

    // 2. Mock proxy gateway using Buffered strategy
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (sock, _) = proxy_listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(sock, &srv_config)
            .await
            .unwrap();
        while let Some(server_stream) = conn.accept().await.unwrap() {
            let cfg = pipe_config;
            tokio::spawn(async move {
                pipe_grpc_stream(
                    server_stream,
                    backend_addr,
                    GrpcPipeStrategy::Buffered,
                    &cfg,
                )
                .await
                .unwrap();
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. Client calls unary request via proxy
    let mut connector = GrpcUpstreamConnector::connect(proxy_addr, &test_config, None, None)
        .await
        .unwrap();
    let mut req_body = BytesMut::new();
    encode_grpc_frame(b"buffered_ping", false, &mut req_body);

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/grpc".parse().unwrap());

    let req = L7Request::new(
        Method::POST,
        Uri::from_static("http://localhost/test.Unary/Ping"),
        http::Version::HTTP_2,
        headers,
        Body::Bytes(req_body.freeze()),
    );

    let resp = connector.invoke_unary(&req, &test_config).await.unwrap();

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.headers.get("grpc-status").unwrap(), "0");

    if let Body::Bytes(b) = resp.body {
        let mut buf = BytesMut::from(&b[..]);
        let (_, payload) = decode_grpc_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&payload[..], b"buffered_reply");
    } else {
        panic!("Expected binary body");
    }

    backend_task.abort();
    proxy_task.abort();
}

#[tokio::test]
async fn test_grpc_buffered_pipe_rejects_payload_exceeding_max_message_size() {
    // Limit max_message_size to 32 bytes
    let restricted_config = GrpcConfig::for_tier(MemoryTier::Constrained).with_max_message_size(32);
    let srv_config = restricted_config;
    let pipe_config = restricted_config;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    // Upstream address that shouldn't even be reached
    let dummy_upstream = "127.0.0.1:1".parse().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (sock, _) = proxy_listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(sock, &srv_config)
            .await
            .unwrap();
        while let Some(server_stream) = conn.accept().await.unwrap() {
            let cfg = pipe_config;
            tokio::spawn(async move {
                let _ = pipe_grpc_stream(
                    server_stream,
                    dummy_upstream,
                    GrpcPipeStrategy::Buffered,
                    &cfg,
                )
                .await;
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Send a 100-byte payload which exceeds the 32-byte limit
    let mut connector = GrpcUpstreamConnector::connect(proxy_addr, &restricted_config, None, None)
        .await
        .unwrap();
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

    proxy_task.abort();
}

#[tokio::test]
async fn test_grpc_server_stream_pipe_roundtrip() {
    let test_config = GrpcConfig::for_tier(MemoryTier::Medium);
    let srv_config = test_config;
    let pipe_config = test_config;

    // 1. Mock upstream gRPC backend streaming responses
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (sock, _) = backend_listener.accept().await.unwrap();
        let mut server = h2::server::handshake(sock).await.unwrap();
        while let Some(res) = server.accept().await {
            let (_req, mut respond) = res.unwrap();
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();
            let mut send = respond.send_response(resp, false).unwrap();

            let mut f1 = BytesMut::new();
            encode_grpc_frame(b"server_stream_1", false, &mut f1);
            send.send_data(f1.freeze(), false).unwrap();

            let mut f2 = BytesMut::new();
            encode_grpc_frame(b"server_stream_2", false, &mut f2);
            send.send_data(f2.freeze(), false).unwrap();

            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        }
    });

    // 2. Mock proxy gateway using ServerStream strategy
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (sock, _) = proxy_listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(sock, &srv_config)
            .await
            .unwrap();
        while let Some(server_stream) = conn.accept().await.unwrap() {
            let cfg = pipe_config;
            tokio::spawn(async move {
                pipe_grpc_stream(
                    server_stream,
                    backend_addr,
                    GrpcPipeStrategy::ServerStream,
                    &cfg,
                )
                .await
                .unwrap();
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. Client receives stream chunks
    let mut connector = GrpcUpstreamConnector::connect(proxy_addr, &test_config, None, None)
        .await
        .unwrap();
    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/test.Stream/Events")
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();

    let (resp_fut, _) = connector.open_stream(req, true).unwrap();
    let resp = resp_fut.await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let (_, mut body) = resp.into_parts();
    let c1 = body.data().await.unwrap().unwrap();
    let mut b1 = BytesMut::from(&c1[..]);
    let (_, p1) = decode_grpc_frame(&mut b1).unwrap().unwrap();
    assert_eq!(&p1[..], b"server_stream_1");

    let c2 = body.data().await.unwrap().unwrap();
    let mut b2 = BytesMut::from(&c2[..]);
    let (_, p2) = decode_grpc_frame(&mut b2).unwrap().unwrap();
    assert_eq!(&p2[..], b"server_stream_2");

    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");

    backend_task.abort();
    proxy_task.abort();
}
