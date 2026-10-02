use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Method, StatusCode, Uri};
use std::time::Duration;
use tokio::net::TcpListener;
use velda_core::{Body, L7Request};
use velda_grpc::composer_parse::GrpcServerConnection;
use velda_grpc::frame::{decode_grpc_frame, encode_grpc_frame};
use velda_grpc::status::GrpcStatus;
use velda_grpc::upstream_connector::GrpcUpstreamConnector;

#[tokio::test]
async fn test_grpc_unary_one_way_roundtrip() {
    // 1. Start mock gRPC server using composer_parse
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket).await.unwrap();

        while let Some(mut stream) = conn.accept().await.unwrap() {
            assert_eq!(stream.path(), "/test.UnaryService/EchoUnary");
            assert_eq!(stream.method(), &Method::POST);

            // Read the 1 chiều (Unary) incoming message
            let msg = stream.read_unary_message(10 * 1024 * 1024).await.unwrap();
            assert_eq!(msg, Some(Bytes::from_static(b"hello_unary")));

            // Send 1 chiều response with GrpcStatus::Ok
            stream
                .send_unary_response(GrpcStatus::Ok, Some(b"world_unary"), None)
                .unwrap();
        }
    });

    // 2. Client calls unary RPC using upstream_connector
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut connector = GrpcUpstreamConnector::connect(server_addr).await.unwrap();

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

    let test_config = velda_grpc::GrpcConfig::for_tier(velda_core::MemoryTier::Medium);
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
async fn test_grpc_composer_trailers_only_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(socket).await.unwrap();

        while let Some(stream) = conn.accept().await.unwrap() {
            stream
                .send_trailers_only(GrpcStatus::NotFound, Some("custom entity not found"))
                .unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut connector = GrpcUpstreamConnector::connect(server_addr).await.unwrap();

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/grpc".parse().unwrap());

    let req = L7Request::new(
        Method::POST,
        Uri::from_static("http://localhost/test.UnaryService/MissingEntity"),
        http::Version::HTTP_2,
        headers,
        Body::Empty,
    );

    let test_config = velda_grpc::GrpcConfig::for_tier(velda_core::MemoryTier::Medium);
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
    use velda_grpc::pipe_grpc_stream;

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

    // 2. Mock proxy gateway using composer_parse + stream_pipe
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (sock, _) = proxy_listener.accept().await.unwrap();
        let mut conn = GrpcServerConnection::handshake(sock).await.unwrap();
        while let Some(server_stream) = conn.accept().await.unwrap() {
            tokio::spawn(async move {
                pipe_grpc_stream(server_stream, backend_addr).await.unwrap();
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. Client connects to proxy and receives streaming messages
    let mut connector = GrpcUpstreamConnector::connect(proxy_addr).await.unwrap();
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
