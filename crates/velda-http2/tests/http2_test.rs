use bytes::Bytes;
use http::{Method, StatusCode, Uri};
use tokio::io::duplex;
use tokio::net::TcpListener;
use velda_core::{Body, L7Request};
use velda_http2::composer_parse::Http2ServerConnection;
use velda_http2::upstream_connector::Http2UpstreamConnector;

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

    let mut server_conn = Http2ServerConnection::handshake(server_io).await.unwrap();
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

    let req = L7Request::new(
        Method::GET,
        Uri::from_static("http://localhost/health"),
        http::Version::HTTP_2,
        http::HeaderMap::new(),
        Body::Empty,
    );

    let resp = Http2UpstreamConnector::forward_request(&req, backend_addr)
        .await
        .unwrap();

    assert_eq!(resp.status, StatusCode::OK);
    if let Body::Bytes(ref b) = resp.body {
        assert_eq!(&b[..], b"h2 alive");
    } else {
        panic!("expected body bytes");
    }
}
