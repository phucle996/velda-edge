use http::{Method, StatusCode, Uri};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::net::TcpListener;
use velda_core::{Body, L7Request};
use velda_http1::composer_parse::Http1ServerConnection;
use velda_http1::upstream_connector::Http1UpstreamConnector;

#[tokio::test]
async fn test_http1_server_connection() {
    let (mut client, server) = duplex(1024);
    let mut conn = Http1ServerConnection::new(server);

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
        http::Version::HTTP_11,
        http::HeaderMap::new(),
        Body::Empty,
    );

    let resp = Http1UpstreamConnector::forward_request(&req, backend_addr)
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}
