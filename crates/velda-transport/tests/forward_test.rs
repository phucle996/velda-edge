use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use velda_transport::{Connection, connect_and_forward, forward_bidirectional, forward_connection};

#[tokio::test]
async fn test_forward_bidirectional_echo() {
    // 1. Setup mock upstream echo server
    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();

    let echo_task = tokio::spawn(async move {
        let (mut socket, _) = echo_listener.accept().await.unwrap();
        let (mut r, mut w) = socket.split();
        tokio::io::copy(&mut r, &mut w).await.unwrap();
    });

    // 2. Setup proxy listener
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (mut client_socket, _) = proxy_listener.accept().await.unwrap();
        let mut upstream_socket = TcpStream::connect(echo_addr).await.unwrap();

        forward_bidirectional(&mut client_socket, &mut upstream_socket)
            .await
            .unwrap()
    });

    // 3. Client connects to proxy
    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        let payload = b"bidirectional transfer test payload";
        client.write_all(payload).await.unwrap();
        client.shutdown().await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(&response, payload);
    });

    client_task.await.unwrap();
    let stats = proxy_task.await.unwrap();
    echo_task.abort();

    assert_eq!(stats.client_to_server_bytes, 35);
    assert_eq!(stats.server_to_client_bytes, 35);
    assert_eq!(stats.total_bytes(), 70);
}

#[tokio::test]
async fn test_forward_connection_with_large_payload() {
    // Upstream server receives data and sends back an acknowledgment payload
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();

    let size = 256 * 1024; // 256 KB
    let upstream_task = tokio::spawn(async move {
        let (mut socket, _) = upstream_listener.accept().await.unwrap();
        let mut buf = vec![0u8; size];
        socket.read_exact(&mut buf).await.unwrap();

        // Respond with 16 bytes ack
        socket.write_all(b"ACK-DATA-SUCCESS").await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        let conn = Connection::from_stream(stream).unwrap();
        let upstream = TcpStream::connect(upstream_addr).await.unwrap();

        forward_connection(conn, upstream).await.unwrap()
    });

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        let payload = vec![0x42u8; size];
        client.write_all(&payload).await.unwrap();
        client.shutdown().await.unwrap();

        let mut ack = Vec::new();
        client.read_to_end(&mut ack).await.unwrap();
        assert_eq!(&ack, b"ACK-DATA-SUCCESS");
    });

    client_task.await.unwrap();
    let stats = proxy_task.await.unwrap();
    upstream_task.await.unwrap();

    assert_eq!(stats.client_to_server_bytes, size as u64);
    assert_eq!(stats.server_to_client_bytes, 16);
    assert_eq!(stats.total_bytes(), size as u64 + 16);
}

#[tokio::test]
async fn test_connect_and_forward_helper() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = upstream_listener.accept().await.unwrap();
        let mut buf = [0u8; 4];
        socket.read_exact(&mut buf).await.unwrap();
        socket.write_all(b"PONG").await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        let conn = Connection::from_stream(stream).unwrap();
        connect_and_forward(conn, upstream_addr).await.unwrap()
    });

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(b"PING").await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"PONG");
    });

    client_task.await.unwrap();
    let stats = proxy_task.await.unwrap();

    assert_eq!(stats.client_to_server_bytes, 4);
    assert_eq!(stats.server_to_client_bytes, 4);
}

#[tokio::test]
async fn test_forward_connection_with_custom_buffer_size() {
    use velda_transport::forward_connection_with_size;

    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();

    let upstream_task = tokio::spawn(async move {
        let (mut socket, _) = upstream_listener.accept().await.unwrap();
        let mut buf = [0u8; 11];
        socket.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"custom-size");
        socket.write_all(b"custom-resp").await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        let conn = Connection::from_stream(stream).unwrap();
        let upstream = TcpStream::connect(upstream_addr).await.unwrap();
        forward_connection_with_size(conn, upstream, 8 * 1024)
            .await
            .unwrap()
    });

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(b"custom-size").await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"custom-resp");
    });

    client_task.await.unwrap();
    let stats = proxy_task.await.unwrap();
    upstream_task.await.unwrap();

    assert_eq!(stats.client_to_server_bytes, 11);
    assert_eq!(stats.server_to_client_bytes, 11);
}

#[tokio::test]
async fn test_splice_bidirectional_large_stream() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();

    let size = 1024 * 1024; // 1 MB
    let upstream_task = tokio::spawn(async move {
        let (mut socket, _) = upstream_listener.accept().await.unwrap();
        let mut buf = vec![0u8; size];
        socket.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[0], 0xAA);
        assert_eq!(buf[size - 1], 0xAA);

        let response = vec![0xBBu8; size / 2];
        socket.write_all(&response).await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        let conn = Connection::from_stream(stream).unwrap();
        let upstream = TcpStream::connect(upstream_addr).await.unwrap();
        velda_transport::forward_connection(conn, upstream)
            .await
            .unwrap()
    });

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        let payload = vec![0xAAu8; size];
        client.write_all(&payload).await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size / 2);
        assert_eq!(buf[0], 0xBB);
    });

    client_task.await.unwrap();
    let stats = proxy_task.await.unwrap();
    upstream_task.await.unwrap();

    assert_eq!(stats.client_to_server_bytes, size as u64);
    assert_eq!(stats.server_to_client_bytes, (size / 2) as u64);
    assert_eq!(stats.total_bytes(), size as u64 + (size / 2) as u64);
}
