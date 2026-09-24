use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use velda_core::{ConnectionId, TransportProtocol};
use velda_transport::{Connection, next_connection_id};

#[tokio::test]
async fn test_connection_io_and_byte_counters() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(server_addr).await.unwrap();
        client.write_all(b"ping from client").await.unwrap();

        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"pong from server");
    });

    let (stream, peer_addr) = listener.accept().await.unwrap();
    let mut conn = Connection::from_stream(stream).unwrap();

    assert_eq!(conn.peer(), peer_addr);
    assert_eq!(conn.local_addr(), server_addr);
    assert_eq!(conn.bytes_read(), 0);
    assert_eq!(conn.bytes_written(), 0);

    let mut read_buf = [0u8; 32];
    let n = conn.read(&mut read_buf).await.unwrap();
    assert_eq!(&read_buf[..n], b"ping from client");
    assert_eq!(conn.bytes_read(), 16);

    conn.write_all(b"pong from server").await.unwrap();
    conn.flush().await.unwrap();
    assert_eq!(conn.bytes_written(), 16);

    client_task.await.unwrap();
}

#[tokio::test]
async fn test_connection_context_and_l4_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let _client = TcpStream::connect(server_addr).await.unwrap();
    let (stream, peer_addr) = listener.accept().await.unwrap();

    let id = ConnectionId::new(42);
    let conn = Connection::new(id, stream, peer_addr, server_addr);

    assert_eq!(conn.id(), id);

    let l4_req = conn.to_l4_request();
    assert_eq!(l4_req.connection_id, id);
    assert_eq!(l4_req.protocol, TransportProtocol::Tcp);
    assert_eq!(l4_req.peer.address, peer_addr);
    assert_eq!(l4_req.local_addr, server_addr);
    assert_eq!(l4_req.client_ip(), peer_addr.ip());
    assert_eq!(l4_req.client_port(), peer_addr.port());

    let ctx = conn.to_connection_context();
    assert_eq!(ctx.id(), id);
    assert_eq!(ctx.client_ip(), peer_addr.ip());
}

#[tokio::test]
async fn test_connection_split_halves() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(server_addr).await.unwrap();
        client.write_all(b"hello reader").await.unwrap();

        let mut buf = [0u8; 12];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello writer");
    });

    let (stream, _) = listener.accept().await.unwrap();
    let conn = Connection::from_stream(stream).unwrap();

    let (mut reader, mut writer) = conn.split();

    let mut buf = [0u8; 32];
    let n = reader.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"hello reader");
    assert_eq!(reader.bytes_read(), 12);

    writer.write_all(b"hello writer").await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(writer.bytes_written(), 12);

    client_task.await.unwrap();
}

#[tokio::test]
async fn test_connection_async_read_write_trait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(server_addr).await.unwrap();
        client.write_all(b"trait test").await.unwrap();
        client.shutdown().await.unwrap();

        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"trait test response");
    });

    let (stream, _) = listener.accept().await.unwrap();
    let mut conn = Connection::from_stream(stream).unwrap();

    // Read using AsyncReadExt trait methods
    let mut buf = [0u8; 10];
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"trait test");
    assert_eq!(conn.bytes_read(), 10);

    // Write using AsyncWriteExt trait methods
    conn.write_all(b"trait test response").await.unwrap();
    conn.shutdown().await.unwrap();
    assert_eq!(conn.bytes_written(), 19);

    client_task.await.unwrap();
}

#[tokio::test]
async fn test_connection_into_inner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let _client = TcpStream::connect(server_addr).await.unwrap();
    let (stream, peer_addr) = listener.accept().await.unwrap();

    let conn = Connection::from_stream(stream).unwrap();
    let conn_id = conn.id();

    let (id, _stream, peer, local) = conn.into_inner();
    assert_eq!(id, conn_id);
    assert_eq!(peer, peer_addr);
    assert_eq!(local, server_addr);
}

#[test]
fn test_monotonic_connection_id_generator() {
    let id1 = next_connection_id();
    let id2 = next_connection_id();
    let id3 = next_connection_id();

    assert!(id2.value() > id1.value());
    assert!(id3.value() > id2.value());
}
