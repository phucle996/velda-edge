use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use velda_sock::{TcpListener, TcpListenerConfig, raw_to_socket_addr};

const LOOPBACK_V4: u128 = velda_core::lut::ip::v4_to_u128(0x7F00_0001);

#[tokio::test]
async fn test_tcp_listener_bind_and_accept() {
    let config = TcpListenerConfig::new().with_nodelay(true);
    let listener = TcpListener::bind(LOOPBACK_V4, 0, config).unwrap();

    let bound_addr = raw_to_socket_addr(listener.local_ip, listener.local_port_be);
    assert_ne!(u16::from_be(listener.local_port_be), 0);

    let client_task = tokio::spawn(async move {
        let mut stream = TcpStream::connect(bound_addr).await.unwrap();
        stream.write_all(b"ping").await.unwrap();
    });

    let mut conn = listener.accept().await.unwrap();
    let mut buf = [0u8; 4];
    let n = conn.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"ping");

    client_task.await.unwrap();
}

#[tokio::test]
async fn test_tcp_listener_accept_with_shutdown() {
    let config = TcpListenerConfig::default();
    let listener = TcpListener::bind(LOOPBACK_V4, 0, config).unwrap();
    let bound_addr = raw_to_socket_addr(listener.local_ip, listener.local_port_be);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn a client that connects
    tokio::spawn(async move {
        let _ = TcpStream::connect(bound_addr).await.unwrap();
    });

    // Accept first connection
    let maybe_conn = listener
        .accept_with_shutdown(&mut shutdown_rx)
        .await
        .unwrap();
    assert!(maybe_conn.is_some());

    // Signal shutdown
    shutdown_tx.send(true).unwrap();

    // Now accept_with_shutdown should return None immediately
    let should_be_none = listener
        .accept_with_shutdown(&mut shutdown_rx)
        .await
        .unwrap();
    assert!(should_be_none.is_none());
}

#[tokio::test]
async fn test_tcp_listener_serve_loop() {
    let config = TcpListenerConfig::default();
    let listener = Arc::new(TcpListener::bind(LOOPBACK_V4, 0, config).unwrap());
    let bound_addr = raw_to_socket_addr(listener.local_ip, listener.local_port_be);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let counter = Arc::new(AtomicU32::new(0));

    let listener_clone = Arc::clone(&listener);
    let counter_clone = Arc::clone(&counter);

    let server_task = tokio::spawn(async move {
        listener_clone
            .serve(shutdown_rx, move |mut conn| {
                let cnt = Arc::clone(&counter_clone);
                async move {
                    let mut buf = [0u8; 8];
                    if matches!(conn.read(&mut buf).await, Ok(n) if n > 0) {
                        cnt.fetch_add(1, Ordering::SeqCst);
                        let _ = conn.write_all(b"ack").await;
                    }
                }
            })
            .await
    });

    // Spawn 5 concurrent clients
    let mut client_handles = Vec::new();
    for _ in 0..5 {
        let handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect(bound_addr).await.unwrap();
            stream.write_all(b"hello").await.unwrap();
            let mut ack = [0u8; 3];
            stream.read_exact(&mut ack).await.unwrap();
            assert_eq!(&ack, b"ack");
        });
        client_handles.push(handle);
    }

    for h in client_handles {
        h.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 5);

    // Signal graceful shutdown
    shutdown_tx.send(true).unwrap();

    // Wait for server task to finish cleanly
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server did not shut down within timeout")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_tcp_listener_reuseport_shards() {
    let config = TcpListenerConfig::new()
        .with_reuseport(true)
        .with_concurrency_shards(4)
        .with_quickack(true);
    let shards = TcpListener::bind_shards(LOOPBACK_V4, 0, config).unwrap();

    #[cfg(unix)]
    assert_eq!(shards.len(), 4);
    #[cfg(not(unix))]
    assert!(!shards.is_empty());

    let port = u16::from_be(shards[0].local_port_be);
    assert_ne!(port, 0);

    for s in &shards {
        assert_eq!(u16::from_be(s.local_port_be), port);
    }
}

#[tokio::test]
async fn test_tcp_listener_shards_with_incoming_cpu() {
    let config = TcpListenerConfig::new()
        .with_reuseport(true)
        .with_concurrency_shards(2)
        .with_incoming_cpu(true);
    let shards = TcpListener::bind_shards(LOOPBACK_V4, 0, config).unwrap();

    #[cfg(unix)]
    assert_eq!(shards.len(), 2);
    #[cfg(not(unix))]
    assert!(!shards.is_empty());

    let port = u16::from_be(shards[0].local_port_be);
    assert_ne!(port, 0);
}
