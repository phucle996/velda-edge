use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;

use velda_ingress::{TcpBinding, TcpIngress, UdpBinding, UdpIngress};
use velda_sock::{Datagram, TcpListenerConfig, UdpSocket, UdpSocketConfig};

#[tokio::test]
async fn test_tcp_ingress_multi_shard_accept_loop() {
    let tcp_config = TcpListenerConfig::new()
        .with_reuseport(true)
        .with_concurrency_shards(4);

    let binding = TcpBinding::new("sharded-test", "127.0.0.1:0".parse().unwrap(), false)
        .with_config(tcp_config);

    let ingress = Arc::new(TcpIngress::bind(binding).unwrap());
    #[cfg(unix)]
    assert_eq!(ingress.shard_count(), 4);

    let bound_addr = ingress.local_addr();
    let mut tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let counter = Arc::new(AtomicU32::new(0));

    let counter_clone = Arc::clone(&counter);
    TcpIngress::spawn_accept_loop(ingress, &mut tasks, shutdown_rx, move |mut conn| {
        let cnt = Arc::clone(&counter_clone);
        async move {
            let mut buf = [0u8; 8];
            if matches!(conn.read(&mut buf).await, Ok(n) if n > 0) {
                cnt.fetch_add(1, Ordering::SeqCst);
                let _ = conn.write_all(b"sharded-ack").await;
            }
        }
    });

    // Concurrently connect 16 clients across the 4 listener shards
    let mut client_handles = Vec::new();
    for _ in 0..16 {
        let handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect(bound_addr).await.unwrap();
            stream.write_all(b"test-req").await.unwrap();
            let mut ack = [0u8; 11];
            stream.read_exact(&mut ack).await.unwrap();
            assert_eq!(&ack, b"sharded-ack");
        });
        client_handles.push(handle);
    }

    for h in client_handles {
        h.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 16);

    // Shutdown
    shutdown_tx.send(true).unwrap();
    while let Some(res) = tasks.join_next().await {
        res.unwrap();
    }
}

#[tokio::test]
async fn test_udp_ingress_delivers_datagram_with_listener_id() {
    let dummy_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let binding = UdpBinding::new("h3-ingress", dummy_addr, true);
    let ingress = Arc::new(UdpIngress::bind(binding).unwrap());
    let local_addr = ingress.local_addr();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (received_tx, mut received_rx) = tokio::sync::mpsc::channel(1);

    let mut tasks = JoinSet::new();
    let tx = received_tx.clone();
    UdpIngress::spawn_receive_loop(
        Arc::clone(&ingress),
        &mut tasks,
        shutdown_rx,
        move |listener_id: Arc<str>, socket: Arc<UdpSocket>, dgram: Datagram| {
            let tx = tx.clone();
            async move {
                assert_eq!(&*listener_id, "h3-ingress");
                assert_eq!(dgram.data(), b"QUIC-Client-Hello");

                socket
                    .send_to(b"QUIC-Server-Hello", dgram.peer())
                    .await
                    .unwrap();
                let _ = tx.send(()).await;
            }
        },
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send packet using client UDP socket
    let client =
        UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap();

    client
        .send_to(b"QUIC-Client-Hello", local_addr)
        .await
        .unwrap();

    let mut buf = [0u8; 64];
    let (n, from) = client.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"QUIC-Server-Hello");
    assert_eq!(from, local_addr);

    received_rx.recv().await.unwrap();

    shutdown_tx.send(true).unwrap();
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn test_udp_binding_and_datagram_response_roundtrip() {
    let client =
        UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap();
    let server = Arc::new(
        UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap(),
    );

    let h3_binding = UdpBinding::new("h3-listener", server.local_addr(), true);
    assert_eq!(h3_binding.id, "h3-listener");
    assert!(h3_binding.tls_enabled);

    let dgram = Datagram::new(
        client.local_addr(),
        server.local_addr(),
        b"HTTP/3-Initial-Packet".to_vec(),
    );

    assert_eq!(dgram.peer(), client.local_addr());
    assert_eq!(dgram.data(), b"HTTP/3-Initial-Packet");

    server.send_to(b"HTTP/3-Ack", dgram.peer()).await.unwrap();

    let mut buf = [0u8; 64];
    let (n, from) = client.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"HTTP/3-Ack");
    assert_eq!(from, server.local_addr());
}
