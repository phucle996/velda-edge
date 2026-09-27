use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use velda_transport::{IngressBinding, IngressListener, PathKind, TrafficEngine};

#[tokio::test]
async fn test_traffic_engine_configured_from_listeners_json_schema() {
    let dummy_ephemeral: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // 1. Mirror listeners.json "http" listener:
    let http_binding =
        IngressBinding::from_protocols("http", dummy_ephemeral, "tcp", "http", false).unwrap();
    let http_listener = IngressListener::bind(http_binding).unwrap();
    let http_addr = http_listener.local_addr();

    // 2. Mirror listeners.json "https" listener:
    let https_binding =
        IngressBinding::from_protocols("https", dummy_ephemeral, "tcp", "http", true).unwrap();
    let https_listener = IngressListener::bind(https_binding).unwrap();
    let https_addr = https_listener.local_addr();

    // 3. Mirror listeners.json "tcp-ingress" listener:
    let tcp_binding =
        IngressBinding::from_protocols("tcp-ingress", dummy_ephemeral, "tcp", "raw", false)
            .unwrap();
    let tcp_listener = IngressListener::bind(tcp_binding).unwrap();
    let tcp_addr = tcp_listener.local_addr();

    let mut engine = TrafficEngine::new();
    engine.add_tcp_listener(http_listener);
    engine.add_tcp_listener(https_listener);
    engine.add_tcp_listener(tcp_listener);
    assert_eq!(engine.tcp_listener_count(), 3);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let l4_count = Arc::new(AtomicU32::new(0));
    let l7_count = Arc::new(AtomicU32::new(0));

    let l4_clone = Arc::clone(&l4_count);
    let l7_clone = Arc::clone(&l7_count);

    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                move |mut conn| {
                    let cnt = Arc::clone(&l4_clone);
                    async move {
                        let mut buf = [0u8; 16];
                        if matches!(conn.read(&mut buf).await, Ok(n) if n > 0) {
                            cnt.fetch_add(1, Ordering::SeqCst);
                            let _ = conn.write_all(b"l4-ok").await;
                        }
                    }
                },
                move |handoff| {
                    let cnt = Arc::clone(&l7_clone);
                    async move {
                        // Verify listener metadata attached to handoff
                        assert!(
                            handoff.listener_id() == "http" || handoff.listener_id() == "https"
                        );

                        cnt.fetch_add(1, Ordering::SeqCst);
                        let mut conn = handoff.into_connection();
                        let _ = conn.write_all(b"l7-ok").await;
                    }
                },
            )
            .await
    });

    // Client 1: Connects to HTTP listener (port 80)
    let client1 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(http_addr).await.unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();

        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"l7-ok");
    });

    // Client 2: Connects to HTTPS listener (port 443)
    let client2 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(https_addr).await.unwrap();
        stream.write_all(b"TLS ClientHello payload").await.unwrap();

        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"l7-ok");
    });

    // Client 3: Connects to TCP ingress listener (port 9000)
    let client3 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(tcp_addr).await.unwrap();
        stream.write_all(b"raw tcp bytes").await.unwrap();

        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"l4-ok");
    });

    client1.await.unwrap();
    client2.await.unwrap();
    client3.await.unwrap();

    assert_eq!(l7_count.load(Ordering::SeqCst), 2);
    assert_eq!(l4_count.load(Ordering::SeqCst), 1);

    // Signal graceful shutdown
    shutdown_tx.send(true).unwrap();

    tokio::time::timeout(Duration::from_secs(2), engine_task)
        .await
        .expect("engine did not shutdown in time")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_multi_port_heterogeneous_bindings_and_concurrency() {
    let dummy_ephemeral: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut engine = TrafficEngine::new();

    // 2 HTTP listeners on different ports
    engine
        .add_binding(
            IngressBinding::from_protocols("http-public", dummy_ephemeral, "tcp", "http", false)
                .unwrap(),
        )
        .unwrap();
    engine
        .add_binding(
            IngressBinding::from_protocols("http-internal", dummy_ephemeral, "tcp", "http", false)
                .unwrap(),
        )
        .unwrap();

    // 2 TCP listeners on different ports
    engine
        .add_binding(
            IngressBinding::from_protocols("tcp-db-pg", dummy_ephemeral, "tcp", "raw", false)
                .unwrap(),
        )
        .unwrap();
    engine
        .add_binding(
            IngressBinding::from_protocols("tcp-redis", dummy_ephemeral, "tcp", "raw", false)
                .unwrap(),
        )
        .unwrap();

    // 2 UDP listeners on different ports
    engine
        .add_binding(
            IngressBinding::from_protocols("udp-dns", dummy_ephemeral, "udp", "raw", false)
                .unwrap(),
        )
        .unwrap();
    engine
        .add_binding(
            IngressBinding::from_protocols("udp-metrics", dummy_ephemeral, "udp", "raw", false)
                .unwrap(),
        )
        .unwrap();

    assert_eq!(engine.tcp_listener_count(), 4);
    assert_eq!(engine.udp_listener_count(), 2);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_task = tokio::spawn(async move {
        engine
            .run_with_udp(
                shutdown_rx,
                |mut conn| async move {
                    let mut buf = [0u8; 32];
                    if let Ok(n) = conn.read(&mut buf).await {
                        let resp = format!("ack-tcp-{}", String::from_utf8_lossy(&buf[..n]));
                        let _ = conn.write_all(resp.as_bytes()).await;
                    }
                },
                |handoff| async move {
                    let mut conn = handoff.into_connection();
                    let mut buf = [0u8; 32];
                    if let Ok(n) = conn.read(&mut buf).await {
                        let resp = format!("ack-http-{}", String::from_utf8_lossy(&buf[..n]));
                        let _ = conn.write_all(resp.as_bytes()).await;
                    }
                },
                |id, socket, dgram| async move {
                    let resp = format!("ack-udp-{id}-{}", String::from_utf8_lossy(dgram.data()));
                    let _ = socket.send_to(resp.as_bytes(), dgram.peer()).await;
                },
            )
            .await
    });

    // Give a brief tick for listeners to initialize
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Shut down gracefully
    shutdown_tx.send(true).unwrap();

    tokio::time::timeout(Duration::from_secs(2), engine_task)
        .await
        .expect("engine did not shutdown in time")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_traffic_engine_declarative_reconciliation() {
    // Determine 2 free ports by binding ephemeral listeners and getting their addresses
    let free_addr1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let free_addr2 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let mut engine = TrafficEngine::new();
    let handle = engine.handle();

    // Initial binding: port 1 only
    let binding1 =
        IngressBinding::from_protocols("listener-1", free_addr1, "tcp", "raw", false).unwrap();
    engine.add_binding(binding1).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                |mut conn| async move {
                    let mut buf = [0u8; 16];
                    if let Ok(n) = conn.read(&mut buf).await {
                        let msg = format!("ack-{}", String::from_utf8_lossy(&buf[..n]));
                        let _ = conn.write_all(msg.as_bytes()).await;
                    }
                },
                |_handoff| async move {},
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Connect to Port 1: should succeed
    {
        let mut stream = TcpStream::connect(free_addr1)
            .await
            .expect("port 1 should be open");
        stream.write_all(b"p1").await.unwrap();
        let mut buf = [0u8; 6];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ack-p1");
    }

    // 2. Reconcile: remove listener 1, add listener 2
    let binding2 =
        IngressBinding::from_protocols("listener-2", free_addr2, "tcp", "raw", false).unwrap();
    handle
        .reconcile(vec![binding2])
        .await
        .expect("reconcile should succeed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Connect to Port 2: should succeed (dynamically opened!)
    {
        let mut stream = TcpStream::connect(free_addr2)
            .await
            .expect("port 2 should be open");
        stream.write_all(b"p2").await.unwrap();
        let mut buf = [0u8; 6];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ack-p2");
    }

    // 4. Connect to Port 1: should fail (dynamically closed!)
    {
        let connect_res = TcpStream::connect(free_addr1).await;
        assert!(
            connect_res.is_err(),
            "port 1 should be closed after reconcile"
        );
    }

    // 5. Graceful shutdown
    shutdown_tx.send(true).unwrap();
    let _ = engine_task.await.unwrap();
}

#[tokio::test]
async fn test_multi_protocol_engine_http1_http2_tcp_udp_http3() {
    use velda_transport::{UdpL7Handoff, UdpSocket, UdpSocketConfig};

    let free_tcp: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let free_h1: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let free_h2: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let free_udp: SocketAddr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap()
    };
    let free_h3: SocketAddr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap()
    };

    let tcp_bind = IngressBinding::from_protocols("tcp", free_tcp, "tcp", "raw", false).unwrap();
    let h1_bind = IngressBinding::from_protocols("h1", free_h1, "tcp", "http", false).unwrap();
    let h2_bind = IngressBinding::from_protocols("h2", free_h2, "tcp", "http", false).unwrap();
    let udp_bind = IngressBinding::from_protocols("udp", free_udp, "udp", "raw", false).unwrap();
    let h3_bind = IngressBinding::from_protocols("h3", free_h3, "udp", "http3", true).unwrap();

    assert_eq!(tcp_bind.path, PathKind::L4Direct);
    assert_eq!(h1_bind.path, PathKind::L7Handoff);
    assert_eq!(h2_bind.path, PathKind::L7Handoff);
    assert_eq!(udp_bind.path, PathKind::L4Direct);
    assert_eq!(h3_bind.path, PathKind::L7Handoff);

    assert!(tcp_bind.is_tcp());
    assert!(h1_bind.is_tcp());
    assert!(h2_bind.is_tcp());
    assert!(udp_bind.is_udp());
    assert!(h3_bind.is_udp());

    let mut engine = TrafficEngine::new();
    engine.add_binding(tcp_bind).unwrap();
    engine.add_binding(h1_bind).unwrap();
    engine.add_binding(h2_bind).unwrap();
    engine.add_binding(udp_bind).unwrap();
    engine.add_binding(h3_bind).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_task = tokio::spawn(async move {
        engine
            .run_all(
                shutdown_rx,
                |mut conn| async move {
                    let mut buf = [0u8; 8];
                    let _ = conn.read(&mut buf).await;
                    let _ = conn.write_all(b"tcp-ack").await;
                },
                |handoff| async move {
                    if handoff.listener_id() == "h1" {
                        let mut conn = handoff.into_connection();
                        let _ = conn.write_all(b"h1-ack").await;
                    } else if handoff.listener_id() == "h2" {
                        let mut conn = handoff.into_connection();
                        let _ = conn.write_all(b"h2-ack").await;
                    }
                },
                |_id, sock, dgram| async move {
                    let _ = sock.send_to(b"udp-ack", dgram.peer()).await;
                },
                |handoff: UdpL7Handoff| async move {
                    assert_eq!(handoff.listener_id(), "h3");
                    let _ = handoff.send_response(b"h3-ack").await;
                },
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Test TCP
    {
        let mut s = TcpStream::connect(free_tcp).await.unwrap();
        s.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 7];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"tcp-ack");
    }

    // 2. Test HTTP/1
    {
        let mut s = TcpStream::connect(free_h1).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut buf = [0u8; 6];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"h1-ack");
    }

    // 3. Test HTTP/2
    {
        let mut s = TcpStream::connect(free_h2).await.unwrap();
        s.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 6];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"h2-ack");
    }

    // 4. Test UDP
    {
        let client =
            UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap();
        client.send_to(b"ping", free_udp).await.unwrap();
        let mut buf = [0u8; 7];
        let (n, _) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"udp-ack");
    }

    // 5. Test HTTP/3 over UDP
    {
        let client =
            UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap();
        client.send_to(b"quic-data", free_h3).await.unwrap();
        let mut buf = [0u8; 6];
        let (n, _) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"h3-ack");
    }

    shutdown_tx.send(true).unwrap();
    let _ = engine_task.await.unwrap();
}
