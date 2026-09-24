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
    // { "id": "http", "address": "0.0.0.0:80", "protocol": "http", "tls": { "enabled": false } }
    let http_binding = IngressBinding::new("http", dummy_ephemeral, "http", false, None).unwrap();
    let http_listener = IngressListener::bind(http_binding).unwrap();
    let http_addr = http_listener.local_addr();

    // 2. Mirror listeners.json "https" listener:
    // { "id": "https", "address": "0.0.0.0:443", "protocol": "http", "tls": { "enabled": true, "profile": "default" } }
    let https_binding = IngressBinding::new(
        "https",
        dummy_ephemeral,
        "http",
        true,
        Some("default".into()),
    )
    .unwrap();
    let https_listener = IngressListener::bind(https_binding).unwrap();
    let https_addr = https_listener.local_addr();

    // 3. Mirror listeners.json "tcp-ingress" listener:
    // { "id": "tcp-ingress", "address": "0.0.0.0:9000", "protocol": "tcp", "tls": { "enabled": false } }
    let tcp_binding =
        IngressBinding::new("tcp-ingress", dummy_ephemeral, "tcp", false, None).unwrap();
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
                        if handoff.listener_id() == "http" {
                            assert_eq!(handoff.path_hint(), PathKind::Http);
                            assert_eq!(handoff.tls_profile(), None);
                        } else if handoff.listener_id() == "https" {
                            assert_eq!(handoff.path_hint(), PathKind::Tls);
                            assert_eq!(handoff.tls_profile(), Some("default"));
                        }

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
            IngressBinding::new("http-public", dummy_ephemeral, "http", false, None).unwrap(),
        )
        .unwrap();
    engine
        .add_binding(
            IngressBinding::new("http-internal", dummy_ephemeral, "http", false, None).unwrap(),
        )
        .unwrap();

    // 2 TCP listeners on different ports
    engine
        .add_binding(IngressBinding::new("tcp-db-pg", dummy_ephemeral, "tcp", false, None).unwrap())
        .unwrap();
    engine
        .add_binding(IngressBinding::new("tcp-redis", dummy_ephemeral, "tcp", false, None).unwrap())
        .unwrap();

    // 2 UDP listeners on different ports
    engine
        .add_binding(IngressBinding::new("udp-dns", dummy_ephemeral, "udp", false, None).unwrap())
        .unwrap();
    engine
        .add_binding(
            IngressBinding::new("udp-metrics", dummy_ephemeral, "udp", false, None).unwrap(),
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
