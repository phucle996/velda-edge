use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use velda_transport::{
    IngressBinding, TcpBinding, TcpIngress, TrafficEngine, UdpSocket, UdpSocketConfig,
};

fn tcp_binding(id: &str, addr: SocketAddr) -> IngressBinding {
    IngressBinding::from_transport(id, addr, "tcp", false).unwrap()
}

fn udp_binding(id: &str, addr: SocketAddr, tls: bool) -> IngressBinding {
    IngressBinding::from_transport(id, addr, "udp", tls).unwrap()
}

#[tokio::test]
async fn test_traffic_engine_pre_bound_tcp_ingress_carries_listener_id() {
    let dummy_ephemeral: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let http = TcpIngress::bind(TcpBinding::new("http", dummy_ephemeral, false)).unwrap();
    let http_addr = http.local_addr();
    let https = TcpIngress::bind(TcpBinding::new("https", dummy_ephemeral, true)).unwrap();
    let https_addr = https.local_addr();
    let raw = TcpIngress::bind(TcpBinding::new("tcp-ingress", dummy_ephemeral, false)).unwrap();
    let raw_addr = raw.local_addr();

    let mut engine = TrafficEngine::new();
    engine.add_tcp_ingress(http);
    engine.add_tcp_ingress(https);
    engine.add_tcp_ingress(raw);
    assert_eq!(engine.tcp_listener_count(), 3);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let web_count = Arc::new(AtomicU32::new(0));
    let raw_count = Arc::new(AtomicU32::new(0));
    let web_clone = Arc::clone(&web_count);
    let raw_clone = Arc::clone(&raw_count);

    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                {
                    let web = Arc::clone(&web_clone);
                    let raw = Arc::clone(&raw_clone);
                    move |_id| {
                        let web = Arc::clone(&web);
                        let raw = Arc::clone(&raw);
                        move |mut conn| {
                            let web = Arc::clone(&web);
                            let raw = Arc::clone(&raw);
                            async move {
                                let listener_id = conn.listener_id().unwrap().to_string();
                                let mut buf = [0u8; 32];
                                let _ = conn.read(&mut buf).await;
                                if listener_id == "http" || listener_id == "https" {
                                    web.fetch_add(1, Ordering::SeqCst);
                                    let _ = conn.write_all(b"web-ok").await;
                                } else {
                                    assert_eq!(listener_id, "tcp-ingress");
                                    raw.fetch_add(1, Ordering::SeqCst);
                                    let _ = conn.write_all(b"raw-ok").await;
                                }
                            }
                        }
                    }
                },
                |_| |_id, _socket, _dgram| async move {},
            )
            .await
    });

    for (addr, payload, expect) in [
        (http_addr, &b"GET / HTTP/1.1\r\n\r\n"[..], b"web-ok"),
        (https_addr, &b"TLS ClientHello payload"[..], b"web-ok"),
        (raw_addr, &b"raw tcp bytes"[..], b"raw-ok"),
    ] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(payload).await.unwrap();
        let mut buf = [0u8; 6];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, expect);
    }

    assert_eq!(web_count.load(Ordering::SeqCst), 2);
    assert_eq!(raw_count.load(Ordering::SeqCst), 1);

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

    for id in ["http-public", "http-internal", "tcp-db-pg", "tcp-redis"] {
        engine
            .add_binding(tcp_binding(id, dummy_ephemeral))
            .unwrap();
    }
    for id in ["udp-dns", "udp-metrics"] {
        engine
            .add_binding(udp_binding(id, dummy_ephemeral, false))
            .unwrap();
    }

    assert_eq!(engine.tcp_listener_count(), 4);
    assert_eq!(engine.udp_listener_count(), 2);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                |_| {
                    |mut conn| async move {
                        let mut buf = [0u8; 32];
                        if let Ok(n) = conn.read(&mut buf).await {
                            let resp = format!("ack-tcp-{}", String::from_utf8_lossy(&buf[..n]));
                            let _ = conn.write_all(resp.as_bytes()).await;
                        }
                    }
                },
                |_| {
                    |id, socket, dgram| async move {
                        let resp =
                            format!("ack-udp-{id}-{}", String::from_utf8_lossy(dgram.data()));
                        let _ = socket.send_to(resp.as_bytes(), dgram.peer()).await;
                    }
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
    engine
        .add_binding(tcp_binding("listener-1", free_addr1))
        .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                |_| {
                    |mut conn| async move {
                        let mut buf = [0u8; 16];
                        if let Ok(n) = conn.read(&mut buf).await {
                            let msg = format!("ack-{}", String::from_utf8_lossy(&buf[..n]));
                            let _ = conn.write_all(msg.as_bytes()).await;
                        }
                    }
                },
                |_| |_id, _socket, _dgram| async move {},
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
    handle
        .reconcile(vec![tcp_binding("listener-2", free_addr2)])
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
async fn test_multi_protocol_engine_tcp_udp_dispatch_by_listener_id() {
    let l_tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let free_tcp = l_tcp.local_addr().unwrap();
    let l_h1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let free_h1 = l_h1.local_addr().unwrap();
    let l_h2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let free_h2 = l_h2.local_addr().unwrap();
    drop(l_tcp);
    drop(l_h1);
    drop(l_h2);

    let s_udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let free_udp = s_udp.local_addr().unwrap();
    let s_h3 = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let free_h3 = s_h3.local_addr().unwrap();
    drop(s_udp);
    drop(s_h3);

    let tcp_bind = tcp_binding("tcp", free_tcp);
    let h1_bind = tcp_binding("h1", free_h1);
    let h2_bind = tcp_binding("h2", free_h2);
    let udp_bind = udp_binding("udp", free_udp, false);
    let h3_bind = udp_binding("h3", free_h3, true);

    assert!(tcp_bind.is_tcp() && h1_bind.is_tcp() && h2_bind.is_tcp());
    assert!(udp_bind.is_udp() && h3_bind.is_udp());
    assert!(h3_bind.tls_enabled());
    assert!(!tcp_bind.tls_enabled());

    let mut engine = TrafficEngine::new();
    engine.add_binding(tcp_bind).unwrap();
    engine.add_binding(h1_bind).unwrap();
    engine.add_binding(h2_bind).unwrap();
    engine.add_binding(udp_bind).unwrap();
    engine.add_binding(h3_bind).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                |id| {
                    let reply: &'static [u8] = match id {
                        "h1" => b"h1-ack",
                        "h2" => b"h2-ack",
                        _ => b"tcp-ack",
                    };
                    move |mut conn| async move {
                        let mut buf = [0u8; 32];
                        let _ = conn.read(&mut buf).await;
                        let _ = conn.write_all(reply).await;
                    }
                },
                |id| {
                    let reply: &'static [u8] = if id == "h3" { b"h3-ack" } else { b"udp-ack" };
                    move |_id, sock, dgram| async move {
                        let _ = sock.send_to(reply, dgram.peer()).await;
                    }
                },
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for (addr, payload, expect) in [
        (free_tcp, &b"hello"[..], &b"tcp-ack"[..]),
        (free_h1, &b"GET / HTTP/1.1\r\n\r\n"[..], &b"h1-ack"[..]),
        (
            free_h2,
            &b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"[..],
            &b"h2-ack"[..],
        ),
    ] {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(payload).await.unwrap();
        let mut buf = vec![0u8; expect.len()];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, expect);
    }

    for (addr, payload, expect) in [
        (free_udp, &b"ping"[..], &b"udp-ack"[..]),
        (free_h3, &b"quic-data"[..], &b"h3-ack"[..]),
    ] {
        let client =
            UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap();
        client.send_to(payload, addr).await.unwrap();
        let mut buf = [0u8; 16];
        let (n, _) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], expect);
    }

    shutdown_tx.send(true).unwrap();
    let _ = engine_task.await.unwrap();
}
