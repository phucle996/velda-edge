//! Integration tests verifying TcpL7Handoff integration with velda-transport.

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use velda_composer::{ApplicationProtocol, CompiledListenerComposition, Composer};
use velda_transport::{IngressBinding, IngressListener, TrafficEngine};

#[tokio::test]
async fn test_traffic_engine_to_composer_handoff_lifecycle() {
    let dummy_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // 1. Setup listeners in TrafficEngine: one cleartext HTTP, one HTTPS
    let http_binding =
        IngressBinding::from_protocols("http-listener", dummy_addr, "tcp", "http1", false).unwrap();
    let http_listener = IngressListener::bind(http_binding).unwrap();
    let http_addr = http_listener.local_addr();

    let https_binding =
        IngressBinding::from_protocols("https-listener", dummy_addr, "tcp", "http2", true).unwrap();
    let https_listener = IngressListener::bind(https_binding).unwrap();
    let https_addr = https_listener.local_addr();

    let mut engine = TrafficEngine::new();
    engine.add_tcp_listener(http_listener);
    engine.add_tcp_listener(https_listener);

    // 2. Setup Composer with compiled configurations
    let mut composer = Composer::new();
    composer.register_listener(CompiledListenerComposition::new(
        "http-listener",
        ApplicationProtocol::Http1,
        false,
        velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    ));
    composer.register_listener(CompiledListenerComposition::new(
        "https-listener",
        ApplicationProtocol::Http2,
        true,
        velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    ));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (tx_outcome, mut rx_outcome) = tokio::sync::mpsc::channel(16);

    let composer_arc = std::sync::Arc::new(composer);

    // 3. Run TrafficEngine accept loop forwarding handoffs to Composer
    let comp_clone = std::sync::Arc::clone(&composer_arc);
    let engine_task = tokio::spawn(async move {
        engine
            .run(
                shutdown_rx,
                |_conn| async move {},
                move |handoff| {
                    let comp = std::sync::Arc::clone(&comp_clone);
                    let tx = tx_outcome.clone();
                    async move {
                        let outcome = comp.compose_tcp_handoff(handoff).unwrap();
                        let is_tls = outcome.is_tls_required();
                        let proto = outcome.protocol();
                        let listener_id = outcome.context().listener_id.clone();

                        let mut conn = outcome.into_connection();
                        let _ = conn.write_all(b"composed-ack").await;

                        let _ = tx.send((listener_id, is_tls, proto)).await;
                    }
                },
            )
            .await
    });

    // 4. Client 1 connects to HTTP port
    let client1 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(http_addr).await.unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut buf = [0u8; 12];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"composed-ack");
    });

    // 5. Client 2 connects to HTTPS port
    let client2 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(https_addr).await.unwrap();
        stream
            .write_all(b"\x16\x03\x01\x00\x05clienthello")
            .await
            .unwrap();
        let mut buf = [0u8; 12];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"composed-ack");
    });

    client1.await.unwrap();
    client2.await.unwrap();

    // Verify outcomes received in Composer
    let (id1, tls1, proto1) = rx_outcome.recv().await.unwrap();
    let (id2, tls2, proto2) = rx_outcome.recv().await.unwrap();

    // Check that one was cleartext HTTP1 and one was HTTPS
    let results = [(id1, tls1, proto1), (id2, tls2, proto2)];
    assert!(results.iter().any(|(id, tls, proto)| id == "http-listener"
        && !*tls
        && *proto == ApplicationProtocol::Http1));
    assert!(results.iter().any(|(id, tls, proto)| id == "https-listener"
        && *tls
        && *proto == ApplicationProtocol::Http2));

    // Cleanup
    let _ = shutdown_tx.send(true);
    let _ = engine_task.await;
}
