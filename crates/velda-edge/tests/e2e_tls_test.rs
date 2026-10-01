use rcgen::generate_simple_self_signed;
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerLimitsConfig, ListenerTlsConfig,
    ListenerTransportConfig, compile_listeners_to_binary,
};
use velda_sync::post_sync::route::{
    RouteConfig, RouteMatch, RouteTimeouts, compile_routes_to_binary,
};
use velda_sync::post_sync::tls::{TlsConfig, compile_tls_to_binary};
use velda_sync::post_sync::upstream::{
    EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    compile_upstreams_to_binary,
};
use velda_tls::{ClientTlsConfig, TlsClientEngine};

#[tokio::test]
async fn test_end_to_end_tls_downstream_termination() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_tls_e2e.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // 0. Start a mock cleartext HTTP backend server
    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 28\r\n\r\n{\"gateway\":\"velda-edge-tls\"}";
                let _ = sock.write_all(resp).await;
                let _ = sock.flush().await;
            });
        }
    });

    // 1. Generate self-signed certificate for "localhost"
    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    // Pick an available ephemeral port
    let https_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    // 2. Compile listeners.bin with TLS enabled on port
    let listeners = vec![ListenerConfig {
        id: "https-in".into(),
        address: https_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http1".into(),
            version: Some("1.1".into()),
            streaming: velda_sync::StreamingMode::Disabled,
        },
        tls: ListenerTlsConfig { enabled: true },
        limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0x11u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    // 3. Compile tls.bin with the generated cert and SNI "localhost"
    let tls = vec![TlsConfig {
        sni: vec!["localhost".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
    }];
    let tls_bin = compile_tls_to_binary(&tls, 1, [0x22u8; 32]).unwrap();
    fs::write(runtime_dir.join("tls.bin"), tls_bin).unwrap();

    // Upstream definition pointing to mock backend
    let upstreams = vec![UpstreamConfig {
        id: "tls-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http1".into(),
            streaming: velda_sync::StreamingMode::Disabled,
        },
        target: None,
        resolver: None,
        endpoints: vec![EndpointConfig {
            address: backend_addr.to_string(),
            weight: 100,
        }],
        load_balancer: LoadBalancerConfig {
            algorithm: "round_robin".into(),
        },
        timeouts: UpstreamTimeouts {
            connect_ms: 1000,
            idle_ms: 10000,
            request_ms: None,
        },
        health_check: None,
        tls: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0x33u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    // Route definition for /api/status
    let routes = vec![RouteConfig {
        id: "tls-route-1".into(),
        kind: "l7".into(),
        listener: "https-in".into(),
        match_rule: RouteMatch {
            protocol: None,
            path: Some("/api/status".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "tls-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0x44u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    // 4. Cold-start bootstrap supervisor
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let shared = supervisor.shared_runtime().clone();

    assert_eq!(shared.load().listener_count(), 1);
    assert_eq!(shared.load().config.tls.len(), 1);
    assert!(
        shared.load().tls_server.is_some(),
        "Downstream TLS server engine must be compiled into RAM"
    );

    // 5. Run supervisor in background
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    // Allow supervisor to start accept loop
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 6. Connect client with TLS client engine
    let client_config = ClientTlsConfig {
        sni: vec!["localhost".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
        ca_pem: Some(cert_pem),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let client_engine = TlsClientEngine::new(&[client_config]).unwrap();

    let tcp_stream = TcpStream::connect(https_addr).await.unwrap();
    let mut tls_client_stream = client_engine
        .connect("localhost", tcp_stream)
        .await
        .expect("Client TLS handshake to Edge must succeed");

    // Verify ALPN protocol negotiated
    let (_, client_conn) = tls_client_stream.get_ref();
    assert_eq!(
        client_conn.alpn_protocol(),
        Some(b"http/1.1".as_slice()),
        "Negotiated ALPN must be http/1.1"
    );

    // Send HTTP/1.1 request over secure TLS tunnel
    tls_client_stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut resp = vec![0u8; 1024];
    let n = tls_client_stream.read(&mut resp).await.unwrap();
    let resp_str = String::from_utf8(resp[..n].to_vec()).unwrap();
    assert!(resp_str.contains("HTTP/1.1 200 OK"));
    assert!(resp_str.contains("velda-edge-tls"));

    // 7. Graceful shutdown
    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(
        run_res.is_ok(),
        "Edge supervisor must terminate cleanly: {:?}",
        run_res
    );
}

#[tokio::test]
async fn test_end_to_end_tls_h2_downstream() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h2_e2e.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // 0. Start a mock cleartext HTTP/2 backend server (h2c)
    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut h2_conn = match h2::server::handshake(sock).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                while let Some(result) = h2_conn.accept().await {
                    let (_, mut respond) = match result {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let body = b"{\"gateway\":\"velda-edge\",\"proto\":\"h2\"}";
                    let resp = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .body(())
                        .unwrap();
                    let mut send_stream = respond.send_response(resp, false).unwrap();
                    send_stream
                        .send_data(bytes::Bytes::from_static(body), true)
                        .unwrap();
                }
            });
        }
    });

    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let https_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let listeners = vec![ListenerConfig {
        id: "https-h2-in".into(),
        address: https_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http2".into(),
            version: None,
            streaming: velda_sync::StreamingMode::Disabled,
        },
        tls: ListenerTlsConfig { enabled: true },
        limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0x11u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let tls = vec![TlsConfig {
        sni: vec!["localhost".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
    }];
    let tls_bin = compile_tls_to_binary(&tls, 1, [0x22u8; 32]).unwrap();
    fs::write(runtime_dir.join("tls.bin"), tls_bin).unwrap();

    // Upstream definition pointing to mock backend
    let upstreams = vec![UpstreamConfig {
        id: "h2-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http2".into(),
            streaming: velda_sync::StreamingMode::Disabled,
        },
        target: None,
        resolver: None,
        endpoints: vec![EndpointConfig {
            address: backend_addr.to_string(),
            weight: 100,
        }],
        load_balancer: LoadBalancerConfig {
            algorithm: "round_robin".into(),
        },
        timeouts: UpstreamTimeouts {
            connect_ms: 1000,
            idle_ms: 10000,
            request_ms: None,
        },
        health_check: None,
        tls: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0x33u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    // Route definition for /api/status
    let routes = vec![RouteConfig {
        id: "h2-route-1".into(),
        kind: "l7".into(),
        listener: "https-h2-in".into(),
        match_rule: RouteMatch {
            protocol: None,
            path: Some("/api/status".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h2-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0x44u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client_config = ClientTlsConfig {
        sni: vec!["localhost".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(cert_pem),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let client_engine = TlsClientEngine::new(&[client_config]).unwrap();

    let tcp_stream = TcpStream::connect(https_addr).await.unwrap();
    let tls_client_stream = client_engine
        .connect("localhost", tcp_stream)
        .await
        .expect("Client TLS handshake to Edge must succeed");

    let (_, client_conn) = tls_client_stream.get_ref();
    assert_eq!(
        client_conn.alpn_protocol(),
        Some(b"h2".as_slice()),
        "Negotiated ALPN must be h2"
    );

    let (mut client, h2_conn) = h2::client::handshake(tls_client_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let req = http::Request::builder()
        .method("GET")
        .uri("https://localhost/api/status")
        .body(())
        .unwrap();

    let (response_fut, _) = client.send_request(req, true).unwrap();
    let response = response_fut.await.unwrap();
    let status = response.status();
    let mut body = response.into_body();
    let chunk = body.data().await.unwrap().unwrap();
    let body_str = String::from_utf8(chunk.to_vec()).unwrap();
    assert_eq!(status, http::StatusCode::OK);
    assert!(body_str.contains("velda-edge"));
    assert!(body_str.contains("proto"));

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}
