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
    ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    compile_listeners_to_binary,
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
            streaming: velda_sync::StreamingMode::DISABLED,
        },
        tls: ListenerTlsConfig { enabled: true },
        http1: None,
        http2: None,
        grpc: None,
        http3: None,
        raw: None,
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
            streaming: velda_sync::StreamingMode::DISABLED,
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
        pool: None,
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

    assert_eq!(shared.load().config.listeners.len(), 1);
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
        insecure_skip_verify: false,
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
            streaming: velda_sync::StreamingMode::DISABLED,
        },
        tls: ListenerTlsConfig { enabled: true },
        http1: None,
        http2: None,
        grpc: None,
        http3: None,
        raw: None,
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
            streaming: velda_sync::StreamingMode::DISABLED,
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
        pool: None,
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
        insecure_skip_verify: false,
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

#[tokio::test]
async fn test_end_to_end_tls_http1_upstream_forwarding() {
    let backend_cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let backend_cert_pem = backend_cert.cert.pem();
    let backend_key_pem = backend_cert.signing_key.serialize_pem();

    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let server_tls_config = velda_tls::ServerTlsConfig {
        sni: vec!["localhost".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
        cert_pem: backend_cert_pem.clone(),
        key_pem: backend_key_pem,
        client_ca_pem: None,
    };
    let backend_tls_engine = velda_tls::TlsServerEngine::new_with_params(
        &[server_tls_config],
        &velda_tls::TlsServerParams::from_hardware(),
    )
    .unwrap();

    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            let engine = backend_tls_engine.clone();
            tokio::spawn(async move {
                if let Ok(mut tls_stream) = engine.accept(sock).await {
                    let mut buf = [0u8; 1024];
                    let n = tls_stream.read(&mut buf).await.unwrap_or(0);
                    if n > 0 {
                        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\nhello from tls!";
                        let _ = tls_stream.write_all(resp).await;
                        let _ = tls_stream.flush().await;
                    }
                }
            });
        }
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h1_upstream_tls.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h1-plain-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http1".into(),
            version: None,
            streaming: velda_sync::StreamingMode::DISABLED,
        },
        tls: ListenerTlsConfig { enabled: false },
        http1: None,
        http2: None,
        grpc: None,
        http3: None,
        raw: None,
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "h1-tls-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http1".into(),
            streaming: velda_sync::StreamingMode::DISABLED,
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
        tls: Some(velda_sync::post_sync::upstream::UpstreamTlsConfig {
            ca_pem: Some(backend_cert_pem),
            client_cert_pem: None,
            client_key_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec!["http/1.1".into()],
            sni: vec!["localhost".into()],
            insecure_skip_verify: false,
        }),
        pool: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "h1-tls-route".into(),
        kind: "l7".into(),
        listener: "h1-plain-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/secure".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-tls-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(gateway_addr).await.unwrap();
    client
        .write_all(b"GET /secure HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024];
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8(buf[..n].to_vec()).unwrap();

    assert!(resp.starts_with("HTTP/1.1 200 OK"));
    assert!(resp.contains("hello from tls!"));

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_end_to_end_tls_http2_upstream_forwarding() {
    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let backend_cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let backend_cert_pem = backend_cert.cert.pem();
    let backend_key_pem = backend_cert.signing_key.serialize_pem();

    let backend_tls_config = velda_tls::ServerTlsConfig {
        sni: vec!["localhost".into()],
        cert_pem: backend_cert_pem.clone(),
        key_pem: backend_key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
    };
    let backend_tls_engine = velda_tls::TlsServerEngine::new_with_params(
        &[backend_tls_config],
        &velda_tls::TlsServerParams::from_hardware(),
    )
    .unwrap();

    // Mock upstream HTTP/2 server wrapped in TLS with ALPN h2
    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            let engine = backend_tls_engine.clone();
            tokio::spawn(async move {
                if let Ok(tls_stream) = engine.accept(sock).await
                    && let Ok(mut h2_server) = h2::server::handshake(tls_stream).await
                {
                    while let Some(Ok((_req, mut respond))) = h2_server.accept().await {
                        let resp = http::Response::builder()
                            .status(http::StatusCode::OK)
                            .body(())
                            .unwrap();
                        let mut send = respond.send_response(resp, false).unwrap();
                        send.send_data(bytes::Bytes::from_static(b"hello from h2 tls!"), true)
                            .unwrap();
                    }
                }
            });
        }
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h2_upstream_tls.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h2-plain-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http2".into(),
            version: None,
            streaming: velda_sync::StreamingMode::DISABLED,
        },
        tls: ListenerTlsConfig { enabled: false },
        http1: None,
        http2: None,
        grpc: None,
        http3: None,
        raw: None,
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "h2-tls-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http2".into(),
            streaming: velda_sync::StreamingMode::DISABLED,
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
        tls: Some(velda_sync::post_sync::upstream::UpstreamTlsConfig {
            ca_pem: Some(backend_cert_pem),
            client_cert_pem: None,
            client_key_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            sni: vec!["localhost".into()],
            insecure_skip_verify: false,
        }),
        pool: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "h2-tls-route".into(),
        kind: "l7".into(),
        listener: "h2-plain-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/h2-secure".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h2-tls-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect downstream HTTP/2 client
    let client_stream = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(client_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let request = http::Request::builder()
        .method("GET")
        .uri("http://localhost/h2-secure")
        .body(())
        .unwrap();

    let (response, _) = client.send_request(request, true).unwrap();
    let response = response.await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);

    let mut body = response.into_body();
    let chunk = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk[..], b"hello from h2 tls!");

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_end_to_end_tls_grpc_upstream_forwarding() {
    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let backend_cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let backend_cert_pem = backend_cert.cert.pem();
    let backend_key_pem = backend_cert.signing_key.serialize_pem();

    let backend_tls_config = velda_tls::ServerTlsConfig {
        sni: vec!["localhost".into()],
        cert_pem: backend_cert_pem.clone(),
        key_pem: backend_key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
    };
    let backend_tls_engine = velda_tls::TlsServerEngine::new_with_params(
        &[backend_tls_config],
        &velda_tls::TlsServerParams::from_hardware(),
    )
    .unwrap();

    // Mock upstream gRPC server wrapped in TLS with ALPN h2
    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            let engine = backend_tls_engine.clone();
            tokio::spawn(async move {
                if let Ok(tls_stream) = engine.accept(sock).await
                    && let Ok(mut h2_server) = h2::server::handshake(tls_stream).await
                {
                    while let Some(Ok((_req, mut respond))) = h2_server.accept().await {
                        let resp = http::Response::builder()
                            .status(http::StatusCode::OK)
                            .header("content-type", "application/grpc")
                            .body(())
                            .unwrap();
                        let mut send = respond.send_response(resp, false).unwrap();
                        let mut resp_frame = bytes::BytesMut::new();
                        velda_grpc::frame::encode_grpc_frame(
                            b"grpc tls pong",
                            false,
                            &mut resp_frame,
                        );
                        send.send_data(resp_frame.freeze(), false).unwrap();

                        let mut trailers = http::HeaderMap::new();
                        trailers.insert("grpc-status", "0".parse().unwrap());
                        send.send_trailers(trailers).unwrap();
                    }
                }
            });
        }
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_grpc_upstream_tls.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "grpc-plain-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "grpc".into(),
            version: None,
            streaming: velda_sync::StreamingMode::DISABLED,
        },
        tls: ListenerTlsConfig { enabled: false },
        http1: None,
        http2: None,
        grpc: None,
        http3: None,
        raw: None,
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "grpc-tls-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "grpc".into(),
            streaming: velda_sync::StreamingMode::DISABLED,
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
        tls: Some(velda_sync::post_sync::upstream::UpstreamTlsConfig {
            ca_pem: Some(backend_cert_pem),
            client_cert_pem: None,
            client_key_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            sni: vec!["localhost".into()],
            insecure_skip_verify: false,
        }),
        pool: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "grpc-tls-route".into(),
        kind: "l7".into(),
        listener: "grpc-plain-in".into(),
        match_rule: RouteMatch {
            protocol: Some("grpc".into()),
            path: Some("test.Service".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "grpc-tls-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect downstream gRPC client over HTTP/2
    let client_stream = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(client_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let mut req_body = bytes::BytesMut::new();
    velda_grpc::frame::encode_grpc_frame(b"grpc ping", false, &mut req_body);

    let request = http::Request::builder()
        .method("POST")
        .uri("http://localhost/test.Service/Echo")
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();

    let (response, mut send_stream) = client.send_request(request, false).unwrap();
    send_stream.send_data(req_body.freeze(), true).unwrap();

    let response = response.await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/grpc"
    );

    let mut body = response.into_body();
    let chunk = body.data().await.unwrap().unwrap();
    assert!(!chunk.is_empty());

    let mut chunk_buf = bytes::BytesMut::from(&chunk[..]);
    let (is_compressed, msg) = velda_grpc::frame::decode_grpc_frame(&mut chunk_buf)
        .unwrap()
        .expect("frame should be present");
    assert!(!is_compressed);
    assert_eq!(&msg[..], b"grpc tls pong");

    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}
