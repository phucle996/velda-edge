use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    compile_listeners_to_binary,
};
use velda_sync::post_sync::route::{
    RouteConfig, RouteMatch, RouteTimeouts, compile_routes_to_binary,
};
use velda_sync::post_sync::upstream::{
    EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    compile_upstreams_to_binary,
};

#[tokio::test]
async fn test_http1_routing() {
    // 1. Mock cleartext HTTP backend echo server
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap();
                let req_str = String::from_utf8_lossy(&buf[..n]);

                if req_str.contains("POST /api/v1/orders") {
                    let resp = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 22\r\n\r\n{\"order_id\":\"ord_123\"}";
                    let _ = sock.write_all(resp).await;
                } else if req_str.contains("GET /api/v1/users") {
                    let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 21\r\n\r\n[{\"id\":1,\"name\":\"A\"}]";
                    let _ = sock.write_all(resp).await;
                } else {
                    let resp = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
                    let _ = sock.write_all(resp).await;
                }
                let _ = sock.flush().await;
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
    let socket_path = tmp.path().join("edge_l7.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Listener: TCP + HTTP/1.1 application
    let listeners = vec![ListenerConfig {
        id: "http-in".into(),
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

    // Upstream definition
    let upstreams = vec![UpstreamConfig {
        id: "user-order-backend".into(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    // Routes: prefix "/api/v1"
    let routes = vec![RouteConfig {
        id: "api-v1-route".into(),
        kind: "l7".into(),
        listener: "http-in".into(),
        match_rule: RouteMatch {
            protocol: None,
            path_prefix: Some("/api/v1".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "user-order-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    // Start supervisor
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Test Case 1: GET /api/v1/users (matched route -> 200 OK)
    {
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        client
            .write_all(
                b"GET /api/v1/users HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();

        let mut buf = vec![0u8; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf[..n].to_vec()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("[{\"id\":1,\"name\":\"A\"}]"));
    }

    // Test Case 2: POST /api/v1/orders with JSON body (matched route -> 201 Created)
    {
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        let req_body = b"{\"item\":\"server\",\"qty\":2}";
        let req = format!(
            "POST /api/v1/orders HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            req_body.len(),
            std::str::from_utf8(req_body).unwrap()
        );
        client.write_all(req.as_bytes()).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf[..n].to_vec()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 201 Created"));
        assert!(resp.contains("{\"order_id\":\"ord_123\"}"));
    }

    // Test Case 3: GET /not_found (route miss -> 404 Not Found)
    {
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        client
            .write_all(b"GET /not_found HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf[..n].to_vec()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"));
        assert!(resp.contains("404 Not Found: no matching route"));
    }

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}

#[tokio::test]
async fn test_http1_headers() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    // Backend verifies that hop-by-hop headers were stripped and X-Forwarded-* headers were injected
    tokio::spawn(async move {
        let (mut sock, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let n = sock.read(&mut buf).await.unwrap();
        let req_str = String::from_utf8_lossy(&buf[..n]);

        assert!(req_str.contains("GET /api/v1/test HTTP/1.1"));
        assert!(!req_str.to_ascii_lowercase().contains("x-custom-hop:"));
        assert!(!req_str.to_ascii_lowercase().contains("keep-alive:"));
        // Anti-spoofing verification: client spoofed values MUST be completely removed
        assert!(!req_str.contains("203.0.113.195"));
        assert!(!req_str.contains("evil.attacker.com"));
        assert!(!req_str.to_ascii_lowercase().contains("x-forwarded-ssl:"));
        // Authoritative values strictly verified
        assert!(
            req_str
                .to_ascii_lowercase()
                .contains("x-forwarded-for: 127.0.0.1")
        );
        assert!(
            req_str
                .to_ascii_lowercase()
                .contains("x-forwarded-proto: http")
        );
        assert!(
            req_str
                .to_ascii_lowercase()
                .contains("x-forwarded-host: localhost")
        );
        assert!(req_str.to_ascii_lowercase().contains("x-forwarded-port:"));
        assert!(
            req_str
                .to_ascii_lowercase()
                .contains("x-real-ip: 127.0.0.1")
        );
        assert!(
            req_str
                .to_ascii_lowercase()
                .contains("forwarded: for=127.0.0.1;proto=http")
        );

        // Return response with a hop-by-hop header to test egress stripping
        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close, X-Upstream-Hop\r\nX-Upstream-Hop: secret\r\n\r\nOK";
        sock.write_all(resp).await.unwrap();
        sock.flush().await.unwrap();
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h1_headers.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h1-headers-in".into(),
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
        id: "h1-headers-up".into(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "h1-headers-route".into(),
        kind: "l7".into(),
        listener: "h1-headers-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/api/v1/test".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-headers-up".into(),
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
        .write_all(b"GET /api/v1/test HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive, X-Custom-Hop\r\nX-Custom-Hop: sensitive\r\nKeep-Alive: timeout=5\r\nX-Forwarded-For: 203.0.113.195\r\nX-Real-IP: 203.0.113.195\r\nX-Forwarded-Host: evil.attacker.com\r\nX-Forwarded-Ssl: on\r\nForwarded: for=203.0.113.195;proto=https\r\n\r\n")
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024];
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8(buf[..n].to_vec()).unwrap();

    assert!(resp.starts_with("HTTP/1.1 200 OK"));
    assert!(resp.contains("OK"));
    assert!(!resp.to_ascii_lowercase().contains("x-upstream-hop:"));

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_http1_server_streaming() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    // Backend streams SSE chunks using HTTP/1.1 chunked encoding with delays
    tokio::spawn(async move {
        let (mut sock, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();

        // Write chunked response head
        let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
        sock.write_all(head).await.unwrap();
        sock.flush().await.unwrap();

        // Chunk 1
        sock.write_all(b"13\r\ndata: token_alpha\n\n\r\n")
            .await
            .unwrap();
        sock.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Chunk 2
        sock.write_all(b"12\r\ndata: token_beta\n\n\r\n")
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Terminal chunk
        sock.write_all(b"0\r\n\r\n").await.unwrap();
        sock.flush().await.unwrap();
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h1_stream.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h1-stream-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http1".into(),
            version: None,
            streaming: velda_sync::StreamingMode::SERVER,
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
        id: "h1-stream-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http1".into(),
            streaming: velda_sync::StreamingMode::SERVER,
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "h1-stream-route".into(),
        kind: "l7".into(),
        listener: "h1-stream-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/sse".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-stream-up".into(),
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
        .write_all(b"GET /sse HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut full_output = Vec::new();
    let mut buf = [0u8; 512];
    loop {
        let n = client.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        full_output.extend_from_slice(&buf[..n]);
        if full_output.ends_with(b"0\r\n\r\n") {
            break;
        }
    }

    let output_str = String::from_utf8_lossy(&full_output);
    assert!(output_str.starts_with("HTTP/1.1 200 OK"));
    assert!(output_str.contains("transfer-encoding: chunked"));
    assert!(output_str.contains("data: token_alpha\n\n"));
    assert!(output_str.contains("data: token_beta\n\n"));

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_http1_downstream_cancellation() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let (disconnect_tx, mut disconnect_rx) = tokio::sync::mpsc::channel::<bool>(1);

    // Backend streams chunks and detects when downstream connection is cancelled
    tokio::spawn(async move {
        let (mut sock, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();

        let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        sock.write_all(head).await.unwrap();
        sock.flush().await.unwrap();

        // Write first chunk
        sock.write_all(b"5\r\nfirst\r\n").await.unwrap();
        sock.flush().await.unwrap();

        // Loop trying to write; should detect broken pipe once client drops
        let mut failed = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            if sock.write_all(b"5\r\npulse\r\n").await.is_err() {
                failed = true;
                break;
            }
        }
        let _ = disconnect_tx.send(failed).await;
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h1_cancel.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h1-cancel-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http1".into(),
            version: None,
            streaming: velda_sync::StreamingMode::SERVER,
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
        id: "h1-cancel-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http1".into(),
            streaming: velda_sync::StreamingMode::SERVER,
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "h1-cancel-route".into(),
        kind: "l7".into(),
        listener: "h1-cancel-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/stream".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-cancel-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client connects, sends request, reads partial response, then abruptly drops
    {
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        client
            .write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut buf = [0u8; 128];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0);
        // Explicitly drop client to simulate unexpected disconnection
        drop(client);
    }

    // Verify upstream detected cancellation
    let cancelled = tokio::time::timeout(Duration::from_secs(2), disconnect_rx.recv())
        .await
        .unwrap()
        .unwrap_or(false);

    assert!(
        cancelled,
        "Upstream backend must receive disconnection when client cancels"
    );

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_http1_chunked_upload_rejection() {
    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_reject_chunked.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Listener with streaming disabled (the default)
    let listeners = vec![ListenerConfig {
        id: "h1-nostream-in".into(),
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
        id: "h1-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http1".into(),
            streaming: velda_sync::StreamingMode::DISABLED,
        },
        target: None,
        resolver: None,
        endpoints: vec![EndpointConfig {
            address: "127.0.0.1:19999".to_string(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "r-upload".into(),
        kind: "l7".into(),
        listener: "h1-nostream-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/upload".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client connects and attempts chunked upload on a non-streaming listener
    let mut client = {
        let mut conn = None;
        for _ in 0..20 {
            if let Ok(c) = TcpStream::connect(gateway_addr).await {
                conn = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        conn.expect("failed to connect to gateway_addr")
    };
    let chunked_req = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    client.write_all(chunked_req).await.unwrap();

    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);

    // Option A: Edge immediately rejects with 403 Forbidden
    assert!(
        resp.contains("403 Forbidden"),
        "Expected 403 Forbidden on chunked upload to non-streaming listener, got: {resp}"
    );

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_http1_streaming_mismatch_bootstrap() {
    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_mismatch_reject.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Listener with streaming DISABLED
    let listeners = vec![ListenerConfig {
        id: "h1-nostream-in".into(),
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

    // Upstream configured with streaming SERVER (misconfiguration by admin)
    let upstreams = vec![UpstreamConfig {
        id: "h1-sse-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http1".into(),
            streaming: velda_sync::StreamingMode::SERVER,
        },
        target: None,
        resolver: None,
        endpoints: vec![EndpointConfig {
            address: "127.0.0.1:19998".to_string(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "r-sse".into(),
        kind: "l7".into(),
        listener: "h1-nostream-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/sse".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-sse-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    // Invariant: Invalid cross-domain streaming policy is rejected at bootstrap!
    let bootstrap_res = EdgeSupervisor::bootstrap(config);
    let err = bootstrap_res
        .err()
        .expect("Bootstrap must fail on invalid config");
    assert!(
        matches!(err, velda_edge::EdgeError::InvalidConfig { .. }),
        "Expected InvalidConfig, got: {err:?}"
    );
}

#[tokio::test]
async fn test_http1_unexpected_chunked_502() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    // Backend unexpectedly returns chunked response to a buffered listener
    tokio::spawn(async move {
        let (mut sock, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();

        let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        sock.write_all(head).await.unwrap();
        sock.flush().await.unwrap();
    });

    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_unexpected_chunked.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Both listener and upstream declare DISABLED (valid config)
    let listeners = vec![ListenerConfig {
        id: "h1-buf-in".into(),
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
        id: "h1-buf-up".into(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "r-buf".into(),
        kind: "l7".into(),
        listener: "h1-buf-in".into(),
        match_rule: RouteMatch {
            path_prefix: Some("/data".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h1-buf-up".into(),
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
        .write_all(b"GET /data HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);

    // Runtime wire inspection returns 502 Bad Gateway
    assert!(
        resp.contains("502 Bad Gateway"),
        "Expected 502 Bad Gateway on unexpected upstream chunked response, got: {resp}"
    );

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}
