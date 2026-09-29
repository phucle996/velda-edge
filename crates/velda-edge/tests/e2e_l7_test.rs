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
async fn test_end_to_end_l7_http_routing_and_forwarding() {
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
        },
        tls: ListenerTlsConfig { enabled: false },
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
async fn test_end_to_end_l7_grpc_routing_and_unimplemented_semantics() {
    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_grpc.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Listener configured for HTTP/2
    let listeners = vec![ListenerConfig {
        id: "grpc-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "grpc".into(),
            version: None,
        },
        tls: ListenerTlsConfig { enabled: false },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    // Empty routes -> route miss test for gRPC semantics
    let routes: Vec<RouteConfig> = vec![];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect via HTTP/2 cleartext (h2c)
    let tcp_stream = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(tcp_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    // Send gRPC request to unmapped service
    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/order.OrderService/CreateOrder")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();

    let (response_fut, _) = client.send_request(req, true).unwrap();
    let response = response_fut.await.unwrap();

    // gRPC semantics: HTTP 200 OK + grpc-status: 12 (UNIMPLEMENTED)
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/grpc"
    );
    assert_eq!(
        response.headers().get("grpc-status").unwrap(),
        "12" // UNIMPLEMENTED
    );

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}

#[tokio::test]
async fn test_end_to_end_l7_grpc_routing_and_forwarding() {
    // 1. Mock HTTP/2 gRPC backend server
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut h2_server = h2::server::handshake(sock).await.unwrap();
                while let Some(res) = h2_server.accept().await {
                    let (_req, mut respond) = res.unwrap();
                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", "0")
                        .header("grpc-message", "OK")
                        .body(())
                        .unwrap();
                    let mut send_stream = respond.send_response(response, false).unwrap();
                    send_stream
                        .send_data(
                            bytes::Bytes::from_static(b"\x00\x00\x00\x00\x07success"),
                            true,
                        )
                        .unwrap();
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
    let socket_path = tmp.path().join("edge_grpc_fwd.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "grpc-fwd-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "grpc".into(),
            version: None,
        },
        tls: ListenerTlsConfig { enabled: false },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "grpc-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "grpc".into(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "grpc-order-route".into(),
        kind: "l7".into(),
        listener: "grpc-fwd-in".into(),
        match_rule: RouteMatch {
            protocol: Some("grpc".into()),
            path: Some("order.OrderService".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "grpc-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let tcp_stream = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(tcp_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/order.OrderService/CreateOrder")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();

    let (response_fut, _) = client.send_request(req, true).unwrap();
    let response = response_fut.await.unwrap();

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/grpc"
    );
    assert_eq!(
        response.headers().get("grpc-status").unwrap(),
        "0" // OK from mock gRPC backend
    );

    let mut body = response.into_body();
    let chunk = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk[..], b"\x00\x00\x00\x00\x07success");

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}

#[tokio::test]
async fn test_end_to_end_l7_grpc_server_streaming() {
    // 1. Mock HTTP/2 gRPC streaming backend server
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut h2_server = h2::server::handshake(sock).await.unwrap();
                while let Some(res) = h2_server.accept().await {
                    let (_req, mut respond) = res.unwrap();
                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap();
                    let mut send_stream = respond.send_response(response, false).unwrap();

                    // Stream 3 sequential gRPC message frames
                    send_stream
                        .send_data(
                            bytes::Bytes::from_static(b"\x00\x00\x00\x00\x05msg_1"),
                            false,
                        )
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    send_stream
                        .send_data(
                            bytes::Bytes::from_static(b"\x00\x00\x00\x00\x05msg_2"),
                            false,
                        )
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    send_stream
                        .send_data(
                            bytes::Bytes::from_static(b"\x00\x00\x00\x00\x05msg_3"),
                            false,
                        )
                        .unwrap();

                    // Send final trailers with grpc-status: 0
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                    trailers.insert(
                        "grpc-message",
                        http::HeaderValue::from_static("Stream complete"),
                    );
                    send_stream.send_trailers(trailers).unwrap();
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
    let socket_path = tmp.path().join("edge_grpc_stream.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "grpc-stream-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "grpc".into(),
            version: None,
        },
        tls: ListenerTlsConfig { enabled: false },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "grpc-stream-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "grpc".into(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "grpc-stream-route".into(),
        kind: "l7".into(),
        listener: "grpc-stream-in".into(),
        match_rule: RouteMatch {
            protocol: Some("grpc".into()),
            path: Some("stream.StreamService".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "grpc-stream-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let tcp_stream = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(tcp_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/stream.StreamService/FetchItems")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();

    let (response_fut, _) = client.send_request(req, true).unwrap();
    let response = response_fut.await.unwrap();

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/grpc"
    );

    let mut body = response.into_body();

    // Verify all 3 streamed messages arrive sequentially
    let chunk1 = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk1[..], b"\x00\x00\x00\x00\x05msg_1");

    let chunk2 = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk2[..], b"\x00\x00\x00\x00\x05msg_2");

    let chunk3 = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk3[..], b"\x00\x00\x00\x00\x05msg_3");

    // Verify trailers arrive with grpc-status: 0
    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    assert_eq!(trailers.get("grpc-message").unwrap(), "Stream complete");

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}

#[tokio::test]
async fn test_end_to_end_l7_grpc_unary_one_way() {
    // 1. Mock HTTP/2 gRPC Unary backend server (1 chiều: 1 request message -> 1 response message)
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut h2_server = h2::server::handshake(sock).await.unwrap();
                while let Some(res) = h2_server.accept().await {
                    let (req, mut respond) = res.unwrap();
                    let mut recv_stream = req.into_body();

                    // Read incoming unary request data
                    let chunk = recv_stream.data().await.unwrap().unwrap();
                    assert_eq!(&chunk[..], b"\x00\x00\x00\x00\x12unary_request_data");

                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap();
                    let mut send_stream = respond.send_response(response, false).unwrap();

                    // Send exactly 1 response message
                    send_stream
                        .send_data(
                            bytes::Bytes::from_static(b"\x00\x00\x00\x00\x13unary_response_data"),
                            false,
                        )
                        .unwrap();

                    // Send final trailers with grpc-status: 0
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                    trailers.insert(
                        "grpc-message",
                        http::HeaderValue::from_static("Unary success"),
                    );
                    send_stream.send_trailers(trailers).unwrap();
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
    let socket_path = tmp.path().join("edge_grpc_unary.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "grpc-unary-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "grpc".into(),
            version: None,
        },
        tls: ListenerTlsConfig { enabled: false },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "grpc-unary-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "grpc".into(),
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
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    let routes = vec![RouteConfig {
        id: "grpc-unary-route".into(),
        kind: "l7".into(),
        listener: "grpc-unary-in".into(),
        match_rule: RouteMatch {
            protocol: Some("grpc".into()),
            path: Some("unary.EchoService".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "grpc-unary-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let tcp_stream = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(tcp_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/unary.EchoService/Echo")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();

    let (response_fut, mut send_stream) = client.send_request(req, false).unwrap();
    // Send 1 chiều request data
    send_stream
        .send_data(
            bytes::Bytes::from_static(b"\x00\x00\x00\x00\x12unary_request_data"),
            true,
        )
        .unwrap();

    let response = response_fut.await.unwrap();

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/grpc"
    );

    let mut body = response.into_body();

    // Verify 1 chiều response message
    let chunk = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk[..], b"\x00\x00\x00\x00\x13unary_response_data");

    // Verify trailers arrive with grpc-status: 0
    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    assert_eq!(trailers.get("grpc-message").unwrap(), "Unary success");

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}
