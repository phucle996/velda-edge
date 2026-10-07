use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
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
async fn test_http2_routing() {
    // 1. Mock cleartext HTTP/2 backend echo server
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut h2_conn = match h2::server::handshake(sock).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                while let Some(result) = h2_conn.accept().await {
                    let (req, mut respond) = match result {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    if req.uri().path() == "/api/v1/h2/echo" {
                        let resp = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/json")
                            .body(())
                            .unwrap();
                        let mut send_stream = respond.send_response(resp, false).unwrap();
                        send_stream
                            .send_data(bytes::Bytes::from_static(b"{\"status\":\"echo-ok\"}"), true)
                            .unwrap();
                    } else {
                        let resp = http::Response::builder().status(404).body(()).unwrap();
                        let _ = respond.send_response(resp, true);
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
    let socket_path = tmp.path().join("edge_l7_h2.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h2-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http2".into(),
            version: None,
            streaming: velda_sync::StreamingMode {
                client: true,
                server: true,
            },
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
        id: "h2-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http2".into(),
            streaming: velda_sync::StreamingMode {
                client: true,
                server: true,
            },
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
        id: "r-h2".into(),
        kind: "l7".into(),
        listener: "h2-in".into(),
        match_rule: RouteMatch {
            protocol: None,
            path_prefix: Some("/api/v1/h2".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h2-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect downstream cleartext H2 client
    let tcp = TcpStream::connect(gateway_addr).await.unwrap();
    let (mut client, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let req = http::Request::builder()
        .method("POST")
        .uri("http://localhost/api/v1/h2/echo")
        .header("host", "localhost")
        .body(())
        .unwrap();

    let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
    send_stream
        .send_data(bytes::Bytes::from_static(b"ping"), true)
        .unwrap();

    let (parts, mut body) = resp_fut.await.unwrap().into_parts();
    assert_eq!(parts.status, http::StatusCode::OK);
    let chunk = body.data().await.unwrap().unwrap();
    assert_eq!(&chunk[..], b"{\"status\":\"echo-ok\"}");

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}

#[tokio::test]
async fn test_http2_streaming_and_multiplexing() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((sock, _)) = backend_listener.accept().await {
            tokio::spawn(async move {
                let mut h2_conn = match h2::server::handshake(sock).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                while let Some(result) = h2_conn.accept().await {
                    let (req, mut respond) = match result {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    if req.uri().path() == "/api/v1/h2/sse" {
                        let resp = http::Response::builder()
                            .status(200)
                            .header("content-type", "text/event-stream")
                            .body(())
                            .unwrap();
                        let mut send_stream = respond.send_response(resp, false).unwrap();
                        tokio::spawn(async move {
                            for i in 1..=3 {
                                let chunk = format!("data: msg-{i}\n\n");
                                send_stream
                                    .send_data(bytes::Bytes::from(chunk), i == 3)
                                    .unwrap();
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        });
                    } else if req.uri().path() == "/api/v1/h2/fast" {
                        let resp = http::Response::builder()
                            .status(200)
                            .header("content-type", "text/plain")
                            .body(())
                            .unwrap();
                        let mut send_stream = respond.send_response(resp, false).unwrap();
                        send_stream
                            .send_data(bytes::Bytes::from_static(b"fast-pong"), true)
                            .unwrap();
                    } else {
                        let resp = http::Response::builder().status(404).body(()).unwrap();
                        let _ = respond.send_response(resp, true);
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
    let socket_path = tmp.path().join("edge_l7_h2_stream.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "h2-stream-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http2".into(),
            version: None,
            streaming: velda_sync::StreamingMode {
                client: true,
                server: true,
            },
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
        id: "h2-stream-up".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "http2".into(),
            streaming: velda_sync::StreamingMode {
                client: true,
                server: true,
            },
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
        id: "r-h2-stream".into(),
        kind: "l7".into(),
        listener: "h2-stream-in".into(),
        match_rule: RouteMatch {
            protocol: None,
            path_prefix: Some("/api/v1/h2".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "h2-stream-up".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect 1 downstream H2 connection and multiplex 2 streams simultaneously
    let tcp = TcpStream::connect(gateway_addr).await.unwrap();
    let (client, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let mut client_sse = client.clone();
    let sse_task = tokio::spawn(async move {
        let req = http::Request::builder()
            .method("GET")
            .uri("http://localhost/api/v1/h2/sse")
            .header("host", "localhost")
            .body(())
            .unwrap();
        let (resp_fut, _) = client_sse.send_request(req, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, http::StatusCode::OK);
        assert_eq!(
            parts.headers.get("content-type").unwrap(),
            "text/event-stream"
        );

        let mut events = Vec::new();
        while let Some(chunk_res) = body.data().await {
            let chunk = chunk_res.unwrap();
            let len = chunk.len();
            if !chunk.is_empty() {
                events.push(String::from_utf8(chunk.to_vec()).unwrap());
            }
            let _ = body.flow_control().release_capacity(len);
        }
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], "data: msg-1\n\n");
        assert_eq!(events[1], "data: msg-2\n\n");
        assert_eq!(events[2], "data: msg-3\n\n");
    });

    let mut client_fast = client.clone();
    let fast_task = tokio::spawn(async move {
        let req = http::Request::builder()
            .method("GET")
            .uri("http://localhost/api/v1/h2/fast")
            .header("host", "localhost")
            .body(())
            .unwrap();
        let (resp_fut, _) = client_fast.send_request(req, true).unwrap();
        let (parts, mut body) = resp_fut.await.unwrap().into_parts();
        assert_eq!(parts.status, http::StatusCode::OK);
        let chunk = body.data().await.unwrap().unwrap();
        assert_eq!(&chunk[..], b"fast-pong");
    });

    sse_task.await.unwrap();
    fast_task.await.unwrap();

    shutdown_tx.send(true).unwrap();
    let _ = edge_task.await;
}
