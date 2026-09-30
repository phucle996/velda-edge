use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerLimitsConfig, ListenerTransportConfig,
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
async fn test_end_to_end_l4_tcp_forwarding() {
    // 1. Start a mock TCP backend echo server
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (mut socket, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let n = socket.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"HELLO_FROM_CLIENT");

        socket.write_all(b"HELLO_FROM_L4_BACKEND").await.unwrap();
        socket.flush().await.unwrap();
    });

    // 2. Reserve an ephemeral port for the gateway L4 listener
    let gateway_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    // 3. Write LKG binary artifacts for listener, route, and upstream
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_l4.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Listener: TCP transport + RAW application (L4 direct path)
    let listeners = vec![ListenerConfig {
        id: "tcp-l4-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "raw".into(),
            version: None,
        },
        tls: Default::default(),
        limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    // Upstream: Points to our mock TCP backend server
    let upstreams = vec![UpstreamConfig {
        id: "tcp-echo-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "tcp".into(),
            application: "raw".into(),
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

    // Route: L4 route linking listener "tcp-l4-in" to upstream "tcp-echo-backend"
    let routes = vec![RouteConfig {
        id: "l4-route-1".into(),
        kind: "l4".into(),
        listener: "tcp-l4-in".into(),
        match_rule: RouteMatch {
            protocol: Some("tcp".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts::default(),
        upstream: "tcp-echo-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    // 4. Cold-start Edge supervisor
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();

    assert_eq!(supervisor.shared_runtime().load().listener_count(), 1);
    assert_eq!(supervisor.shared_runtime().load().route_count(), 1);
    assert_eq!(supervisor.shared_runtime().load().upstream_count(), 1);

    // 5. Run supervisor in background
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    // Wait briefly for TrafficEngine to bind the ingress port
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 6. Connect client directly to Edge gateway L4 listener
    let mut client = TcpStream::connect(gateway_addr)
        .await
        .expect("Failed to connect to gateway");

    // Send payload from client to gateway
    client.write_all(b"HELLO_FROM_CLIENT").await.unwrap();
    client.flush().await.unwrap();

    // Read echo response from backend through gateway
    let mut resp = [0u8; 1024];
    let n = client.read(&mut resp).await.unwrap();
    assert_eq!(&resp[..n], b"HELLO_FROM_L4_BACKEND");

    // 7. Verify backend task completed successfully
    backend_task.await.unwrap();

    // 8. Graceful shutdown
    let _ = shutdown_tx.send(true);
    let res = edge_task.await.unwrap();
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_end_to_end_l4_udp_bidirectional_forwarding() {
    // 1. Start a mock UDP backend echo server
    let backend_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_socket.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        // Request 1
        let (n, peer) = backend_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"UDP_PING_1");
        backend_socket.send_to(b"UDP_PONG_1", peer).await.unwrap();

        // Request 2 (same session)
        let (n2, peer2) = backend_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n2], b"UDP_PING_2");
        assert_eq!(
            peer, peer2,
            "Subsequent packet in same session should use same ephemeral upstream socket"
        );
        backend_socket.send_to(b"UDP_PONG_2", peer2).await.unwrap();
    });

    // 2. Reserve an ephemeral port for the gateway L4 UDP listener
    let gateway_addr: SocketAddr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap()
    };

    // 3. Write LKG binary artifacts
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_l4_udp_bi.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "udp-l4-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "udp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "raw".into(),
            version: None,
        },
        tls: Default::default(),
        limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "udp-echo-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "udp".into(),
            application: "raw".into(),
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
            idle_ms: 5000,
            request_ms: None,
        },
        health_check: None,
        tls: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    // Route configured with 5000ms idle timeout -> bidirectional stateful session proxy
    let routes = vec![RouteConfig {
        id: "udp-bi-route".into(),
        kind: "l4".into(),
        listener: "udp-l4-in".into(),
        match_rule: RouteMatch {
            protocol: Some("udp".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts {
            downstream_idle_ms: Some(5000),
        },
        upstream: "udp-echo-backend".into(),
        plugins: vec![],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    // 4. Cold-start Edge supervisor
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();

    // 5. Run supervisor in background
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 6. Client sends datagram 1 to gateway
    let client_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_socket
        .send_to(b"UDP_PING_1", gateway_addr)
        .await
        .unwrap();

    // Client receives response 1 from gateway
    let mut reply = [0u8; 1024];
    let (n1, from_addr) = client_socket.recv_from(&mut reply).await.unwrap();
    assert_eq!(&reply[..n1], b"UDP_PONG_1");
    assert_eq!(from_addr, gateway_addr);

    // Client sends datagram 2 in same session
    client_socket
        .send_to(b"UDP_PING_2", gateway_addr)
        .await
        .unwrap();
    let (n2, from_addr2) = client_socket.recv_from(&mut reply).await.unwrap();
    assert_eq!(&reply[..n2], b"UDP_PONG_2");
    assert_eq!(from_addr2, gateway_addr);

    // 7. Verify backend task finished
    backend_task.await.unwrap();

    // 8. Graceful shutdown
    let _ = shutdown_tx.send(true);
    let res = edge_task.await.unwrap();
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_end_to_end_l4_udp_unidirectional_forwarding() {
    // 1. Start a mock UDP backend receiver (fire-and-forget collector)
    let backend_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_socket.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let (n, _) = backend_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"FIRE_AND_FORGET_METRIC");
    });

    // 2. Reserve an ephemeral port for the gateway L4 UDP listener
    let gateway_addr: SocketAddr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap()
    };

    // 3. Write LKG binary artifacts
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_l4_udp_uni.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let listeners = vec![ListenerConfig {
        id: "udp-uni-in".into(),
        address: gateway_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "udp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "raw".into(),
            version: None,
        },
        tls: Default::default(),
        limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let upstreams = vec![UpstreamConfig {
        id: "udp-collector-backend".into(),
        mode: "endpoints".into(),
        protocol: UpstreamProtocolConfig {
            transport: "udp".into(),
            application: "raw".into(),
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
            idle_ms: 5000,
            request_ms: None,
        },
        health_check: None,
        tls: None,
    }];
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    // Route configured with downstream_idle_ms = Some(0) -> unidirectional mode
    let routes = vec![RouteConfig {
        id: "udp-uni-route".into(),
        kind: "l4".into(),
        listener: "udp-uni-in".into(),
        match_rule: RouteMatch {
            protocol: Some("udp".into()),
            ..Default::default()
        },
        timeouts: RouteTimeouts {
            downstream_idle_ms: Some(0),
        },
        upstream: "udp-collector-backend".into(),
        plugins: vec!["unidirectional".into()],
    }];
    let routes_bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();

    // 4. Cold-start Edge supervisor
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();

    // 5. Run supervisor in background
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 6. Client fires datagram to gateway
    let client_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_socket
        .send_to(b"FIRE_AND_FORGET_METRIC", gateway_addr)
        .await
        .unwrap();

    // 7. Verify backend received the datagram
    backend_task.await.unwrap();

    // 8. Graceful shutdown
    let _ = shutdown_tx.send(true);
    let res = edge_task.await.unwrap();
    assert!(res.is_ok());
}
