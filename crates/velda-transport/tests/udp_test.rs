use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use velda_core::{ConnectionId, TransportProtocol};
use velda_transport::{Datagram, UdpSocket, UdpSocketConfig, forward_datagram, forward_udp_flow};

#[tokio::test]
async fn test_udp_socket_send_recv_and_byte_counters() {
    let config = UdpSocketConfig::new()
        .with_recv_buffer_size(65536)
        .with_send_buffer_size(65536);

    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = UdpSocket::bind(server_addr, config.clone()).unwrap();
    let actual_server_addr = server.local_addr();

    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client = UdpSocket::bind(client_addr, config).unwrap();
    let actual_client_addr = client.local_addr();

    // Client sends to server
    let payload = b"hello udp server";
    let sent = client.send_to(payload, actual_server_addr).await.unwrap();
    assert_eq!(sent, payload.len());
    assert_eq!(client.bytes_sent(), payload.len() as u64);

    // Server receives
    let mut buf = [0u8; 64];
    let (n, peer) = server.recv_from(&mut buf).await.unwrap();
    assert_eq!(n, payload.len());
    assert_eq!(&buf[..n], payload);
    assert_eq!(peer, actual_client_addr);
    assert_eq!(server.bytes_received(), payload.len() as u64);

    // Server replies
    let reply = b"ack from server";
    server.send_to(reply, peer).await.unwrap();
    assert_eq!(server.bytes_sent(), reply.len() as u64);

    // Client receives reply
    let (reply_n, reply_peer) = client.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..reply_n], reply);
    assert_eq!(reply_peer, actual_server_addr);
    assert_eq!(client.bytes_received(), reply.len() as u64);
}

#[tokio::test]
async fn test_udp_datagram_api() {
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = UdpSocket::bind(server_addr, UdpSocketConfig::default()).unwrap();
    let actual_server_addr = server.local_addr();

    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client = UdpSocket::bind(client_addr, UdpSocketConfig::default()).unwrap();
    let actual_client_addr = client.local_addr();

    let out_dgram = Datagram::new(
        actual_server_addr,
        actual_client_addr,
        b"datagram-payload".to_vec(),
    );
    client.send_datagram(&out_dgram).await.unwrap();

    let in_dgram = server.recv_datagram(1024).await.unwrap();
    assert_eq!(in_dgram.peer(), actual_client_addr);
    assert_eq!(in_dgram.data(), b"datagram-payload");

    let l4_req = in_dgram.to_l4_request(ConnectionId::new(777));
    assert_eq!(l4_req.connection_id, ConnectionId::new(777));
    assert_eq!(l4_req.protocol, TransportProtocol::Udp);
    assert_eq!(l4_req.client_ip(), actual_client_addr.ip());
    assert_eq!(l4_req.client_port(), actual_client_addr.port());
}

#[tokio::test]
async fn test_forward_datagram_helper() {
    let target_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let target = UdpSocket::bind(target_addr, UdpSocketConfig::default()).unwrap();
    let actual_target_addr = target.local_addr();

    let sender_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sender = UdpSocket::bind(sender_addr, UdpSocketConfig::default()).unwrap();

    let payload = b"forwarded udp packet";
    let sent = forward_datagram(&sender, payload, actual_target_addr)
        .await
        .unwrap();
    assert_eq!(sent, payload.len());

    let mut buf = [0u8; 64];
    let (n, from) = target.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], payload);
    assert_eq!(from, sender.local_addr());
}

#[tokio::test]
async fn test_forward_udp_flow() {
    // 1. Mock upstream server
    let upstream_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let upstream = UdpSocket::bind(upstream_addr, UdpSocketConfig::default()).unwrap();
    let actual_upstream_addr = upstream.local_addr();

    let upstream_task = tokio::spawn(async move {
        let mut buf = [0u8; 128];
        let (n, sender) = upstream.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"client query");

        // Respond with uppercase
        upstream.send_to(b"SERVER RESPONSE", sender).await.unwrap();
    });

    // 2. Downstream proxy socket
    let proxy_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let proxy_socket = Arc::new(UdpSocket::bind(proxy_addr, UdpSocketConfig::default()).unwrap());

    // 3. Client socket
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client = UdpSocket::bind(client_addr, UdpSocketConfig::default()).unwrap();
    let actual_client_addr = client.local_addr();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let proxy_clone = Arc::clone(&proxy_socket);
    let flow_task = tokio::spawn(async move {
        forward_udp_flow(
            proxy_clone,
            actual_client_addr,
            actual_upstream_addr,
            b"client query",
            shutdown_rx,
        )
        .await
        .unwrap()
    });

    // Client receives response from proxy
    let mut client_buf = [0u8; 64];
    let (n, from) = client.recv_from(&mut client_buf).await.unwrap();
    assert_eq!(&client_buf[..n], b"SERVER RESPONSE");
    assert_eq!(from, proxy_socket.local_addr());

    // Shut down flow
    shutdown_tx.send(true).unwrap();

    let stats = tokio::time::timeout(Duration::from_secs(2), flow_task)
        .await
        .unwrap()
        .unwrap();

    upstream_task.await.unwrap();

    assert_eq!(stats.client_to_server_bytes, 12); // "client query"
    assert_eq!(stats.server_to_client_bytes, 15); // "SERVER RESPONSE"
    assert_eq!(stats.total_bytes(), 27);
}
