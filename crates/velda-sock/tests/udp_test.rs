use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use velda_core::TransportProtocol;
use velda_sock::{Datagram, UdpSocket, UdpSocketConfig, raw_to_socket_addr};

const LOOPBACK_V4: u128 = velda_core::lut::ip::v4_to_u128(0x7F00_0001);

#[tokio::test]
async fn test_udp_socket_send_recv_and_byte_counters() {
    let config = UdpSocketConfig::new()
        .with_recv_buffer_size(65536)
        .with_send_buffer_size(65536);

    let server = UdpSocket::bind(LOOPBACK_V4, 0, config.clone()).unwrap();
    let (srv_ip, srv_port_be) = (server.local_ip, server.local_port_be);

    let client = UdpSocket::bind(LOOPBACK_V4, 0, config).unwrap();
    let (cli_ip, cli_port_be) = (client.local_ip, client.local_port_be);

    // Client sends to server
    let payload = b"hello udp server";
    let sent = client.send_to(payload, srv_ip, srv_port_be).await.unwrap();
    assert_eq!(sent, payload.len());
    assert_eq!(client.bytes_sent(), payload.len() as u64);

    // Server receives
    let mut buf = [0u8; 64];
    let (n, peer_ip, peer_port_be) = server.recv_from(&mut buf).await.unwrap();
    assert_eq!(n, payload.len());
    assert_eq!(&buf[..n], payload);
    assert_eq!((peer_ip, peer_port_be), (cli_ip, cli_port_be));
    assert_eq!(server.bytes_received(), payload.len() as u64);

    // Server replies
    let reply = b"ack from server";
    server.send_to(reply, peer_ip, peer_port_be).await.unwrap();
    assert_eq!(server.bytes_sent(), reply.len() as u64);

    // Client receives reply
    let (reply_n, reply_ip, reply_port_be) = client.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..reply_n], reply);
    assert_eq!((reply_ip, reply_port_be), (srv_ip, srv_port_be));
    assert_eq!(client.bytes_received(), reply.len() as u64);
}

#[tokio::test]
async fn test_udp_datagram_api() {
    let server = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();
    let (srv_ip, srv_port_be) = (server.local_ip, server.local_port_be);

    let client = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();
    let (cli_ip, cli_port_be) = (client.local_ip, client.local_port_be);

    let out_dgram = Datagram::new(
        srv_ip,
        srv_port_be,
        cli_ip,
        cli_port_be,
        b"datagram-payload".to_vec(),
    );
    client.send_datagram(&out_dgram).await.unwrap();

    let in_dgram = server.recv_datagram(1024).await.unwrap();
    assert_eq!(
        (in_dgram.peer_ip, in_dgram.peer_port_be),
        (cli_ip, cli_port_be)
    );
    assert_eq!(in_dgram.data(), b"datagram-payload");

    let l4_req = in_dgram.to_l4_request(777);
    assert_eq!(l4_req.connection_id, 777);
    assert_eq!(l4_req.protocol, TransportProtocol::Udp);
    assert_eq!(l4_req.peer_ip, cli_ip);
    assert_eq!(l4_req.peer_port_be, cli_port_be);
}

#[tokio::test]
async fn test_forward_datagram_helper() {
    let target = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();
    let sender = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();

    let payload = b"forwarded udp packet";
    let sent = sender
        .send_to(payload, target.local_ip, target.local_port_be)
        .await
        .unwrap();
    assert_eq!(sent, payload.len());

    let mut buf = [0u8; 64];
    let (n, from_ip, from_port_be) = target.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], payload);
    assert_eq!(
        (from_ip, from_port_be),
        (sender.local_ip, sender.local_port_be)
    );
}

#[tokio::test]
async fn test_forward_udp_flow() {
    // 1. Mock upstream server
    let upstream = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();
    let actual_upstream_addr = raw_to_socket_addr(upstream.local_ip, upstream.local_port_be);

    let upstream_task = tokio::spawn(async move {
        let mut buf = [0u8; 128];
        let (n, sender_ip, sender_port_be) = upstream.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"client query");

        // Respond with uppercase
        upstream
            .send_to(b"SERVER RESPONSE", sender_ip, sender_port_be)
            .await
            .unwrap();
    });

    // 2. Downstream proxy socket
    let proxy_socket =
        Arc::new(UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap());

    // 3. Client socket
    let client = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();
    let (cli_ip, cli_port_be) = (client.local_ip, client.local_port_be);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let proxy_clone = Arc::clone(&proxy_socket);
    let flow_task = tokio::spawn(async move {
        let upstream_socket = UdpSocket::bind(LOOPBACK_V4, 0, UdpSocketConfig::default()).unwrap();
        upstream_socket
            .inner()
            .connect(actual_upstream_addr)
            .await
            .unwrap();
        upstream_socket.inner().send(b"client query").await.unwrap();

        let mut buf = [0u8; 1024];
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                res = upstream_socket.inner().recv(&mut buf) => {
                    let n = res.unwrap();
                    proxy_clone.send_to(&buf[..n], cli_ip, cli_port_be).await.unwrap();
                }
            }
        }
    });

    // Client receives response from proxy
    let mut client_buf = [0u8; 64];
    let (n, from_ip, from_port_be) = client.recv_from(&mut client_buf).await.unwrap();
    assert_eq!(&client_buf[..n], b"SERVER RESPONSE");
    assert_eq!(
        (from_ip, from_port_be),
        (proxy_socket.local_ip, proxy_socket.local_port_be)
    );

    // Shut down flow
    shutdown_tx.send(true).unwrap();

    let _ = tokio::time::timeout(Duration::from_secs(2), flow_task)
        .await
        .unwrap();

    upstream_task.await.unwrap();
}

#[tokio::test]
async fn test_udp_socket_reuseport_shards() {
    let config = UdpSocketConfig::new()
        .with_reuseport(true)
        .with_concurrency_shards(4);

    let shards = UdpSocket::bind_shards(LOOPBACK_V4, 0, config).unwrap();

    #[cfg(unix)]
    assert_eq!(shards.len(), 4);
    #[cfg(not(unix))]
    assert!(!shards.is_empty());

    let port = u16::from_be(shards[0].local_port_be);
    assert_ne!(port, 0);

    for s in &shards {
        assert_eq!(u16::from_be(s.local_port_be), port);
    }
}
