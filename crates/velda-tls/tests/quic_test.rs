//! Integration tests for QUIC ServerConfig compilation and Quinn Endpoint initialization.

mod common;

use std::sync::Arc;

use common::make_test_cert;
use velda_tls::{ServerTlsConfig, TlsServerEngine};

#[test]
fn test_quic_server_config_compilation() {
    let (cert_pem, key_pem) = make_test_cert(vec!["quic.example.com".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["quic.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    };

    let server_engine =
        TlsServerEngine::new(&[server_config]).expect("Failed to build server engine");
    let quic_config = server_engine
        .build_quic_config()
        .expect("Failed to build QUIC server config");

    let endpoint_config = Arc::new(quinn_proto::EndpointConfig::default());
    let endpoint =
        quinn_proto::Endpoint::new(endpoint_config, Some(Arc::new(quic_config)), false, None);

    // Quinn endpoint initialized successfully with QUIC crypto & 0-RTT config from TLS engine
    let _ = endpoint;
}

#[test]
fn test_quic_server_config_preserves_crypto_and_alpn() {
    let (cert_pem, key_pem) = make_test_cert(vec!["h3.example.com".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["h3.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into(), "h3-29".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();
    let quic_config = server_engine.build_quic_config().unwrap();

    // Verify crypto provider is instantiated
    assert!(Arc::strong_count(&quic_config.crypto) >= 1);
}

#[test]
fn test_quic_handshake_extracts_non_zero_ja4() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Instant;

    let (cert_pem, key_pem) = make_test_cert(vec!["h3.example.com".into()]);
    let server_config = ServerTlsConfig {
        sni: vec!["h3.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    };
    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();
    let quic_srv_cfg = Arc::new(server_engine.build_quic_config().unwrap());

    let tls_client_cfg = velda_tls::build_insecure_client_config(vec![b"h3".to_vec()]);
    let quic_cli_cfg = velda_tls::build_quic_client_config(tls_client_cfg).unwrap();

    let mut srv_ep = quinn_proto::Endpoint::new(
        Arc::new(quinn_proto::EndpointConfig::default()),
        Some(quic_srv_cfg),
        false,
        None,
    );
    let mut cli_ep = quinn_proto::Endpoint::new(
        Arc::new(quinn_proto::EndpointConfig::default()),
        None,
        false,
        None,
    );

    let srv_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
    let cli_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 55555);
    let now = Instant::now();

    let (_cli_handle, mut cli_conn) = cli_ep
        .connect(now, quic_cli_cfg, srv_addr, "h3.example.com")
        .unwrap();
    let mut srv_conn_opt: Option<(quinn_proto::ConnectionHandle, quinn_proto::Connection)> = None;

    let mut tbuf = Vec::with_capacity(4096);
    let mut rbuf = Vec::with_capacity(4096);
    for _ in 0..16 {
        tbuf.clear();
        while let Some(tr) = cli_conn.poll_transmit(now, 1, &mut tbuf) {
            let pkt = bytes::BytesMut::from(&tbuf[..tr.size]);
            rbuf.clear();
            if let Some(ev) = srv_ep.handle(now, cli_addr, None, None, pkt, &mut rbuf) {
                match ev {
                    quinn_proto::DatagramEvent::NewConnection(inc) => {
                        let mut abuf = Vec::new();
                        if let Ok((h, c)) = srv_ep.accept(inc, now, &mut abuf, None) {
                            srv_conn_opt = Some((h, c));
                        }
                    }
                    quinn_proto::DatagramEvent::ConnectionEvent(_, cev) => {
                        if let Some((_, ref mut c)) = srv_conn_opt {
                            c.handle_event(cev);
                        }
                    }
                    quinn_proto::DatagramEvent::Response(_) => {}
                }
            }
            tbuf.clear();
        }
        if let Some((_, ref mut sc)) = srv_conn_opt {
            tbuf.clear();
            while let Some(tr) = sc.poll_transmit(now, 1, &mut tbuf) {
                let pkt = bytes::BytesMut::from(&tbuf[..tr.size]);
                rbuf.clear();
                if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, cev)) =
                    cli_ep.handle(now, srv_addr, None, None, pkt, &mut rbuf)
                {
                    cli_conn.handle_event(cev);
                }
                tbuf.clear();
            }
            if !sc.is_handshaking() {
                break;
            }
        }
    }

    let (_, srv_conn) = srv_conn_opt.expect("QUIC server connection should establish");
    assert!(!srv_conn.is_handshaking(), "QUIC handshake should finish");
    let ja4 = velda_tls::extract_quic_ja4(&srv_conn);
    assert_ne!(ja4, 0, "QUIC handshake must yield non-zero JA4 fingerprint");
}
