use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use quinn_proto::ServerConfig;
use velda_http3::composer_parse::Http3Engine;
use velda_http3::frame::{Http3Frame, decode_frame, encode_frame};

#[test]
fn test_http3_frame_roundtrip() {
    let frame = Http3Frame::Data(Bytes::from_static(b"quic datagram payload"));
    let mut buf = bytes::BytesMut::new();
    encode_frame(&frame, &mut buf);

    let decoded = decode_frame(&mut buf).unwrap().unwrap();
    assert_eq!(decoded, frame);
    assert!(buf.is_empty());
}

#[test]
fn test_http3_engine_lifecycle() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der)];
    let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
    );

    let mut rustls_server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .unwrap();
    rustls_server.alpn_protocols = vec![b"h3".to_vec()];
    let quic_crypto =
        quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server)).unwrap();
    let server_config = ServerConfig::with_crypto(Arc::new(quic_crypto));
    let mut engine = Http3Engine::new(Arc::new(server_config));

    assert_eq!(engine.connection_count(), 0);

    let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let (outgoing, requests) =
        engine.handle_datagram(Instant::now(), peer, None, b"invalid-udp-packet");

    assert_eq!(requests.len(), 0);
    let _ = outgoing;
}
