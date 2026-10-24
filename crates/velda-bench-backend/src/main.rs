//! Velda High-Performance Real-Stream Benchmark Backend.
//!
//! Provides 3 isolated Protocol Routers:
//! - HTTP/1.1 Router (Port 8081 Plain, Port 8441 TLS)
//! - HTTP/2 Router (Port 8082 Cleartext H2C, Port 8442 TLS H2)
//! - HTTP/3 Router (Port 8443 QUIC UDP)
//!
//! Each Protocol Router serves all 8 canonical routes across 4 streaming pipeline strategies
//! (buffered, server_stream, client_stream, duplex) in Plain and TLS modes.

mod grpc;
mod http1;
mod http2;
mod http3;
mod routes;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_rustls::rustls::ServerConfig;
use velda_http3::quinn_proto;

fn load_or_generate_tls() -> (Arc<ServerConfig>, Arc<quinn_proto::ServerConfig>) {
    let crt_path = "/etc/ssl/velda/backend.crt";
    let key_path = "/etc/ssl/velda/backend.key";

    let (cert_chain, private_key) = if let (Ok(crt_file), Ok(key_file)) =
        (File::open(crt_path), File::open(key_path))
    {
        let mut crt_reader = BufReader::new(crt_file);
        let certs = rustls_pemfile::certs(&mut crt_reader)
            .filter_map(|r| r.ok())
            .collect::<Vec<_>>();
        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .ok()
            .flatten()
            .expect("Failed to parse private key");
        (certs, key)
    } else {
        tracing::warn!(
            "Backend certificate files not found, generating self-signed TLS certificate"
        );
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .unwrap();
        let cert_der = cert.cert.der().to_vec();
        let key_der = cert.signing_key.serialize_der();
        let cert_chain = vec![CertificateDer::from(cert_der)];
        let private_key =
            PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(key_der));
        (cert_chain, private_key)
    };

    // TCP TLS Server Config (ALPN: h2, http/1.1)
    let mut tcp_tls_cfg = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(cert_chain.clone(), private_key.clone_key())
    .unwrap();
    tcp_tls_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    // QUIC H3 Server Config (ALPN: h3)
    let mut quic_tls_cfg = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(cert_chain, private_key)
    .unwrap();
    quic_tls_cfg.alpn_protocols = vec![b"h3".to_vec()];

    let quic_server_crypto =
        quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(quic_tls_cfg)).unwrap();
    let quic_server_cfg = quinn_proto::ServerConfig::with_crypto(Arc::new(quic_server_crypto));

    (Arc::new(tcp_tls_cfg), Arc::new(quic_server_cfg))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    tracing::info!("🚀 Starting Velda Real-Stream Benchmark Backend");

    let (tcp_tls, quic_tls) = load_or_generate_tls();

    // 1. Launch HTTP/1.1 Router
    let h1_plain: SocketAddr = "0.0.0.0:8081".parse().unwrap();
    let h1_tls: SocketAddr = "0.0.0.0:8441".parse().unwrap();
    let tcp_tls_h1 = tcp_tls.clone();
    tokio::spawn(async move {
        if let Err(e) = http1::Http1Router::run(h1_plain, Some(h1_tls), Some(tcp_tls_h1)).await {
            tracing::error!(error = %e, "HTTP/1.1 Router crashed");
        }
    });

    // 2. Launch HTTP/2 Router
    let h2_h2c: SocketAddr = "0.0.0.0:8082".parse().unwrap();
    let h2_tls: SocketAddr = "0.0.0.0:8442".parse().unwrap();
    let tcp_tls_h2 = tcp_tls.clone();
    tokio::spawn(async move {
        if let Err(e) = http2::Http2Router::run(h2_h2c, Some(h2_tls), Some(tcp_tls_h2)).await {
            tracing::error!(error = %e, "HTTP/2 Router crashed");
        }
    });

    // 3. Launch HTTP/3 Router
    let h3_addr: SocketAddr = "0.0.0.0:8443".parse().unwrap();
    tokio::spawn(async move {
        if let Err(e) = http3::Http3Router::run(h3_addr, quic_tls).await {
            tracing::error!(error = %e, "HTTP/3 Router crashed");
        }
    });

    // 4. Launch gRPC Router (Port 50051 Cleartext H2C, Port 50052 TLS H2)
    let grpc_plain: SocketAddr = "0.0.0.0:50051".parse().unwrap();
    let grpc_tls: SocketAddr = "0.0.0.0:50052".parse().unwrap();
    let tcp_tls_grpc = tcp_tls.clone();
    tokio::spawn(async move {
        if let Err(e) = grpc::GrpcRouter::run(grpc_plain, Some(grpc_tls), Some(tcp_tls_grpc)).await
        {
            tracing::error!(error = %e, "gRPC Router crashed");
        }
    });

    tracing::info!(
        "✅ All 4 Protocol Routers initialized successfully (HTTP/1, HTTP/2, HTTP/3, and native gRPC)"
    );
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down backend...");
    Ok(())
}
