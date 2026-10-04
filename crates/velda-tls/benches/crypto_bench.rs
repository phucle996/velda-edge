//! Crate `velda-tls` Specialized Cryptography, mTLS Overhead, and PEM Compilation Benchmark.
//!
//! Evaluates the unique computational costs specific to TLS:
//! 1. Standard Downstream TLS 1.3 Handshake vs Mutual TLS (mTLS) Client Auth Overhead.
//! 2. Upstream Outbound TLS Handshake Throughput (`TlsClientEngine::connect`).
//! 3. Dynamic In-Memory PEM Parsing & ServerConfig Compilation Latency (Reload Phase).

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{make_client_connector, make_mtls_client_connector, make_test_cert};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_tls::client::{ClientTlsConfig, TlsClientEngine};
use velda_tls::{ServerTlsConfig, TlsServerEngine, TlsServerParams};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("# Velda Edge — `velda-tls` Cryptography & mTLS Overhead Benchmark\n");

    bench_mtls_vs_standard_handshake().await;
    bench_upstream_egress_handshake().await;
    bench_pem_compilation_scaling();
}

/// Stage 1: Standard TLS 1.3 Handshake vs Mutual TLS (mTLS) Client Auth Overhead
async fn bench_mtls_vs_standard_handshake() {
    println!("### 1. Standard TLS 1.3 vs Mutual TLS (mTLS) Handshake Overhead\n");

    let iterations = 1000;
    let (server_cert, server_key) = make_test_cert(vec!["bench.internal".into()]);
    let (client_cert, client_key) = make_test_cert(vec!["client.internal".into()]);

    let standard_server_cfg = ServerTlsConfig {
        sni: vec!["bench.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: server_cert.clone(),
        key_pem: server_key.clone(),
        client_ca_pem: None,
    };
    let params = TlsServerParams::from_hardware().with_session_cache_capacity(4096);
    let standard_engine =
        TlsServerEngine::new_with_params(&[standard_server_cfg], &params).unwrap();
    let standard_connector = make_client_connector(&server_cert, Some(vec!["h2"]));

    // Standard 1-way TLS 1.3
    let start_std = Instant::now();
    for _ in 0..iterations {
        let (client_io, server_io) = duplex(65536);
        let s_engine = standard_engine.clone();

        let srv_task = tokio::spawn(async move {
            let mut s = s_engine.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });

        let s_name = ServerName::try_from("bench.internal".to_string()).unwrap();
        let mut c = standard_connector.connect(s_name, client_io).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        c.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }
    let dur_std = start_std.elapsed();
    let avg_std = dur_std / iterations;
    let thput_std = (iterations as f64) / dur_std.as_secs_f64();

    // mTLS (2-way TLS 1.3 with Client Auth verification)
    let mtls_server_cfg = ServerTlsConfig {
        sni: vec!["bench.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: server_cert.clone(),
        key_pem: server_key,
        client_ca_pem: Some(client_cert.clone()),
    };
    let mtls_engine = TlsServerEngine::new_with_params(&[mtls_server_cfg], &params).unwrap();
    let mtls_connector =
        make_mtls_client_connector(&server_cert, &client_cert, &client_key, Some(vec!["h2"]));

    let start_mtls = Instant::now();
    for _ in 0..iterations {
        let (client_io, server_io) = duplex(65536);
        let s_engine = mtls_engine.clone();

        let srv_task = tokio::spawn(async move {
            let mut s = s_engine.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });

        let s_name = ServerName::try_from("bench.internal".to_string()).unwrap();
        let mut c = mtls_connector.connect(s_name, client_io).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        c.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }
    let dur_mtls = start_mtls.elapsed();
    let avg_mtls = dur_mtls / iterations;
    let thput_mtls = (iterations as f64) / dur_mtls.as_secs_f64();

    let overhead_pct =
        ((avg_mtls.as_secs_f64() - avg_std.as_secs_f64()) / avg_std.as_secs_f64()) * 100.0;

    println!(
        "| Handshake Mode | Iterations | Avg Latency | Throughput | Crypto Verification Cost |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| **Standard TLS 1.3** | {} | **{:.2?}** | {:.0} hsk/s | Baseline (Server-only) |",
        iterations, avg_std, thput_std
    );
    println!(
        "| **Mutual TLS (mTLS)** | {} | **{:.2?}** | {:.0} hsk/s | **+{:.1}% Overhead** |",
        iterations, avg_mtls, thput_mtls, overhead_pct
    );
    println!();
}

/// Stage 2: Upstream Outbound TLS Handshake (`TlsClientEngine::connect`)
async fn bench_upstream_egress_handshake() {
    println!("### 2. Upstream Outbound TLS Handshake Throughput (`TlsClientEngine`)\n");

    let iterations = 1000;
    let (backend_cert, backend_key) = make_test_cert(vec!["backend.internal".into()]);

    let backend_server_cfg = ServerTlsConfig {
        sni: vec!["backend.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: backend_cert.clone(),
        key_pem: backend_key,
        client_ca_pem: None,
    };
    let backend_server = TlsServerEngine::new(&[backend_server_cfg]).unwrap();

    let upstream_cfg = ClientTlsConfig {
        sni: vec!["backend.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(backend_cert),
        client_cert_pem: None,
        client_key_pem: None,
        insecure_skip_verify: false,
    };
    let client_engine = TlsClientEngine::new(&[upstream_cfg]).unwrap();

    let start = Instant::now();
    for _ in 0..iterations {
        let (edge_io, backend_io) = duplex(65536);
        let b_srv = backend_server.clone();

        let srv_task = tokio::spawn(async move {
            let mut stream = b_srv.accept(backend_io).await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
        });

        let mut c = client_engine
            .connect("backend.internal", edge_io)
            .await
            .unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        c.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }
    let elapsed = start.elapsed();
    let avg = elapsed / iterations;
    let thput = (iterations as f64) / elapsed.as_secs_f64();

    println!("| Pipeline Flow | Iterations | Avg Latency | Throughput | Invariant |");
    println!("| :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| **Gateway -> Upstream TLS** | {} | **{:.2?}** | {:.0} conn/s | **Zero-IO In-Memory** |\n",
        iterations, avg, thput
    );
}

/// Stage 3: Dynamic In-Memory PEM Parsing & ServerConfig Compilation Latency (Reload Phase)
fn bench_pem_compilation_scaling() {
    println!("### 3. Dynamic PEM Parsing & `ServerConfig` Compilation Latency (Reload Phase)\n");

    let cert_counts = [1, 5, 10, 25, 50];
    let cycles_per_count = 50;

    println!(
        "| Certificates Compiled | Cycles | Total Batch Latency | Avg Time / Certificate | Compilation Rate |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");

    for &count in &cert_counts {
        // Pre-generate certs
        let mut configs = Vec::with_capacity(count);
        for i in 0..count {
            let domain = format!("tenant-{i}.edge.velda");
            let (cert, key) = make_test_cert(vec![domain.clone()]);
            configs.push(ServerTlsConfig {
                sni: vec![domain],
                versions: vec!["tls1.3".into()],
                alpn: vec!["h2".into(), "http/1.1".into()],
                cert_pem: cert,
                key_pem: key,
                client_ca_pem: None,
            });
        }

        let start = Instant::now();
        for _ in 0..cycles_per_count {
            let compiled = ServerTlsConfig::build(&configs).unwrap();
            assert_eq!(Arc::strong_count(&compiled), 1);
        }
        let elapsed = start.elapsed();
        let avg_batch = elapsed / cycles_per_count;
        let avg_per_cert = avg_batch / (count as u32);
        let certs_per_sec = ((count * cycles_per_count as usize) as f64) / elapsed.as_secs_f64();

        println!(
            "| **{} certs** | {} | **{:.2?}** | **{:.2?}** | {:.0} certs/s |",
            count, cycles_per_count, avg_batch, avg_per_cert, certs_per_sec
        );
    }
    println!();
}
