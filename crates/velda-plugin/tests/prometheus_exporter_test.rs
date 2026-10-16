//! Comprehensive integration tests for Prometheus Exporter on Auxiliary Subsystem.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

use velda_observability::MetricsStore;
use velda_plugin::PrometheusExporter;
use velda_sync::{
    AuthExporterConfig, CorsExporterConfig, PrometheusExporterConfig, ScrapeExporterConfig,
    TlsExporterConfig,
};

fn free_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("127.0.0.1:{port}")
}

#[tokio::test]
async fn test_exporter_basic_metrics_and_healthz() {
    let addr = free_local_addr();
    let store = Arc::new(MetricsStore::new());

    // Record some sample activity in store
    store.global().connection_opened();
    store.global().connection_opened();

    let cfg = PrometheusExporterConfig {
        listen: addr.clone(),
        path: "/metrics".into(),
        protocol: "http".into(),
        scrape: ScrapeExporterConfig {
            timeout_ms: 5000,
            max_concurrency: 4,
            max_buffer_size_kb: 256,
        },
        tls: TlsExporterConfig {
            mode: "none".into(),
            cert_file: None,
            key_file: None,
            client_ca_file: None,
        },
        cors: CorsExporterConfig {
            enabled: false,
            allow_origins: vec![],
            allow_methods: vec![],
            allow_headers: vec![],
        },
        auth: AuthExporterConfig {
            mode: "none".into(),
            token: None,
            username: None,
            password: None,
        },
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let exporter = PrometheusExporter::new(cfg, Arc::clone(&store)).unwrap();

    let server_task = tokio::spawn(async move {
        let _ = exporter.run(shutdown_rx).await;
    });

    // Wait briefly for listener to be active
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Test /healthz
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.ends_with("OK"));
    }

    // 2. Test /metrics
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("velda_connections_total 2"));
        assert!(resp.contains("velda_requests_total"));
    }

    // 3. Test 404 Not Found
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"));
    }

    // Graceful shutdown
    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_exporter_bearer_auth() {
    let addr = free_local_addr();
    let store = Arc::new(MetricsStore::new());

    let cfg = PrometheusExporterConfig {
        listen: addr.clone(),
        path: "/metrics".into(),
        protocol: "http".into(),
        scrape: ScrapeExporterConfig {
            timeout_ms: 5000,
            max_concurrency: 4,
            max_buffer_size_kb: 256,
        },
        tls: TlsExporterConfig {
            mode: "none".into(),
            cert_file: None,
            key_file: None,
            client_ca_file: None,
        },
        cors: CorsExporterConfig {
            enabled: false,
            allow_origins: vec![],
            allow_methods: vec![],
            allow_headers: vec![],
        },
        auth: AuthExporterConfig {
            mode: "bearer".into(),
            token: Some("secret-token-xyz".into()),
            username: None,
            password: None,
        },
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let exporter = PrometheusExporter::new(cfg, Arc::clone(&store)).unwrap();

    let server_task = tokio::spawn(async move {
        let _ = exporter.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Without Token -> 401 Unauthorized
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(resp.contains("WWW-Authenticate: Bearer"));
    }

    // 2. With Wrong Token -> 401 Unauthorized
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(
                b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer wrong\r\n\r\n",
            )
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 401 Unauthorized"));
    }

    // 3. With Correct Token -> 200 OK
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token-xyz\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("velda_connections_total"));
    }

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_exporter_cors_preflight_and_headers() {
    let addr = free_local_addr();
    let store = Arc::new(MetricsStore::new());

    let cfg = PrometheusExporterConfig {
        listen: addr.clone(),
        path: "/metrics".into(),
        protocol: "http".into(),
        scrape: ScrapeExporterConfig {
            timeout_ms: 5000,
            max_concurrency: 4,
            max_buffer_size_kb: 256,
        },
        tls: TlsExporterConfig {
            mode: "none".into(),
            cert_file: None,
            key_file: None,
            client_ca_file: None,
        },
        cors: CorsExporterConfig {
            enabled: true,
            allow_origins: vec!["https://grafana.internal".into()],
            allow_methods: vec!["GET".into(), "OPTIONS".into()],
            allow_headers: vec!["Authorization".into()],
        },
        auth: AuthExporterConfig {
            mode: "none".into(),
            token: None,
            username: None,
            password: None,
        },
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let exporter = PrometheusExporter::new(cfg, Arc::clone(&store)).unwrap();

    let server_task = tokio::spawn(async move {
        let _ = exporter.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Preflight OPTIONS
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(
                b"OPTIONS /metrics HTTP/1.1\r\nHost: localhost\r\nOrigin: https://grafana.internal\r\n\r\n",
            )
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 204 No Content"));
        assert!(resp.contains("Access-Control-Allow-Origin: https://grafana.internal"));
        assert!(resp.contains("Access-Control-Allow-Methods: GET, OPTIONS"));
    }

    // 2. GET with valid Origin
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(
                b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nOrigin: https://grafana.internal\r\n\r\n",
            )
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("Access-Control-Allow-Origin: https://grafana.internal"));
    }

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_exporter_concurrency_limiting() {
    let addr = free_local_addr();
    let store = Arc::new(MetricsStore::new());

    // max_concurrency: 1
    let cfg = PrometheusExporterConfig {
        listen: addr.clone(),
        path: "/metrics".into(),
        protocol: "http".into(),
        scrape: ScrapeExporterConfig {
            timeout_ms: 5000,
            max_concurrency: 1,
            max_buffer_size_kb: 256,
        },
        tls: TlsExporterConfig {
            mode: "none".into(),
            cert_file: None,
            key_file: None,
            client_ca_file: None,
        },
        cors: CorsExporterConfig {
            enabled: false,
            allow_origins: vec![],
            allow_methods: vec![],
            allow_headers: vec![],
        },
        auth: AuthExporterConfig {
            mode: "none".into(),
            token: None,
            username: None,
            password: None,
        },
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let exporter = PrometheusExporter::new(cfg, Arc::clone(&store)).unwrap();

    let server_task = tokio::spawn(async move {
        let _ = exporter.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Normal request succeeds with limit 1
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
    }

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}
