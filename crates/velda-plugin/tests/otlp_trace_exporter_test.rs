use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

use velda_observability::traces::{SpanRecord, TraceStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::otlp_trace_exporter::{
    OtlpTraceExporterConfig, OtlpTraceExporterPlugin,
};

async fn spawn_mock_trace_collector(
    expected_path: &'static str,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    watch::Sender<bool>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Bind mock collector");
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}{expected_path}");

    let conns = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let conns_clone = Arc::clone(&conns);
    let reqs_clone = Arc::clone(&reqs);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok((mut stream, _)) = listener.accept() => {
                    conns_clone.fetch_add(1, Ordering::SeqCst);
                    let reqs_stream = Arc::clone(&reqs_clone);
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 16384];
                        while let Ok(n) = stream.read(&mut buf).await {
                            if n == 0 { break; }
                            reqs_stream.fetch_add(1, Ordering::SeqCst);
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                            let _ = stream.flush().await;
                        }
                    });
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }
    });

    (url, conns, reqs, shutdown_tx)
}

#[tokio::test]
async fn trace_exporter_e2e() {
    let (url, conns, reqs, shutdown_tx) = spawn_mock_trace_collector("/v1/traces").await;

    let store = Arc::new(TraceStore::with_cores_and_capacity(2, 64));

    let config = OtlpTraceExporterConfig {
        protocol: "http".to_string(),
        endpoint: url,
        service_name: "test-service".to_string(),
        flush_interval_ms: 20,
        max_batch_size: 100,
        connect_timeout_ms: 1000,
        request_timeout_ms: 2000,
        headers: HashMap::new(),
    };

    let exporter = Arc::new(
        OtlpTraceExporterPlugin::with_store("trace_exp", config, Arc::clone(&store))
            .expect("Exporter created"),
    );

    assert_eq!(exporter.weight(), 50);

    let (stop_tx, stop_rx) = watch::channel(false);
    let exp_clone = Arc::clone(&exporter);
    let handle = tokio::spawn(async move {
        exp_clone.run(stop_rx).await.expect("Exporter ran");
    });

    // Subscriber is now registered
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 1);

    let catalog = velda_router::RouteCatalog::build(["/", "/api/orders"]);
    velda_router::set_global_route_catalog(catalog);

    // Emit 5 spans across core 0 and core 1
    for i in 0..5 {
        let mut span = SpanRecord::empty();
        span.start_time_unix_nano = 1_000_000_000 + i * 1_000_000;
        span.end_time_unix_nano = 1_000_000_000 + (i + 1) * 1_000_000;
        span.status_code = 200;
        span.otlp_status = 1;
        span.route_id = 1;
        span.trace_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        span.span_id = [0, 0, 0, 0, 0, 0, 0, i as u8];

        store.emit_on_core((i % 2) as usize, &span);
    }

    // Wait for exporter to tick, drain, format, and ship
    tokio::time::sleep(Duration::from_millis(80)).await;

    assert!(reqs.load(Ordering::SeqCst) >= 1);
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    assert_eq!(exporter.shipped_spans.load(Ordering::Relaxed), 5);
    assert_eq!(exporter.failed_spans.load(Ordering::Relaxed), 0);

    // Graceful shutdown
    let _ = stop_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_tx.send(true);

    // After exporter task drops its subscriber handle, active_subscriber_count returns to 0
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
}
