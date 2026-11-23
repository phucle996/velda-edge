//! Integration tests for `otlp_trace_exporter` via `velda_etp::otlp::OtlpEtp`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prost_otlp::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_etp::otlp::OtlpEtp;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_observability::{TelemetryRecord, TelemetryStore};
use velda_plugin::PluginConfig;
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::otlp_trace_exporter::wire::OTLP_TRACES_GRPC_PATH;
use velda_plugin::background::otlp_trace_exporter::{
    OtlpTraceExporterConfig, OtlpTraceExporterPlugin,
};

fn make_otlp_etp(id: &str, endpoint: String, protocol: &str) -> Arc<OtlpEtp> {
    Arc::new(
        OtlpEtp::from_entry(&EtpEntryConfig {
            id: id.to_string(),
            kind: EtpKind::Otlp,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "endpoint": endpoint,
                "protocol": protocol,
                "headers": {}
            }),
        })
        .unwrap(),
    )
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buf = Vec::with_capacity(16384);
    let mut tmp = [0u8; 4096];
    let hdr_end = loop {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
    let cl = headers
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buf.len() < hdr_end + cl {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    Some((headers, buf[hdr_end..hdr_end + cl].to_vec()))
}

async fn spawn_mock_http_trace_collector(
    expected_content_type: &'static str,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    watch::Sender<bool>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Bind mock collector");
    let port = listener.local_addr().unwrap().port();
    let base_url = format!("http://127.0.0.1:{port}");

    let conns = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let conns_clone = Arc::clone(&conns);
    let reqs_clone = Arc::clone(&reqs);
    let bodies_clone = Arc::clone(&bodies);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok((mut stream, _)) = listener.accept() => {
                    conns_clone.fetch_add(1, Ordering::SeqCst);
                    let reqs_stream = Arc::clone(&reqs_clone);
                    let bodies_stream = Arc::clone(&bodies_clone);
                    tokio::spawn(async move {
                        while let Some((headers, body)) = read_http_request(&mut stream).await {
                            assert!(
                                headers.starts_with("POST /v1/traces HTTP/1.1\r\n"),
                                "Unexpected request line in headers: {headers}"
                            );
                            assert!(
                                headers.contains(&format!("Content-Type: {expected_content_type}\r\n")),
                                "Missing expected Content-Type {expected_content_type}: {headers}"
                            );
                            bodies_stream.lock().await.push(body);
                            reqs_stream.fetch_add(1, Ordering::SeqCst);
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
                            if stream.write_all(response.as_bytes()).await.is_err() {
                                break;
                            }
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

    (base_url, conns, reqs, bodies, shutdown_tx)
}

#[tokio::test]
async fn trace_exporter_e2e() {
    let (url, conns, reqs, _bodies, shutdown_tx) =
        spawn_mock_http_trace_collector("application/json").await;

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(2, 64));
    let etp = make_otlp_etp("otlp-trace-json", url, "http/json");

    let config = OtlpTraceExporterConfig {
        etp_id: "otlp-trace-json".to_string(),
        service_name: "test-service".to_string(),
        flush_interval_ms: 20,
        max_batch_size: 100,
        request_timeout_ms: 2000,
    };

    let exporter = Arc::new(
        OtlpTraceExporterPlugin::with_etp("trace_exp", config, etp, Arc::clone(&store))
            .expect("Exporter created"),
    );

    assert_eq!(exporter.pbww(), 50);
    assert_eq!(exporter.etp_id(), Some("otlp-trace-json"));

    let (stop_tx, stop_rx) = watch::channel(false);
    let exp_clone = Arc::clone(&exporter);
    let handle = tokio::spawn(async move {
        exp_clone.run(stop_rx).await.expect("Exporter ran");
    });

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let catalog = velda_router::RouteCatalog::build(["/", "/api/orders"]);
    velda_router::set_global_route_catalog(catalog);

    // Emit 5 unified records across core 0 and core 1
    for i in 0..5 {
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_000_000_000 + i * 1_000_000;
        rec.duration_ns = 1_000_000;
        rec.request_id = i + 1;
        rec.status_code = 200;
        rec.route_id = 1;

        store.emit_on_core((i % 2) as usize, &rec);
    }
    store.flush_all_lanes(100);

    tokio::time::timeout(Duration::from_secs(2), async {
        while exporter.shipped_spans.load(Ordering::Relaxed) < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Timed out waiting for shipped_spans == 5");

    assert!(reqs.load(Ordering::SeqCst) >= 1);
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    assert_eq!(exporter.shipped_spans.load(Ordering::Relaxed), 5);
    assert_eq!(exporter.failed_spans.load(Ordering::Relaxed), 0);

    let _ = stop_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_tx.send(true);

    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn trace_exporter_http_protobuf_e2e() {
    let (url, _conns, reqs, bodies, shutdown_tx) =
        spawn_mock_http_trace_collector("application/x-protobuf").await;

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 64));
    let etp = make_otlp_etp("otlp-trace-pb", url, "http");
    let config = OtlpTraceExporterConfig {
        etp_id: "otlp-trace-pb".to_string(),
        service_name: "pb-trace-service".to_string(),
        flush_interval_ms: 20,
        ..Default::default()
    };

    let exporter = Arc::new(
        OtlpTraceExporterPlugin::with_etp("trace_pb", config, etp, Arc::clone(&store)).unwrap(),
    );

    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(Arc::clone(&exporter).run(stop_rx));
    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut rec = TelemetryRecord::zeroed();
    rec.ingress_time_nanos = 2_000_000_000;
    rec.duration_ns = 500_000;
    rec.request_id = 42;
    rec.status_code = 201;
    rec.route_id = 0;
    store.emit_on_core(0, &rec);
    store.flush_all_lanes(64);

    tokio::time::timeout(Duration::from_secs(2), async {
        while exporter.shipped_spans.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Timed out waiting for protobuf trace export");

    assert!(reqs.load(Ordering::SeqCst) >= 1);
    let recorded = bodies.lock().await;
    let decoded = <opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest as Message>::decode(
        recorded[0].as_slice(),
    )
    .expect("valid ExportTraceServiceRequest protobuf");
    assert_eq!(decoded.resource_spans.len(), 1);
    assert_eq!(decoded.resource_spans[0].scope_spans[0].spans.len(), 1);

    let _ = stop_tx.send(true);
    handle.await.unwrap().unwrap();
    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn trace_exporter_grpc_e2e() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let reqs = Arc::new(AtomicUsize::new(0));
    let decoded_spans = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let reqs_c = Arc::clone(&reqs);
    let spans_c = Arc::clone(&decoded_spans);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                res = listener.accept() => {
                    let Ok((socket, _)) = res else { break };
                    let reqs_s = Arc::clone(&reqs_c);
                    let spans_s = Arc::clone(&spans_c);
                    tokio::spawn(async move {
                        let Ok(mut conn) = h2::server::handshake(socket).await else { return };
                        while let Some(Ok((req, mut respond))) = conn.accept().await {
                            assert_eq!(req.uri().path(), OTLP_TRACES_GRPC_PATH);
                            let mut raw = Vec::new();
                            let mut body = req.into_body();
                            while let Some(Ok(chunk)) = body.data().await {
                                let _ = body.flow_control().release_capacity(chunk.len());
                                raw.extend_from_slice(&chunk);
                            }
                            assert!(raw.len() >= 5);
                            assert_eq!(raw[0], 0);
                            let msg_len = u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
                            let msg = <opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest as Message>::decode(
                                &raw[5..5 + msg_len],
                            )
                            .expect("valid gRPC ExportTraceServiceRequest");
                            spans_s.fetch_add(msg.resource_spans[0].scope_spans[0].spans.len(), Ordering::SeqCst);
                            reqs_s.fetch_add(1, Ordering::SeqCst);

                            let resp = http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "0")
                                .body(())
                                .unwrap();
                            let _ = respond.send_response(resp, true);
                        }
                    });
                }
            }
        }
    });

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 64));
    let etp = make_otlp_etp("otlp-trace-grpc", format!("grpc://{addr}"), "grpc");
    let config = OtlpTraceExporterConfig {
        etp_id: "otlp-trace-grpc".to_string(),
        service_name: "grpc-trace-service".to_string(),
        flush_interval_ms: 20,
        ..Default::default()
    };

    let exporter = Arc::new(
        OtlpTraceExporterPlugin::with_etp("trace_grpc", config, etp, Arc::clone(&store)).unwrap(),
    );

    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(Arc::clone(&exporter).run(stop_rx));
    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut rec = TelemetryRecord::zeroed();
    rec.ingress_time_nanos = 3_000_000_000;
    rec.duration_ns = 900_000;
    rec.request_id = 99;
    rec.status_code = 200;
    store.emit_on_core(0, &rec);
    store.flush_all_lanes(64);

    tokio::time::timeout(Duration::from_secs(2), async {
        while exporter.shipped_spans.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Timed out waiting for gRPC trace export");

    assert_eq!(reqs.load(Ordering::SeqCst), 1);
    assert_eq!(decoded_spans.load(Ordering::SeqCst), 1);

    let _ = stop_tx.send(true);
    handle.await.unwrap().unwrap();
    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn trace_exporter_from_config_requires_attached_etp_and_shutdowns_cleanly() {
    let plugin_cfg = PluginConfig {
        id: "otlp_trace_exporter".into(),
        config: serde_json::json!({
            "etp_id": "otlp-main",
            "service_name": "edge-traces",
            "flush_interval_ms": 50
        }),
    };

    let unattached = Arc::new(OtlpTraceExporterPlugin::from_config(&plugin_cfg).unwrap());
    assert_eq!(unattached.etp_id(), Some("otlp-main"));
    let (_tx, rx) = watch::channel(false);
    let err = unattached.run(rx).await.unwrap_err();
    assert!(err.to_string().contains("no attached OtlpEtp"));

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 32));
    let etp = make_otlp_etp("otlp-main", "http://127.0.0.1:4318".into(), "http/json");
    let attached = Arc::new(
        OtlpTraceExporterPlugin::with_etp(
            "otlp_trace_test",
            OtlpTraceExporterConfig {
                etp_id: "otlp-main".into(),
                flush_interval_ms: 50,
                ..Default::default()
            },
            etp,
            store,
        )
        .unwrap(),
    );
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(attached.run(rx));
    tx.send(true).unwrap();
    assert!(handle.await.unwrap().is_ok());
}
