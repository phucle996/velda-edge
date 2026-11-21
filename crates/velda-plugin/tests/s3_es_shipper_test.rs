use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use flate2::read::GzDecoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
use velda_observability::{TelemetryRecord, TelemetryStore};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::elasticsearch_log_shipper::{
    ElasticsearchLogShipperConfig, ElasticsearchLogShipperPlugin, EsAuthConfig, EsBulkOpType,
    EsCompression,
};
use velda_plugin::background::s3_log_shipper::{
    S3AddressingStyle, S3Compression, S3LogShipperConfig, S3LogShipperPlugin,
};
use velda_plugin::config::PluginConfig;
use velda_plugin::validation::validate_plugin_config;

fn make_record(req_id: u64, url: &str) -> TelemetryRecord {
    let mut rec = TelemetryRecord::zeroed();
    rec.ingress_time_nanos = 1_732_015_800_123_456_789;
    rec.request_id = req_id;
    rec.duration_ns = 45_000;
    rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0105;
    rec.upstream_ip = 0x0A00_0002;
    rec.upstream_port = 8081;
    rec.route_id = 3;
    rec.status_code = 200;
    rec.pipe = PipeKind::BufferedH1H1;
    rec.method = HttpMethod::Get;
    rec.completion = CompletionReason::Complete;
    rec.grpc_status = GrpcStatus::Ok;
    rec.ua_family = UaFamily::Curl;
    rec.ua_os = UaOs::Linux;
    rec.set_url(url);
    rec
}

async fn read_http_request(listener: &TcpListener) -> (String, Vec<u8>) {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 2048];
    let header_end;
    loop {
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "unexpected EOF reading request");
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            header_end = pos + 4;
            break;
        }
    }
    let headers = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let content_len = headers
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    while buf.len() < header_end + content_len {
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "unexpected EOF reading body");
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = buf[header_end..header_end + content_len].to_vec();

    let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n{\"errors\":false}";
    sock.write_all(resp).await.unwrap();
    sock.flush().await.unwrap();

    (headers, body)
}

#[tokio::test]
async fn test_elasticsearch_shipper_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let store = Arc::new(TelemetryStore::new());
    let cfg = ElasticsearchLogShipperConfig {
        endpoints: vec![format!("http://{addr}")],
        index: "velda-test".to_string(),
        op_type: EsBulkOpType::Create,
        compression: EsCompression::Gzip,
        auth: EsAuthConfig::ApiKey {
            key: "test-key".to_string(),
        },
        flush_interval_ms: 25,
        ..Default::default()
    };

    let plugin = Arc::new(
        ElasticsearchLogShipperPlugin::with_store("es_test", cfg, Arc::clone(&store)).unwrap(),
    );
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(Arc::clone(&plugin).run(rx));

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_record(1, "/es/one"));
    store.emit_on_core(0, &make_record(2, "/es/two"));
    store.flush_all_lanes(128);

    let (headers, body) =
        tokio::time::timeout(Duration::from_secs(3), read_http_request(&listener))
            .await
            .unwrap();

    assert!(headers.starts_with("POST /_bulk HTTP/1.1\r\n"));
    assert!(headers.contains("Content-Encoding: gzip\r\n"));
    assert!(headers.contains("Authorization: ApiKey test-key\r\n"));

    let mut dec = GzDecoder::new(body.as_slice());
    let mut raw = String::new();
    dec.read_to_string(&mut raw).unwrap();
    let lines: Vec<&str> = raw.lines().collect();
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], r#"{"create":{"_index":"velda-test"}}"#);
    assert!(lines[1].contains("/es/one"));
    assert!(lines[3].contains("/es/two"));

    for _ in 0..50 {
        if plugin.shipped_records() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(plugin.shipped_records(), 2);

    let _ = tx.send(true);
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_s3_shipper_end_to_end_sigv4_and_zstd() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let store = Arc::new(TelemetryStore::new());
    let cfg = S3LogShipperConfig {
        endpoint: format!("http://{addr}"),
        bucket: "velda-bucket".to_string(),
        region: "us-east-1".to_string(),
        prefix: "edge-logs".to_string(),
        addressing_style: S3AddressingStyle::Path,
        access_key_id: "AKIA_TEST".to_string(),
        secret_access_key: "SECRET_TEST".to_string(),
        compression: S3Compression::Zstd,
        flush_interval_ms: 25,
        ..Default::default()
    };

    let plugin =
        Arc::new(S3LogShipperPlugin::with_store("s3_test", cfg, Arc::clone(&store)).unwrap());
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(Arc::clone(&plugin).run(rx));

    while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    store.emit_on_core(0, &make_record(10, "/s3/alpha"));
    store.emit_on_core(0, &make_record(11, "/s3/beta"));
    store.flush_all_lanes(128);

    let (headers, body) =
        tokio::time::timeout(Duration::from_secs(3), read_http_request(&listener))
            .await
            .unwrap();

    assert!(headers.starts_with("PUT /velda-bucket/edge-logs/year="));
    assert!(headers.contains(".jsonl.zst HTTP/1.1\r\n"));
    assert!(headers.contains("Authorization: AWS4-HMAC-SHA256 Credential=AKIA_TEST/"));
    assert!(headers.contains("x-amz-content-sha256: "));

    let decompressed = zstd::bulk::decompress(&body, 64 * 1024).unwrap();
    let text = std::str::from_utf8(&decompressed).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("/s3/alpha"));
    assert!(lines[1].contains("/s3/beta"));

    for _ in 0..50 {
        if plugin.shipped_records() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(plugin.shipped_records(), 2);

    let _ = tx.send(true);
    handle.await.unwrap().unwrap();
}

#[test]
fn test_validation_accepts_es_and_s3_background_plugins() {
    let mut es = PluginConfig {
        id: "elasticsearch_log_shipper".to_string(),
        phase: "background".to_string(),
        config: serde_json::json!({
            "endpoints": ["http://127.0.0.1:9200"],
            "index": "velda-logs"
        }),
    };
    let mut s3 = PluginConfig {
        id: "s3_log_shipper".to_string(),
        phase: "background".to_string(),
        config: serde_json::json!({
            "endpoint": "http://127.0.0.1:9000",
            "bucket": "velda-bucket",
            "region": "us-east-1",
            "access_key_id": "AKIA",
            "secret_access_key": "SECRET"
        }),
    };
    validate_plugin_config(&mut es).unwrap();
    validate_plugin_config(&mut s3).unwrap();
}
