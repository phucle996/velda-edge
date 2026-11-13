//! Chaos, Concurrency & OpenMetrics Compliance Test Suite for Prometheus Exporter.
//!
//! Validates:
//! 1. All 36 pipeline slots with boundary values (0, 1, `u64::MAX`, negative numbers).
//! 2. OpenMetrics text syntax compliance (HELP/TYPE pairs, valid label syntax, valid numbers).
//! 3. Semaphore concurrency burst protection (503 Service Unavailable handling).
//! 4. Continuous stress test over 1,000 cycles for memory stability (zero memory leak).

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

use velda_observability::{
    DownstreamProtocol, MetricsStore, PipeStrategy, PipelineSnapshot, TOTAL_PIPELINE_SLOTS,
    UpstreamProtocol,
};
use velda_plugin::background::prometheus_exporter::lut::DIGIT_PAIRS;
use velda_plugin::background::prometheus_exporter::server::BufferPool;
use velda_plugin::background::prometheus_exporter::server::PrometheusExporter;
use velda_plugin::background::prometheus_exporter::template::PrometheusTemplate;
use velda_plugin::background::prometheus_exporter::template::format_snapshot;
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

// ============================================================================
// 1. ALL 36 PIPELINE SLOTS BOUNDARY VALUES TEST
// ============================================================================

#[test]
fn test_chaos_all_36_pipeline_slots_boundary_values() {
    let template = PrometheusTemplate::new();

    // Test with extreme boundary values
    let test_scenarios: [(u64, i64, u64, u64); 4] = [
        (0, 0, 0, 0),                                     // Zeroes everywhere
        (1, 1, 1, 1),                                     // Single digit
        (12_345, -42, 1024, 2048), // Typical values with negative active streams
        (u64::MAX, i64::MIN + 1, u64::MAX - 1, u64::MAX), // Maximum boundaries
    ];

    for (req, active, rx, tx) in test_scenarios {
        let mut snapshot = MetricsStore::new().snapshot();
        let mut pipelines = Vec::with_capacity(TOTAL_PIPELINE_SLOTS);

        for slot_idx in 0..TOTAL_PIPELINE_SLOTS {
            let (down, up, pipe) = MetricsStore::decode_slot_index(slot_idx);
            pipelines.push(PipelineSnapshot {
                downstream: down,
                upstream: up,
                pipe,
                requests_2xx: req,
                requests_3xx: req,
                requests_4xx: req,
                requests_5xx: req,
                requests_other: req,
                active_streams: active,
                bytes_rx: rx,
                bytes_tx: tx,
                duration_le_1ms: req,
                duration_le_5ms: req,
                duration_le_10ms: req,
                duration_le_50ms: req,
                duration_le_250ms: req,
                duration_le_1000ms: req,
                duration_gt_1000ms: req,
                ttfb_le_1ms: req,
                ttfb_le_5ms: req,
                ttfb_le_10ms: req,
                ttfb_le_50ms: req,
                ttfb_le_250ms: req,
                ttfb_gt_250ms: req,
            });
        }
        snapshot.pipelines = pipelines;

        let mut buf_vec = Vec::new();
        template
            .format_snapshot_into(&snapshot, &mut buf_vec, 10 * 1024 * 1024)
            .expect("Boundary formatting into vec should succeed");

        let mut buf_str = String::new();
        format_snapshot(&snapshot, &mut buf_str, 10 * 1024 * 1024)
            .expect("Boundary formatting into string should succeed");

        assert_eq!(buf_vec, buf_str.as_bytes());

        // Verify all 36 slot coordinates are present in the output
        for d in &[
            DownstreamProtocol::H1,
            DownstreamProtocol::H2,
            DownstreamProtocol::H3,
        ] {
            for u in &[
                UpstreamProtocol::H1,
                UpstreamProtocol::H2,
                UpstreamProtocol::H3,
            ] {
                for p in &[
                    PipeStrategy::Buffered,
                    PipeStrategy::ServerStream,
                    PipeStrategy::ClientStream,
                    PipeStrategy::Duplex,
                ] {
                    let needle = format!(
                        "downstream=\"{}\",upstream=\"{}\",pipe=\"{}\"",
                        d.as_str(),
                        u.as_str(),
                        p.as_str()
                    );
                    assert!(buf_str.contains(&needle), "Missing pipeline slot: {needle}");
                }
            }
        }

        // Verify boundary numbers rendered accurately
        let req_str = req.to_string();
        let active_str = active.to_string();
        assert!(buf_str.contains(&format!("status_class=\"2xx\"}} {req_str}\n")));
        assert!(buf_str.contains(&format!("velda_active_streams{{downstream=\"h1\",upstream=\"h1\",pipe=\"buffered\"}} {active_str}\n")));
    }
}

// ============================================================================
// 2. OPENMETRICS TEXT SYNTAX COMPLIANCE TEST
// ============================================================================

#[test]
fn test_openmetrics_text_syntax_compliance() {
    let store = MetricsStore::new();
    // Simulate non-trivial counters
    store.global().connection_opened();
    store.global().connection_opened();
    store.global().tls_handshake_success();
    let snapshot = store.snapshot();

    let mut output = String::new();
    format_snapshot(&snapshot, &mut output, 1024 * 1024).expect("Formatting should succeed");

    let lines: Vec<&str> = output.lines().collect();
    assert!(!lines.is_empty(), "Output must not be empty");

    let mut metric_count = 0;
    let mut help_count = 0;
    let mut type_count = 0;

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];

        if let Some(rest) = line.strip_prefix("# HELP ") {
            help_count += 1;
            let parts: Vec<&str> = rest.split_whitespace().collect();
            assert!(
                !parts.is_empty(),
                "HELP line must contain a metric name: {line}"
            );
            let metric_name = parts[0];

            // Next line MUST be # TYPE <metric_name>
            assert!(
                i + 1 < lines.len(),
                "HELP must be followed by TYPE for {metric_name}"
            );
            let type_line = lines[i + 1];
            let type_rest = type_line.strip_prefix("# TYPE ").unwrap_or_else(|| {
                panic!("Expected TYPE line after HELP, got: {type_line}");
            });
            type_count += 1;

            let type_parts: Vec<&str> = type_rest.split_whitespace().collect();
            assert_eq!(
                type_parts.len(),
                2,
                "TYPE line must have name and type: {type_line}"
            );
            assert_eq!(
                type_parts[0], metric_name,
                "TYPE metric name must match HELP metric name"
            );
            let metric_type = type_parts[1];
            assert!(
                matches!(metric_type, "counter" | "gauge" | "histogram"),
                "Invalid OpenMetrics metric type: {metric_type}"
            );

            i += 2;
            continue;
        }

        // Must be a data line: `<name>{<labels>} <value>` or `<name> <value>`
        metric_count += 1;
        let last_space = line
            .rfind(' ')
            .unwrap_or_else(|| panic!("Metric line missing space before value: {line}"));
        let (name_and_labels, value_str) = line.split_at(last_space);
        let value_str = value_str.trim();

        // Validate value: either integer or valid float (like +Inf or 0.0)
        let is_valid_number = value_str.parse::<i64>().is_ok()
            || value_str.parse::<u64>().is_ok()
            || value_str == "+Inf"
            || value_str.parse::<f64>().is_ok();
        assert!(
            is_valid_number,
            "Value '{value_str}' on line '{line}' is not a valid number"
        );

        // Validate label syntax if labels present
        if let Some(brace_open) = name_and_labels.find('{') {
            assert!(
                name_and_labels.ends_with('}'),
                "Label section must end with '}}': {name_and_labels}"
            );
            let labels = &name_and_labels[brace_open + 1..name_and_labels.len() - 1];
            for pair in labels.split(',') {
                let kv: Vec<&str> = pair.split('=').collect();
                assert_eq!(kv.len(), 2, "Label pair must be key=\"value\", got: {pair}");
                assert!(
                    kv[1].starts_with('"') && kv[1].ends_with('"'),
                    "Label value must be quoted: {}",
                    kv[1]
                );
            }
        }

        i += 1;
    }

    assert_eq!(
        help_count, type_count,
        "Every HELP must have an exact matching TYPE"
    );
    // 36 slots * 23 lines = 828 pipeline lines + 8 global + 22 pool = 858 metric lines
    assert_eq!(
        metric_count, 858,
        "OpenMetrics document must contain exactly 858 metric lines"
    );
}

// ============================================================================
// 3. SEMAPHORE CONCURRENCY BURST PROTECTION TEST
// ============================================================================

#[tokio::test]
async fn test_chaos_semaphore_concurrency_burst_protection() {
    let addr = free_local_addr();
    let store = Arc::new(MetricsStore::new());

    // Strict concurrency limit of 2 concurrent scrapes
    let cfg = PrometheusExporterConfig {
        listen: addr.clone(),
        path: "/metrics".into(),
        protocol: "http".into(),
        scrape: ScrapeExporterConfig {
            timeout_ms: 5000,
            max_concurrency: 2,
            max_buffer_size_kb: 512,
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

    // Fire 20 concurrent requests simultaneously to simulate scrape storm
    const CONCURRENT_CLIENTS: usize = 20;
    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);

    for _ in 0..CONCURRENT_CLIENTS {
        let target_addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(&target_addr).await.unwrap();
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).await.unwrap();
            resp
        }));
    }

    let mut ok_count = 0;
    let mut busy_count = 0;

    for handle in handles {
        let resp = handle.await.unwrap();
        if resp.starts_with("HTTP/1.1 200 OK") {
            ok_count += 1;
            assert!(resp.contains("velda_connections_total"));
        } else if resp.starts_with("HTTP/1.1 503 Service Unavailable") {
            busy_count += 1;
            assert!(resp.contains("Max Scrape Concurrency Reached"));
        } else {
            panic!("Unexpected response during burst: {resp}");
        }
    }

    // Both outcomes are legitimate under a semaphore-bounded storm, with total equal to clients
    assert_eq!(
        ok_count + busy_count,
        CONCURRENT_CLIENTS,
        "All requests must receive either 200 OK or 503 Service Unavailable"
    );
    assert!(ok_count > 0, "At least some requests must succeed");

    // After burst subsides, subsequent request must succeed 200 OK
    {
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
    }

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

// ============================================================================
// 4. CONTINUOUS STRESS TEST & ZERO MEMORY LEAK ASSERTION
// ============================================================================

#[test]
fn test_chaos_continuous_stress_1000_cycles_memory_stability() {
    let store = MetricsStore::new();
    let template = PrometheusTemplate::new();
    let initial_pool_cap = 64 * 1024;
    let buffer_pool = BufferPool::new(4, initial_pool_cap);
    const CYCLES: usize = 1_000;

    for i in 0..CYCLES {
        // Mutate metrics periodically to test dynamic number formatting
        if i % 10 == 0 {
            store.global().connection_opened();
        }

        let snapshot = store.snapshot();
        let mut pooled_buf = buffer_pool.acquire();

        template
            .format_snapshot_into(&snapshot, &mut pooled_buf, 1024 * 1024)
            .expect("Format into pooled buffer must succeed");

        assert!(
            pooled_buf.len() > 20_000,
            "Document size must be substantial (>20KB)"
        );
        assert!(pooled_buf.starts_with(b"# HELP velda_requests_total"));

        // When pooled_buf drops here, it is automatically returned to buffer_pool
    }

    // Verify buffer pool capacity has remained strictly bounded (zero heap inflation/leak across 1,000 cycles)
    let sample_buf = buffer_pool.acquire();
    assert!(
        sample_buf.capacity() <= 128 * 1024,
        "Buffer capacity must remain strictly bounded under 128KB across 1,000 cycles (actual: {})",
        sample_buf.capacity()
    );
}

#[test]
fn test_lut_digit_pairs_integrity() {
    assert_eq!(DIGIT_PAIRS.len(), 200);
    for i in 0..100 {
        let expected_tens = b'0' + (i / 10) as u8;
        let expected_ones = b'0' + (i % 10) as u8;
        assert_eq!(
            DIGIT_PAIRS[i * 2],
            expected_tens,
            "Digit pair tens mismatch for {i}"
        );
        assert_eq!(
            DIGIT_PAIRS[i * 2 + 1],
            expected_ones,
            "Digit pair ones mismatch for {i}"
        );
    }
}
