//! Runtime Machine Code & Cycle Profiling for OTLP Metrics Exporter
//!
//! Measures real CPU cycles, division-free integer and timestamp formatting,
//! in-place Protobuf template encoding (`OtlpProtoTemplate`),
//! and full OpenTelemetry JSON formatting throughput.

use http::HeaderMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use velda_observability::MetricsStore;
use velda_observability::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};
use velda_plugin::background::otlp_metrics_exporter::OtlpMetricsClient;
use velda_plugin::background::otlp_metrics_exporter::format::{
    fast_format_u128_into, fast_write_i64, fast_write_u64, format_otlp_metrics_into,
};
use velda_plugin::background::otlp_metrics_exporter::proto::OtlpProtoTemplate;

#[test]
fn test_runtime_machine_code_execution_cycles() {
    let store = MetricsStore::new();
    // Record sample activity across pipelines
    store.global().connection_opened();
    store.global().connection_opened();
    let snapshot = store.snapshot();

    let mut buf = Vec::with_capacity(64);
    let mut fmt_buf = Vec::with_capacity(64);
    let mut ts_buf = [0u8; 40];

    // Warm up caches
    for i in 0..10_000u64 {
        let n = std::hint::black_box(12345 + (i & 0xF));
        buf.clear();
        fast_write_u64(&mut buf, 7);
        buf.clear();
        fast_write_u64(&mut buf, n);
        buf.clear();
        fast_write_u64(&mut buf, u64::MAX - i);
        buf.clear();
        fast_write_i64(&mut buf, -987654321);

        fast_format_u128_into(&mut ts_buf, 1_700_000_000_000_000_000u128 + i as u128);

        fmt_buf.clear();
        let _ = write!(&mut fmt_buf, "{}", n);
    }

    const ITERATIONS: u64 = 1_000_000;

    // 1. Single digit fast path (< 10)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let n = std::hint::black_box(i % 10);
        buf.clear();
        fast_write_u64(&mut buf, n);
        std::hint::black_box(buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let single_digit_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 2. Typical multi-digit number (12,345..12,360) via 2-digit LUT
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let n = std::hint::black_box(12345 + (i & 0xF));
        buf.clear();
        fast_write_u64(&mut buf, n);
        std::hint::black_box(buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let typical_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 3. Full 64-bit max integer (20 digits)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let n = std::hint::black_box(u64::MAX - (i & 0xF));
        buf.clear();
        fast_write_u64(&mut buf, n);
        std::hint::black_box(buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let max_u64_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 4. Negative integer fast_write_i64 (-987,654,321)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let n = std::hint::black_box(-987654321 - (i as i64 & 0xF));
        buf.clear();
        fast_write_i64(&mut buf, n);
        std::hint::black_box(buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let signed_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 5. 128-bit nanosecond timestamp formatter (fast_format_u128_into)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let ts = std::hint::black_box(1_700_000_000_000_000_000u128 + i as u128);
        let len = fast_format_u128_into(&mut ts_buf, ts);
        std::hint::black_box(&ts_buf[..len]);
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let timestamp_u128_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 6. Standard library std::fmt::write! baseline comparison
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let n = std::hint::black_box(12345 + (i & 0xF));
        fmt_buf.clear();
        let _ = write!(&mut fmt_buf, "{}", n);
        std::hint::black_box(fmt_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let std_fmt_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 7. In-Place Protobuf Template Mutation & Encoding (OtlpProtoTemplate)
    const PROTO_ITERATIONS: u64 = 10_000;
    let mut proto_template = OtlpProtoTemplate::new("velda-edge-bench");
    let mut proto_buf = Vec::with_capacity(32 * 1024);
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..PROTO_ITERATIONS {
        proto_buf.clear();
        proto_template.update_and_encode(&snapshot, &mut proto_buf);
        std::hint::black_box(proto_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let proto_template_cycles = (end_tsc - start_tsc) / PROTO_ITERATIONS;

    // 8. Full OpenTelemetry ResourceMetrics JSON Formatter (format_otlp_metrics_into)
    const JSON_ITERATIONS: u64 = 10_000;
    let mut json_buf = Vec::with_capacity(64 * 1024);
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..JSON_ITERATIONS {
        json_buf.clear();
        format_otlp_metrics_into(&snapshot, "velda-edge-bench", &mut json_buf);
        std::hint::black_box(json_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let json_format_cycles = (end_tsc - start_tsc) / JSON_ITERATIONS;

    println!("============================================================");
    println!(" OTLP METRICS EXPORTER REAL RUNTIME CPU CYCLES (RDTSC)");
    println!("============================================================");
    println!(" 1. fast_write_u64(n < 10) [Single Digit]:       {single_digit_cycles} cycles/call");
    println!(" 2. fast_write_u64(n = 12345) [Typical LUT]:    {typical_cycles} cycles/call");
    println!(" 3. fast_write_u64(n = u64::MAX) [20 Digits]:   {max_u64_cycles} cycles/call");
    println!(" 4. fast_write_i64(n < 0) [Signed Negative]:     {signed_cycles} cycles/call");
    println!(
        " 5. fast_format_u128_into(ts_nanos) [Unix Time]: {timestamp_u128_cycles} cycles/call"
    );
    println!(" 6. std::fmt::write!(n = 12345) [Std Baseline]: {std_fmt_cycles} cycles/call");
    println!(
        " 7. OtlpProtoTemplate::update_and_encode() [PB]: {proto_template_cycles} cycles/call"
    );
    println!(" 8. format_otlp_metrics_into() [OTel JSON]:      {json_format_cycles} cycles/call");
    println!("============================================================");

    if !cfg!(debug_assertions) {
        assert!(
            typical_cycles < 30,
            "fast_write_u64 typical cycles should be < 30 cycles in release mode"
        );
        assert!(
            typical_cycles <= std_fmt_cycles,
            "fast_write_u64 must be <= std_fmt_cycles"
        );
    }
}

#[tokio::test]
async fn test_ab_comparative_traffic_coexistence() {
    let store_b = Arc::new(MetricsStore::new());
    const WORKER_REQUESTS: u64 = 200_000;
    let completion = StreamCompletion::new(
        200,
        512,
        1024,
        Duration::from_micros(350),
        Some(Duration::from_micros(120)),
    );

    // --- VARIANT B: Raw Proxy Baseline (Zero background plugins active) ---
    let start_b = Instant::now();
    let start_tsc_b = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..WORKER_REQUESTS {
        let core_id = (i & 1) as usize;
        store_b.global().connection_opened();
        let slot = store_b.slot_on_core(
            core_id,
            DownstreamProto::H1,
            UpstreamProto::H1,
            PipeStrategy::Buffered,
        );
        slot.stream_started();
        slot.record_completion(std::hint::black_box(&completion));
        slot.stream_ended();
    }
    let end_tsc_b = unsafe { core::arch::x86_64::_rdtsc() };
    let duration_b = start_b.elapsed();
    let cycles_per_req_b = (end_tsc_b - start_tsc_b) / WORKER_REQUESTS;

    // --- VARIANT A: Velda Edge + Active OTLP Exporter pushing to mock collector ---
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let export_count = Arc::new(AtomicU64::new(0));
    let export_count_clone = Arc::clone(&export_count);
    let server_running = Arc::new(AtomicBool::new(true));
    let server_running_clone = Arc::clone(&server_running);

    // Mock OTLP HTTP/1.1 Collector receiving POST /v1/metrics
    let collector_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    if !server_running_clone.load(Ordering::Relaxed) {
                        break;
                    }
                }
                res = listener.accept() => {
                    if let Ok((mut socket, _)) = res {
                        let exp_c = Arc::clone(&export_count_clone);
                        tokio::spawn(async move {
                            let mut buf = [0u8; 8192];
                            while let Ok(n) = socket.read(&mut buf).await {
                                if n == 0 { break; }
                                let s = String::from_utf8_lossy(&buf[..n]);
                                if s.contains("/v1/metrics") || s.starts_with("POST ") {
                                    exp_c.fetch_add(1, Ordering::Relaxed);
                                    let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
                                    let _ = socket.write_all(resp).await;
                                }
                            }
                        });
                    }
                }
            }
        }
    });

    let collector_url = format!("http://{addr}/v1/metrics");
    let mut otlp_client = OtlpMetricsClient::new(
        &collector_url,
        Duration::from_secs(5),
        Duration::from_secs(10),
    )
    .expect("OTLP Client initialized");

    let store_a = Arc::new(MetricsStore::new());
    let store_a_clone = Arc::clone(&store_a);
    let exporter_running = Arc::new(AtomicBool::new(true));
    let exporter_running_clone = Arc::clone(&exporter_running);
    let custom_headers = HeaderMap::new();

    // Active Shadow Logistics Exporter Thread (pushes every 2ms)
    let exporter_task = tokio::spawn(async move {
        let mut json_scratch = Vec::with_capacity(32 * 1024);
        while exporter_running_clone.load(Ordering::Relaxed) {
            let snap = store_a_clone.snapshot();
            json_scratch.clear();
            format_otlp_metrics_into(&snap, "velda-edge-ab-test", &mut json_scratch);
            let _ = otlp_client
                .send_metrics(&json_scratch, &custom_headers)
                .await;
            tokio::task::yield_now().await;
        }
    });

    // Run Frontline Worker simulation concurrently with active OTLP export pushes
    let start_a = Instant::now();
    let start_tsc_a = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..WORKER_REQUESTS {
        let core_id = (i & 1) as usize;
        store_a.global().connection_opened();
        let slot = store_a.slot_on_core(
            core_id,
            DownstreamProto::H1,
            UpstreamProto::H1,
            PipeStrategy::Buffered,
        );
        slot.stream_started();
        slot.record_completion(std::hint::black_box(&completion));
        slot.stream_ended();
        if i % 10_000 == 0 {
            tokio::task::yield_now().await;
        }
    }
    let end_tsc_a = unsafe { core::arch::x86_64::_rdtsc() };
    let duration_a = start_a.elapsed();
    let cycles_per_req_a = (end_tsc_a - start_tsc_a) / WORKER_REQUESTS;

    exporter_running.store(false, Ordering::Relaxed);
    server_running.store(false, Ordering::Relaxed);
    let _ = exporter_task.await;
    collector_task.abort();

    let completed_exports = export_count.load(Ordering::Relaxed);

    println!("============================================================");
    println!(" A/B COMPARATIVE EVALUATION: OTLP EXPORTER VS RAW PROXY");
    println!("============================================================");
    println!(
        " Variant B (Raw Proxy, 0 plugins):    {cycles_per_req_b} cycles/req ({duration_b:?})"
    );
    println!(
        " Variant A (OTLP Exporter Active):    {cycles_per_req_a} cycles/req ({duration_a:?})"
    );
    println!(" Concurrent Export Batches Delivered: {completed_exports} batches during run");
    println!(
        " Cycles Delta (A vs B):               {} cycles/req",
        cycles_per_req_a as i64 - cycles_per_req_b as i64
    );
    println!("============================================================");

    assert!(
        completed_exports > 0,
        "OTLP Exporter must successfully deliver metric batches concurrently with frontline traffic"
    );
}
