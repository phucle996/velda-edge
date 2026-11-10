//! Runtime Machine Code & Cycle Profiling for Prometheus Exporter Serializer
//!
//! Measures real CPU cycles, division-free reciprocal arithmetic, LUT lookup performance,
//! and full OpenMetrics text formatting throughput.

use std::io::Write;
use velda_observability::MetricsStore;
use velda_plugin::background::prometheus_exporter::formatter::{
    fast_write_i64, fast_write_u64, format_snapshot,
};

#[test]
fn test_runtime_machine_code_execution_cycles() {
    let store = MetricsStore::new();
    // Record sample activity
    store.global().connection_opened();
    store.global().connection_opened();
    let snapshot = store.snapshot();

    let mut buf = Vec::with_capacity(64);
    let mut fmt_buf = Vec::with_capacity(64);

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

    // 5. Standard library std::fmt::write! baseline comparison
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for i in 0..ITERATIONS {
        let n = std::hint::black_box(12345 + (i & 0xF));
        fmt_buf.clear();
        let _ = write!(&mut fmt_buf, "{}", n);
        std::hint::black_box(fmt_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let std_fmt_cycles = (end_tsc - start_tsc) / ITERATIONS;

    // 6. Full OpenMetrics format_snapshot benchmark (10,000 iterations)
    const SNAPSHOT_ITERATIONS: u64 = 10_000;
    let mut render_buf = String::with_capacity(32 * 1024);
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..SNAPSHOT_ITERATIONS {
        let _ = format_snapshot(&snapshot, &mut render_buf, 1024 * 1024);
        std::hint::black_box(render_buf.as_str());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let snapshot_cycles = (end_tsc - start_tsc) / SNAPSHOT_ITERATIONS;

    println!("============================================================");
    println!(" PROMETHEUS EXPORTER REAL RUNTIME CPU CYCLES (RDTSC)");
    println!("============================================================");
    println!(" 1. fast_write_u64(n < 10) [Single Digit]:       {single_digit_cycles} cycles/call");
    println!(" 2. fast_write_u64(n = 12345) [Typical LUT]:    {typical_cycles} cycles/call");
    println!(" 3. fast_write_u64(n = u64::MAX) [20 Digits]:   {max_u64_cycles} cycles/call");
    println!(" 4. fast_write_i64(n < 0) [Signed Negative]:     {signed_cycles} cycles/call");
    println!(" 5. std::fmt::write!(n = 12345) [Std Baseline]: {std_fmt_cycles} cycles/call");
    println!(" 6. format_snapshot() [Full Metric Document]:   {snapshot_cycles} cycles/call");
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::watch;
    use velda_observability::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};
    use velda_plugin::PrometheusExporter;
    use velda_sync::{
        AuthExporterConfig, CorsExporterConfig, PrometheusExporterConfig, ScrapeExporterConfig,
        TlsExporterConfig,
    };

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

    // --- VARIANT A: Velda Edge + Prometheus Exporter active on management port ---
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let store_a = Arc::new(MetricsStore::new());
    let cfg = PrometheusExporterConfig {
        listen: addr.clone(),
        path: "/metrics".into(),
        protocol: "http".into(),
        scrape: ScrapeExporterConfig {
            timeout_ms: 5000,
            max_concurrency: 8,
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
    let exporter = PrometheusExporter::new(cfg, Arc::clone(&store_a)).unwrap();

    let server_task = tokio::spawn(async move {
        let _ = exporter.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify exporter is listening and responsive
    {
        let mut probe_stream = TcpStream::connect(&addr)
            .await
            .expect("Exporter must be reachable");
        probe_stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut probe_resp = String::new();
        probe_stream.read_to_string(&mut probe_resp).await.unwrap();
        assert!(probe_resp.starts_with("HTTP/1.1 200 OK"));
    }

    // Background Scraper Client: Concurrently sends GET /metrics requests
    let scraper_running = Arc::new(AtomicBool::new(true));
    let scraper_running_clone = Arc::clone(&scraper_running);
    let scrape_addr = addr.clone();
    let scrape_count = Arc::new(AtomicU64::new(0));
    let scrape_count_clone = Arc::clone(&scrape_count);

    let scraper_handle = tokio::spawn(async move {
        while scraper_running_clone.load(Ordering::Relaxed) {
            if let Ok(mut stream) = TcpStream::connect(&scrape_addr).await {
                let _ = stream
                    .write_all(
                        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                let mut resp = Vec::with_capacity(16 * 1024);
                let _ = stream.read_to_end(&mut resp).await;
                if !resp.is_empty() {
                    scrape_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::task::yield_now().await;
        }
    });

    // Run Frontline Worker simulation concurrently with active scrapes
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

    scraper_running.store(false, Ordering::Relaxed);
    let _ = scraper_handle.await;
    let _ = shutdown_tx.send(true);
    let _ = server_task.await;

    let completed_scrapes = scrape_count.load(Ordering::Relaxed);

    println!("============================================================");
    println!(" A/B COMPARATIVE EVALUATION: PROMETHEUS EXPORTER VS RAW PROXY");
    println!("============================================================");
    println!(
        " Variant B (Raw Proxy, 0 plugins):    {cycles_per_req_b} cycles/req ({duration_b:?})"
    );
    println!(
        " Variant A (Prometheus Exporter ON):  {cycles_per_req_a} cycles/req ({duration_a:?})"
    );
    println!(" Concurrent Scrapes Completed:        {completed_scrapes} scrapes during run");
    println!(
        " Cycles Delta (A vs B):               {} cycles/req",
        cycles_per_req_a as i64 - cycles_per_req_b as i64
    );
    println!("============================================================");

    // Assert that scrapes succeeded concurrently
    assert!(
        completed_scrapes > 0,
        "Prometheus exporter must serve scrapes concurrently with worker requests"
    );
}
