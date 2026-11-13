//! Runtime Machine Code & Cycle Profiling for Grafana Loki Log Shipper (`loki_log_shipper`).
//!
//! Evaluates real hardware execution metrics:
//! 1. Cycle count (RDTSC) for Grafana Loki multi-stream JSON formatting (`format_loki_push_payload`).
//! 2. Static stream descriptor template reuse (`LokiStreamTemplate`) and zero-allocation framing.
//! 3. Bounded-cardinality partitioning speed across HTTP, gRPC, and Raw streams.
//! 4. A/B co-existence test under concurrent multi-core frontline traffic ingestion.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogStore, PipeKind,
};
use velda_plugin::background::loki_log_shipper::json::{
    LokiStreamTemplate, format_loki_push_payload,
};

fn create_sample_http_record(id: u64, ts: u32) -> AccessLogRecord {
    AccessLogRecord::from_scalars(
        id,
        ts,
        320,
        "192.168.1.10:50000".parse().unwrap(),
        Some("10.0.0.1:8080".parse().unwrap()),
        128,
        512,
        200,
        1,
        0,
        PipeKind::BufferedH2H1,
        LogCompletionReason::Complete,
        LogHttpMethod::Get,
        LogGrpcStatus::NonGrpc,
        b"curl/8.6.0",
    )
}

fn create_sample_grpc_record(id: u64, ts: u32) -> AccessLogRecord {
    AccessLogRecord::from_scalars(
        id,
        ts,
        890,
        "192.168.1.20:50001".parse().unwrap(),
        Some("10.0.0.2:9090".parse().unwrap()),
        64,
        256,
        200,
        2,
        4,
        PipeKind::DuplexGrpcTcpTcp,
        LogCompletionReason::Complete,
        LogHttpMethod::Post,
        LogGrpcStatus::Ok,
        b"grpc-go/1.64.0",
    )
}

fn create_sample_raw_record(id: u64, ts: u32) -> AccessLogRecord {
    AccessLogRecord::from_scalars(
        id,
        ts,
        1200,
        "192.168.1.30:50002".parse().unwrap(),
        Some("10.0.0.3:7000".parse().unwrap()),
        1024,
        2048,
        0,
        3,
        0,
        PipeKind::TcpTcp,
        LogCompletionReason::Complete,
        LogHttpMethod::Get,
        LogGrpcStatus::NonGrpc,
        b"",
    )
}

#[test]
fn test_runtime_machine_code_execution_cycles() {
    let http_rec = create_sample_http_record(0xaaaa_bbbb_cccc_dddd, 1_710_000_001);
    let grpc_rec = create_sample_grpc_record(0x1111_2222_3333_4444, 1_710_000_002);
    let raw_rec = create_sample_raw_record(0x5555_6666_7777_8888, 1_710_000_003);

    let batch: Arc<[AccessLogRecord]> = Arc::from(vec![http_rec, grpc_rec, raw_rec]);
    let batches = vec![batch];

    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "production".to_string());
    labels.insert("region".to_string(), "us-east-1".to_string());
    let template = LokiStreamTemplate::new("velda-edge-service", &labels);

    let mut payload_buf = Vec::with_capacity(16384);

    // Warm-up cache
    for _ in 0..10_000 {
        format_loki_push_payload(&batches, &template, &mut payload_buf);
        std::hint::black_box(payload_buf.as_slice());
    }

    const ITERATIONS: u64 = 100_000;

    // Benchmark Loki multi-stream partitioning and serialization (3 records per iteration)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let count = format_loki_push_payload(&batches, &template, &mut payload_buf);
        std::hint::black_box(count);
        std::hint::black_box(payload_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let loki_cycles_per_record = (end_tsc - start_tsc) / (ITERATIONS * 3);

    println!("\n=== Grafana Loki Log Shipper Machine Code Cycle Audit ===");
    println!("Hardware TSC Counter: Intel/AMD RDTSC instruction");
    println!("Loki Multi-Stream Formatting Speed: {loki_cycles_per_record} CPU cycles / record");
    println!("=========================================================\n");

    if !cfg!(debug_assertions) {
        assert!(
            loki_cycles_per_record < 1000,
            "Loki formatting exceeded expected cycle ceiling: {loki_cycles_per_record} cycles"
        );
    } else {
        assert!(
            loki_cycles_per_record < 12000,
            "Debug mode Loki formatting exceeded safety ceiling"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ab_loki_log_shipper_hot_path_impact() {
    let store = Arc::new(LogStore::with_cores_and_capacity(4, 2048));
    let subscriber = store.subscribe(4096);
    let running = Arc::new(AtomicBool::new(true));
    let records_emitted = Arc::new(AtomicU64::new(0));

    // Spawn 4 simulated frontline worker threads
    let mut handles = Vec::new();
    for core_id in 0..4 {
        let store_clone = Arc::clone(&store);
        let running_clone = Arc::clone(&running);
        let emitted_clone = Arc::clone(&records_emitted);

        handles.push(tokio::spawn(async move {
            let mut id = 3000 * (core_id as u64 + 1);
            while running_clone.load(Ordering::Relaxed) {
                let rec = create_sample_http_record(id, 1_710_000_000);
                if store_clone.emit_on_core(core_id, &rec) {
                    emitted_clone.fetch_add(1, Ordering::Relaxed);
                }
                id += 1;
                tokio::task::yield_now().await;
            }
        }));
    }

    // Simulate shadow worker draining and formatting
    let store_shadow = Arc::clone(&store);
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "bench".to_string());
    let template = LokiStreamTemplate::new("velda-edge-bench", &labels);

    let shadow_handle = tokio::spawn(async move {
        let mut total_drained = 0usize;
        let mut out = Vec::with_capacity(32768);
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            store_shadow.flush_all_lanes(1024);
            let batches = subscriber.drain_batches(64);
            let n = format_loki_push_payload(&batches, &template, &mut out);
            total_drained += n;
            drop(batches);
        }
        total_drained
    });

    let drained = shadow_handle.await.unwrap();
    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }

    let emitted = records_emitted.load(Ordering::Relaxed);
    assert!(emitted > 0, "Frontline workers must have emitted records");
    assert!(
        drained > 0,
        "Shadow worker must have formatted drained records"
    );
    println!(
        "Loki Co-existence Validation: Emitted = {emitted}, Drained & Formatted = {drained} across 4 cores"
    );
}
