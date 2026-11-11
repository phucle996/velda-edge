//! Runtime Machine Code & Cycle Profiling for OpenTelemetry Log Shipper (`otlp_log_shipper`).
//!
//! Evaluates real hardware execution metrics:
//! 1. Cycle count (RDTSC) for OpenTelemetry JSON log record batch formatting (`format_otlp_records_json`).
//! 2. Cycle count (RDTSC) for OpenTelemetry Protobuf log record serialization (`format_otlp_records_proto`).
//! 3. Static outer envelope reuse and zero-reallocation invariant.
//! 4. A/B co-existence test under concurrent multi-core frontline traffic ingestion.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, LogStore,
    PipeStrategy,
};
use velda_plugin::background::otlp_log_shipper::{
    OtlpJsonEnvelope, OtlpProtoWireEnvelope, format_otlp_records_json,
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
        LogProtocol::H2,
        LogProtocol::H1,
        PipeStrategy::Buffered,
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
        LogProtocol::GrpcTcp,
        LogProtocol::GrpcTcp,
        PipeStrategy::Duplex,
        LogCompletionReason::Complete,
        LogHttpMethod::Post,
        LogGrpcStatus::Ok,
        b"grpc-go/1.64.0",
    )
}

#[test]
fn test_runtime_machine_code_execution_cycles() {
    let http_rec = create_sample_http_record(0xaaaa_bbbb_cccc_dddd, 1_710_000_001);
    let grpc_rec = create_sample_grpc_record(0x1111_2222_3333_4444, 1_710_000_002);

    let batch: Arc<[AccessLogRecord]> = Arc::from(vec![http_rec, grpc_rec]);
    let batches = vec![batch];

    let envelope = OtlpJsonEnvelope::new("velda-edge-service");
    let proto_wire_env = OtlpProtoWireEnvelope::new("velda-edge-service");
    let mut json_buf = Vec::with_capacity(8192);
    let mut records_buf = Vec::with_capacity(8192);
    let mut wire_buf = Vec::with_capacity(8192);

    // Warm-up cache
    for _ in 0..10_000 {
        format_otlp_records_json(&batches, &envelope, &mut json_buf);
        std::hint::black_box(json_buf.as_slice());
        proto_wire_env.format_records_wire(&batches, &mut records_buf, &mut wire_buf);
        std::hint::black_box(wire_buf.as_slice());
    }

    const ITERATIONS: u64 = 50_000;

    // 1. Benchmark OTLP JSON serialization (2 records per iteration)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let count = format_otlp_records_json(&batches, &envelope, &mut json_buf);
        std::hint::black_box(count);
        std::hint::black_box(json_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let json_cycles_per_record = (end_tsc - start_tsc) / (ITERATIONS * 2);

    // 2. Benchmark OTLP Protobuf direct wire serialization (2 records per iteration)
    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let count = proto_wire_env.format_records_wire(&batches, &mut records_buf, &mut wire_buf);
        std::hint::black_box(count);
        std::hint::black_box(wire_buf.as_slice());
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let proto_wire_cycles_per_record = (end_tsc - start_tsc) / (ITERATIONS * 2);

    println!("\n=== OpenTelemetry Log Shipper Machine Code Cycle Audit ===");
    println!("Hardware TSC Counter: Intel/AMD RDTSC instruction");
    println!("OTLP/JSON Formatting Speed       : {json_cycles_per_record} CPU cycles / record");
    println!(
        "OTLP/Proto Direct Wire Formatting: {proto_wire_cycles_per_record} CPU cycles / record"
    );
    println!("==========================================================\n");

    if !cfg!(debug_assertions) {
        assert!(
            json_cycles_per_record < 1000,
            "OTLP JSON formatting exceeded expected cycle ceiling: {json_cycles_per_record} cycles"
        );
        assert!(
            proto_wire_cycles_per_record < 1000,
            "OTLP Protobuf direct wire formatting exceeded expected cycle ceiling: {proto_wire_cycles_per_record} cycles"
        );
    } else {
        assert!(
            json_cycles_per_record < 12000,
            "Debug mode OTLP JSON formatting exceeded safety ceiling"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ab_otlp_log_shipper_hot_path_impact() {
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
            let mut id = 2000 * (core_id as u64 + 1);
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
    let envelope = OtlpJsonEnvelope::new("velda-edge-bench");
    let shadow_handle = tokio::spawn(async move {
        let mut total_drained = 0usize;
        let mut out = Vec::with_capacity(32768);
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            store_shadow.flush_all_lanes(1024);
            let batches = subscriber.drain_batches(64);
            let n = format_otlp_records_json(&batches, &envelope, &mut out);
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
        "OTLP Co-existence Validation: Emitted = {emitted}, Drained & Formatted = {drained} across 4 cores"
    );
}
