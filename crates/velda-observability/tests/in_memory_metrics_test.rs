//! Comprehensive In-Memory Metrics Tests for `velda-observability`.

use std::sync::Arc;
use std::time::Duration;
use velda_observability::*;

#[test]
fn test_noop_telemetry_is_zero_sized_type() {
    assert_eq!(
        std::mem::size_of::<NoopTelemetry>(),
        0,
        "NoopTelemetry MUST be a Zero-Sized Type (ZST) with size = 0 bytes"
    );
}

#[test]
fn test_pipeline_slot_cacheline_alignment() {
    assert_eq!(
        std::mem::align_of::<PipelineSlot>(),
        64,
        "PipelineSlot MUST be aligned to 64 bytes (L1 CPU cacheline length)"
    );
}

#[test]
fn test_total_slots_matches_cartesian_product() {
    assert_eq!(
        TOTAL_PIPELINE_SLOTS, 36,
        "Total pipeline slots must be exactly 3 * 3 * 4 = 36"
    );
}

#[test]
fn test_slot_index_bijective_encoding_decoding() {
    let mut seen_indices = std::collections::HashSet::new();

    for d in [
        DownstreamProto::H1,
        DownstreamProto::H2,
        DownstreamProto::H3,
    ] {
        for u in [UpstreamProto::H1, UpstreamProto::H2, UpstreamProto::H3] {
            for p in [
                PipeStrategy::Buffered,
                PipeStrategy::ServerStream,
                PipeStrategy::ClientStream,
                PipeStrategy::Duplex,
            ] {
                let idx = MetricsStore::slot_index(d, u, p);
                assert!(
                    idx < TOTAL_PIPELINE_SLOTS,
                    "Slot index {idx} out of range [0, 36)"
                );

                assert!(
                    seen_indices.insert(idx),
                    "Duplicate slot index {idx} encountered!"
                );

                let (dec_d, dec_u, dec_p) = MetricsStore::decode_slot_index(idx);
                assert_eq!(dec_d, d);
                assert_eq!(dec_u, u);
                assert_eq!(dec_p, p);
            }
        }
    }

    assert_eq!(seen_indices.len(), 36);
}

#[test]
fn test_status_class_classification() {
    assert_eq!(StatusClass::from_status_code(200), StatusClass::Success2xx);
    assert_eq!(StatusClass::from_status_code(204), StatusClass::Success2xx);
    assert_eq!(StatusClass::from_status_code(301), StatusClass::Redirect3xx);
    assert_eq!(StatusClass::from_status_code(304), StatusClass::Redirect3xx);
    assert_eq!(
        StatusClass::from_status_code(400),
        StatusClass::ClientError4xx
    );
    assert_eq!(
        StatusClass::from_status_code(404),
        StatusClass::ClientError4xx
    );
    assert_eq!(
        StatusClass::from_status_code(500),
        StatusClass::ServerError5xx
    );
    assert_eq!(
        StatusClass::from_status_code(502),
        StatusClass::ServerError5xx
    );
    assert_eq!(StatusClass::from_status_code(101), StatusClass::Other);
}

#[test]
fn test_record_completion_and_snapshot() {
    let store = MetricsStore::new();

    store.record_stream_start(
        DownstreamProto::H2,
        UpstreamProto::H1,
        PipeStrategy::ServerStream,
    );

    let initial_snap = store.snapshot();
    let initial_pipe = initial_snap
        .find_pipeline(
            DownstreamProto::H2,
            UpstreamProto::H1,
            PipeStrategy::ServerStream,
        )
        .expect("pipeline slot must exist");
    assert_eq!(initial_pipe.active_streams, 1);
    assert_eq!(initial_pipe.total_requests(), 0);

    store.record_stream_completion(
        DownstreamProto::H2,
        UpstreamProto::H1,
        PipeStrategy::ServerStream,
        StreamCompletion::new(
            200,
            128,
            1024,
            Duration::from_millis(3),
            Some(Duration::from_millis(1)),
        ),
    );

    let snap = store.snapshot();
    let pipe = snap
        .find_pipeline(
            DownstreamProto::H2,
            UpstreamProto::H1,
            PipeStrategy::ServerStream,
        )
        .unwrap();

    assert_eq!(pipe.active_streams, 0);
    assert_eq!(pipe.requests_2xx, 1);
    assert_eq!(pipe.requests_5xx, 0);
    assert_eq!(pipe.total_requests(), 1);
    assert_eq!(pipe.bytes_rx, 128);
    assert_eq!(pipe.bytes_tx, 1024);
    assert_eq!(pipe.duration_le_5ms, 1);
    assert_eq!(pipe.ttfb_le_1ms, 1);
    assert_eq!(pipe.success_rate(), 1.0);
}

#[test]
fn test_concurrent_multi_threaded_recording() {
    let store = Arc::new(MetricsStore::new());
    let mut handles = Vec::new();

    let threads = 16;
    let requests_per_thread = 10_000;

    for _ in 0..threads {
        let store_clone = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for i in 0..requests_per_thread {
                let status = if i % 10 == 0 { 500 } else { 200 };
                store_clone.record_stream_start(
                    DownstreamProto::H1,
                    UpstreamProto::H1,
                    PipeStrategy::Buffered,
                );
                store_clone.record_stream_completion(
                    DownstreamProto::H1,
                    UpstreamProto::H1,
                    PipeStrategy::Buffered,
                    StreamCompletion::new(
                        status,
                        64,
                        256,
                        Duration::from_millis(2),
                        Some(Duration::from_millis(1)),
                    ),
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let snap = store.snapshot();
    let pipe = snap
        .find_pipeline(
            DownstreamProto::H1,
            UpstreamProto::H1,
            PipeStrategy::Buffered,
        )
        .unwrap();

    let expected_total = threads * requests_per_thread;
    assert_eq!(pipe.total_requests(), expected_total as u64);
    assert_eq!(pipe.active_streams, 0);
    assert_eq!(pipe.requests_5xx, (expected_total / 10) as u64);
    assert_eq!(
        pipe.requests_2xx,
        (expected_total - (expected_total / 10)) as u64
    );
    assert_eq!(pipe.bytes_rx, expected_total as u64 * 64);
    assert_eq!(pipe.bytes_tx, expected_total as u64 * 256);
}

#[test]
fn test_live_telemetry_flow() {
    let store = Arc::new(MetricsStore::new());
    let mut tel = LiveTelemetry::new(
        Arc::clone(&store),
        DownstreamProto::H3,
        UpstreamProto::H2,
        PipeStrategy::Duplex,
    );

    tel.on_start();
    tel.on_bytes_rx(500);
    tel.on_ttfb();
    tel.on_bytes_tx(2000);
    tel.on_finish(200);

    let snap = store.snapshot();
    let pipe = snap
        .find_pipeline(DownstreamProto::H3, UpstreamProto::H2, PipeStrategy::Duplex)
        .unwrap();

    assert_eq!(pipe.total_requests(), 1);
    assert_eq!(pipe.requests_2xx, 1);
    assert_eq!(pipe.bytes_rx, 500);
    assert_eq!(pipe.bytes_tx, 2000);
    assert_eq!(pipe.active_streams, 0);
}

#[test]
fn test_noop_telemetry_inlining() {
    let mut tel = NoopTelemetry;
    tel.on_start();
    tel.on_bytes_rx(100);
    tel.on_ttfb();
    tel.on_bytes_tx(200);
    tel.on_finish(200);
    // Compiles to literally nothing
}
