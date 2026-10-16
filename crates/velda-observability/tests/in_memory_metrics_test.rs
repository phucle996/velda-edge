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

#[test]
fn test_pool_metrics_cacheline_alignment() {
    assert_eq!(
        std::mem::align_of::<PoolMetrics>(),
        64,
        "PoolMetrics MUST be aligned to 64 bytes (L1 CPU cacheline boundary)"
    );
}

#[test]
fn test_pool_metrics_hit_miss_and_lease_lifecycle() {
    let store = MetricsStore::new();
    let pool = store.pool();

    // 1. Initial snapshot state
    let initial_snap = store.snapshot();
    assert_eq!(initial_snap.pool.hit_rate(), 1.0);
    assert_eq!(initial_snap.pool.total_lease_attempts(), 0);
    assert_eq!(initial_snap.pool.connections_leased, 0);

    // 2. Perform lease hit & miss
    pool.record_hit();
    pool.record_hit();
    pool.record_hit();
    pool.record_miss();
    pool.record_timeout();

    let snap = store.snapshot();
    assert_eq!(snap.pool.lease_hits_total, 3);
    assert_eq!(snap.pool.lease_misses_total, 1);
    assert_eq!(snap.pool.lease_timeouts_total, 1);
    assert_eq!(snap.pool.total_lease_attempts(), 5);
    // Hit rate = 3 / (3 + 1) = 0.75
    assert!((snap.pool.hit_rate() - 0.75).abs() < f64::EPSILON);
    // Leased count = 3 hits + 1 miss = 4 leased
    assert_eq!(snap.pool.connections_leased, 4);

    // 3. Return 2 connections to pool
    pool.record_release();
    pool.record_release();
    assert_eq!(store.snapshot().pool.connections_leased, 2);
    assert_eq!(store.snapshot().pool.lease_releases_total, 2);

    // 4. Drop 1 connection due to broken pipe during request
    pool.record_lease_dropped();
    assert_eq!(store.snapshot().pool.connections_leased, 1);
}

#[test]
fn test_pool_metrics_eviction_classification_and_lifespan_histogram() {
    let store = MetricsStore::new();
    let pool = store.pool();

    // Record various evictions with lifespans:
    // 1. Idle timeout after 3s (<= 5s)
    pool.record_eviction(EvictionReason::IdleTimeout, Duration::from_secs(3));
    // 2. Unhealthy socket after 20s (<= 30s)
    pool.record_eviction(EvictionReason::Unhealthy, Duration::from_secs(20));
    // 3. Capacity exceeded after 120s (<= 300s / 5m)
    pool.record_eviction(EvictionReason::CapacityExceeded, Duration::from_secs(120));
    // 4. Goaway frame after 1500s (<= 1800s / 30m)
    pool.record_eviction(EvictionReason::Goaway, Duration::from_secs(1500));
    // 5. Max lifetime reached after 3500s (<= 3600s / 1h)
    pool.record_eviction(EvictionReason::MaxLifetime, Duration::from_secs(3500));
    // 6. Long-lived connection evicted after 7200s (> 1h)
    pool.record_eviction(EvictionReason::IdleTimeout, Duration::from_secs(7200));

    let snap = store.snapshot().pool;

    // Verify eviction classification
    assert_eq!(snap.evictions_idle_timeout, 2);
    assert_eq!(snap.evictions_unhealthy, 1);
    assert_eq!(snap.evictions_capacity_exceeded, 1);
    assert_eq!(snap.evictions_goaway, 1);
    assert_eq!(snap.evictions_max_lifetime, 1);
    assert_eq!(snap.total_evictions(), 6);

    // Verify cumulative histogram buckets
    // <= 5s: [3s] -> 1
    assert_eq!(snap.lifespan_le_5s, 1);
    // <= 30s: [3s, 20s] -> 2
    assert_eq!(snap.lifespan_le_30s, 2);
    // <= 5m: [3s, 20s, 120s] -> 3
    assert_eq!(snap.lifespan_le_5m, 3);
    // <= 30m: [3s, 20s, 120s, 1500s] -> 4
    assert_eq!(snap.lifespan_le_30m, 4);
    // <= 1h: [3s, 20s, 120s, 1500s, 3500s] -> 5
    assert_eq!(snap.lifespan_le_1h, 5);
    // > 1h: [7200s] -> 1
    assert_eq!(snap.lifespan_gt_1h, 1);
    // Total lifespan recorded = 5 + 1 = 6
    assert_eq!(snap.total_lifespan_recorded(), 6);
}

#[test]
fn test_pool_metrics_containers_and_multiplexed_gauges() {
    let store = MetricsStore::new();
    let pool = store.pool();

    // Containers & idle gauges
    pool.set_pool_containers(42);
    pool.set_idle_connections(128);
    pool.record_connection_created();
    pool.record_connection_created();

    // Multiplexed connections & active streams
    pool.set_multiplexed_connections(4);
    pool.mux_stream_started();
    pool.mux_stream_started();
    pool.mux_stream_started();
    pool.mux_stream_started();
    pool.mux_stream_started();
    pool.mux_stream_started();
    pool.mux_stream_started();
    pool.mux_stream_started(); // 8 active streams across 4 mux conns

    let snap = store.snapshot().pool;
    assert_eq!(snap.pool_containers_total, 42);
    assert_eq!(snap.connections_idle, 128);
    assert_eq!(snap.connections_created_total, 2);
    assert_eq!(snap.connections_multiplexed, 4);
    assert_eq!(snap.multiplexed_active_streams, 8);
    // 8 streams / 4 connections = 2.0 avg
    assert!((snap.avg_streams_per_mux_conn() - 2.0).abs() < f64::EPSILON);

    // End 2 streams
    pool.mux_stream_ended();
    pool.mux_stream_ended();
    assert_eq!(store.snapshot().pool.multiplexed_active_streams, 6);
    assert!((store.snapshot().pool.avg_streams_per_mux_conn() - 1.5).abs() < f64::EPSILON);
}

#[test]
fn test_concurrent_pool_operations() {
    let store = Arc::new(MetricsStore::new());
    let mut handles = Vec::new();

    let threads = 16;
    let ops_per_thread = 10_000;

    for _ in 0..threads {
        let store_clone = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            let pool = store_clone.pool();
            for i in 0..ops_per_thread {
                if i % 3 == 0 {
                    pool.record_hit();
                    pool.record_release();
                } else if i % 3 == 1 {
                    pool.record_miss();
                    pool.record_release();
                } else {
                    pool.record_hit();
                    pool.record_lease_dropped();
                    pool.record_eviction(EvictionReason::IdleTimeout, Duration::from_secs(2));
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let snap = store.snapshot().pool;
    let expected_ops = (threads * ops_per_thread) as u64;

    // Hits + Misses should equal total ops
    assert_eq!(
        snap.lease_hits_total + snap.lease_misses_total,
        expected_ops
    );
    // All leased connections were either released or dropped -> leased gauge must be 0
    assert_eq!(snap.connections_leased, 0);
    // Evictions recorded on 1/3 of iterations
    let expected_evictions =
        (threads * (ops_per_thread / 3 + if ops_per_thread % 3 > 2 { 1 } else { 0 })) as u64;
    assert_eq!(snap.evictions_idle_timeout, expected_evictions);
    assert_eq!(snap.lifespan_le_5s, expected_evictions);
}
