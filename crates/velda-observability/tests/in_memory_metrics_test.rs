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
        DownstreamProtocol::H1,
        DownstreamProtocol::H2,
        DownstreamProtocol::H3,
    ] {
        for u in [
            UpstreamProtocol::H1,
            UpstreamProtocol::H2,
            UpstreamProtocol::H3,
        ] {
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
        DownstreamProtocol::H2,
        UpstreamProtocol::H1,
        PipeStrategy::ServerStream,
    );

    let initial_snap = store.snapshot();
    let initial_pipe = initial_snap
        .find_pipeline(
            DownstreamProtocol::H2,
            UpstreamProtocol::H1,
            PipeStrategy::ServerStream,
        )
        .expect("pipeline slot must exist");
    assert_eq!(initial_pipe.active_streams, 1);
    assert_eq!(initial_pipe.total_requests(), 0);

    store.record_stream_completion(
        DownstreamProtocol::H2,
        UpstreamProtocol::H1,
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
            DownstreamProtocol::H2,
            UpstreamProtocol::H1,
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
                    DownstreamProtocol::H1,
                    UpstreamProtocol::H1,
                    PipeStrategy::Buffered,
                );
                store_clone.record_stream_completion(
                    DownstreamProtocol::H1,
                    UpstreamProtocol::H1,
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
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
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
        DownstreamProtocol::H3,
        UpstreamProtocol::H2,
        PipeStrategy::Duplex,
    );

    tel.on_start();
    tel.on_bytes_rx(500);
    tel.on_ttfb();
    tel.on_bytes_tx(2000);
    tel.on_finish(200);

    let snap = store.snapshot();
    let pipe = snap
        .find_pipeline(
            DownstreamProtocol::H3,
            UpstreamProtocol::H2,
            PipeStrategy::Duplex,
        )
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

#[test]
fn test_vectorized_pipeline_aggregation_and_basis_points() {
    let store = MetricsStore::new();

    // Pipeline 1: 100 requests (90 success 2xx, 10 errors 5xx)
    for _ in 0..90 {
        store.record_stream_completion(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            StreamCompletion {
                status_code: 200,
                bytes_rx: 100,
                bytes_tx: 500,
                duration: Duration::from_millis(2),
                ttfb: Some(Duration::from_millis(1)),
            },
        );
    }
    for _ in 0..10 {
        store.record_stream_completion(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            StreamCompletion {
                status_code: 500,
                bytes_rx: 50,
                bytes_tx: 200,
                duration: Duration::from_millis(50),
                ttfb: Some(Duration::from_millis(20)),
            },
        );
    }

    // Pipeline 2: 50 requests (50 success 2xx)
    for _ in 0..50 {
        store.record_stream_completion(
            DownstreamProtocol::H2,
            UpstreamProtocol::H2,
            PipeStrategy::Duplex,
            StreamCompletion {
                status_code: 200,
                bytes_rx: 200,
                bytes_tx: 1000,
                duration: Duration::from_millis(8),
                ttfb: Some(Duration::from_millis(3)),
            },
        );
    }

    let snap = store.snapshot();
    let p1 = snap
        .find_pipeline(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
        )
        .unwrap();

    assert_eq!(p1.total_requests(), 100);
    assert_eq!(p1.success_rate_basis_points(), 9_000); // 90.00%
    assert!((p1.success_rate() - 0.90).abs() < f64::EPSILON);

    // Vectorized aggregation across all 36 slots
    let totals = snap.aggregate_pipelines();
    assert_eq!(totals.total_requests(), 150);
    assert_eq!(totals.requests_2xx, 140);
    assert_eq!(totals.requests_5xx, 10);
    assert_eq!(totals.bytes_rx, 90 * 100 + 10 * 50 + 50 * 200);
    assert_eq!(totals.bytes_tx, 90 * 500 + 10 * 200 + 50 * 1000);
    // 140 / 150 = 93.33% = 9333 basis points
    assert_eq!(totals.success_rate_basis_points(), 9_333);

    // Verify snapshot.total_requests() uses SIMD aggregation
    assert_eq!(snap.total_requests(), 150);
}

#[test]
fn test_per_core_sharded_simd_aggregation() {
    let store = MetricsStore::with_cores(4);
    assert_eq!(store.active_cores(), 4);

    // Each core records to its own isolated shard
    for core_id in 0..4 {
        store.record_stream_completion_on_core(
            core_id,
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            StreamCompletion {
                status_code: 200,
                bytes_rx: 100,
                bytes_tx: 500,
                duration: Duration::from_millis(core_id as u64 + 1),
                ttfb: Some(Duration::from_millis(1)),
            },
        );
    }

    let snap = store.snapshot();
    let p = snap
        .find_pipeline(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
        )
        .unwrap();

    assert_eq!(p.total_requests(), 4);
    assert_eq!(p.requests_2xx, 4);
    assert_eq!(p.bytes_rx, 400);
    assert_eq!(p.bytes_tx, 2000);
    assert_eq!(p.success_rate_basis_points(), 10_000);
}

#[test]
fn test_multi_hardware_accumulate_fn_dispatch_equivalence() {
    // Instantiate 3 sharded stores bound explicitly to 3 distinct hardware widths:
    // 256-bit (AVX2), 128-bit (SSE2 / NEON), and 64-bit (Scalar ALU)
    let store_256 = MetricsStore::with_cores_and_fn(4, add_counters_256);
    let store_128 = MetricsStore::with_cores_and_fn(4, add_counters_128);
    let store_64 = MetricsStore::with_cores_and_fn(4, add_counters_64);

    let stores = [&store_256, &store_128, &store_64];

    // Seed diverse completions across all 4 core shards
    let test_cases = [
        (
            0,
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            200,
            100,
            500,
            Duration::from_millis(1),
            Some(Duration::from_millis(1)),
        ),
        (
            1,
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            302,
            50,
            150,
            Duration::from_millis(4),
            Some(Duration::from_millis(2)),
        ),
        (
            2,
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            404,
            80,
            200,
            Duration::from_millis(8),
            Some(Duration::from_millis(4)),
        ),
        (
            3,
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            500,
            20,
            100,
            Duration::from_millis(45),
            Some(Duration::from_millis(25)),
        ),
        (
            0,
            DownstreamProtocol::H2,
            UpstreamProtocol::H2,
            PipeStrategy::Duplex,
            200,
            1024,
            4096,
            Duration::from_millis(150),
            Some(Duration::from_millis(80)),
        ),
        (
            1,
            DownstreamProtocol::H2,
            UpstreamProtocol::H2,
            PipeStrategy::Duplex,
            502,
            30,
            80,
            Duration::from_millis(800),
            Some(Duration::from_millis(200)),
        ),
        (
            2,
            DownstreamProtocol::H2,
            UpstreamProtocol::H2,
            PipeStrategy::Duplex,
            200,
            5000,
            12000,
            Duration::from_millis(1500),
            Some(Duration::from_millis(400)),
        ),
        (
            3,
            DownstreamProtocol::H3,
            UpstreamProtocol::H2,
            PipeStrategy::ServerStream,
            200,
            256,
            1024,
            Duration::from_millis(3),
            Some(Duration::from_millis(1)),
        ),
    ];

    for store in &stores {
        for (core_id, down, up, pipe, status, rx, tx, dur, ttfb) in test_cases {
            store.record_stream_completion_on_core(
                core_id,
                down,
                up,
                pipe,
                StreamCompletion {
                    status_code: status,
                    bytes_rx: rx,
                    bytes_tx: tx,
                    duration: dur,
                    ttfb,
                },
            );
        }
    }

    let snap_256 = store_256.snapshot();
    let snap_128 = store_128.snapshot();
    let snap_64 = store_64.snapshot();

    // Verify bit-for-bit equivalence across all 36 pipeline snapshots
    assert_eq!(snap_256.pipelines.len(), 36);
    assert_eq!(snap_128.pipelines.len(), 36);
    assert_eq!(snap_64.pipelines.len(), 36);

    for i in 0..36 {
        let p_256 = &snap_256.pipelines[i];
        let p_128 = &snap_128.pipelines[i];
        let p_64 = &snap_64.pipelines[i];

        assert_eq!(
            p_256, p_128,
            "Pipeline slot {} mismatch between 256 and 128",
            i
        );
        assert_eq!(
            p_256, p_64,
            "Pipeline slot {} mismatch between 256 and 64",
            i
        );
    }

    // Verify bit-for-bit equivalence across aggregate totals
    let tot_256 = snap_256.aggregate_pipelines();
    let tot_128 = snap_128.aggregate_pipelines();
    let tot_64 = snap_64.aggregate_pipelines();

    assert_eq!(tot_256, tot_128);
    assert_eq!(tot_256, tot_64);

    assert_eq!(tot_256.total_requests(), 8);
    assert_eq!(tot_256.requests_2xx, 4);
    assert_eq!(tot_256.requests_3xx, 1);
    assert_eq!(tot_256.requests_4xx, 1);
    assert_eq!(tot_256.requests_5xx, 2);
}

#[test]
fn test_aggregate_pipelines_with_explicit_function_pointers() {
    let store = MetricsStore::new();

    store.record_stream_completion(
        DownstreamProtocol::H1,
        UpstreamProtocol::H1,
        PipeStrategy::Buffered,
        StreamCompletion {
            status_code: 200,
            bytes_rx: 200,
            bytes_tx: 800,
            duration: Duration::from_millis(5),
            ttfb: Some(Duration::from_millis(2)),
        },
    );

    let snap = store.snapshot();

    // Call aggregate_pipelines_with with all function pointers explicitly
    let res_256 = snap.aggregate_pipelines_with(add_counters_256);
    let res_128 = snap.aggregate_pipelines_with(add_counters_128);
    let res_64 = snap.aggregate_pipelines_with(add_counters_64);

    assert_eq!(res_256, res_128);
    assert_eq!(res_256, res_64);
    assert_eq!(res_256.total_requests(), 1);
    assert_eq!(res_256.requests_2xx, 1);
    assert_eq!(res_256.bytes_rx, 200);
    assert_eq!(res_256.bytes_tx, 800);
}

#[test]
fn test_resolve_counters_fn_returns_callable_pointer() {
    use velda_core::hardware::RegisterWidth;

    let fn_256 = resolve_counters_fn(RegisterWidth::Bits256);
    let fn_128 = resolve_counters_fn(RegisterWidth::Bits128);
    let fn_64 = resolve_counters_fn(RegisterWidth::Bits64);

    let mut dst = [10u64; 20];
    let src = [5u64; 20];

    // Each pointer must be callable without panic or invalid instruction
    fn_256(&mut dst, &src);
    assert_eq!(dst[0], 15);

    fn_128(&mut dst, &src);
    assert_eq!(dst[0], 20);

    fn_64(&mut dst, &src);
    assert_eq!(dst[0], 25);
}

#[test]
fn test_workload_adaptive_counters_and_narrow_slice_safety() {
    // 1. Check adaptive function resolution matching workload length
    let fn_20 = resolve_counters_fn_for_len(20);
    let fn_4 = resolve_counters_fn_for_len(4);
    let fn_2 = resolve_counters_fn_for_len(2);
    let fn_1 = resolve_counters_fn_for_len(1);

    let mut buf = [100u64; 20];
    let src = [10u64; 20];

    // Standard workload (20 counters)
    fn_20(&mut buf, &src);
    assert_eq!(buf[0], 110);
    assert_eq!(buf[19], 110);

    // Narrow workload (4 counters)
    fn_4(&mut buf[..4], &src[..4]);
    assert_eq!(buf[0], 120);

    // Narrow workload (2 counters) adapts to 128-bit
    fn_2(&mut buf[..2], &src[..2]);
    assert_eq!(buf[0], 130);

    // Single scalar counter adapts directly to 64-bit ALU
    fn_1(&mut buf[..1], &src[..1]);
    assert_eq!(buf[0], 140);

    // 2. Direct calls to add_counters_256 on narrow slices (< 4 and < 2)
    // MUST adapt safely to 128-bit and scalar without panics, SIGILL, or out-of-bounds
    let mut small_dst = [5u64; 3];
    let small_src = [2u64; 3];
    add_counters_256(&mut small_dst, &small_src);
    assert_eq!(small_dst, [7, 7, 7]);

    let mut single_dst = [42u64; 1];
    let single_src = [8u64; 1];
    add_counters_256(&mut single_dst, &single_src);
    assert_eq!(single_dst[0], 50);

    let mut empty_dst: [u64; 0] = [];
    let empty_src: [u64; 0] = [];
    add_counters_256(&mut empty_dst, &empty_src);
}
