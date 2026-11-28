//! Comprehensive In-Memory Metrics Tests for `velda-observability`.

use std::sync::Arc;
use velda_observability::*;

#[test]
fn test_pipeline_slot_cacheline_alignment() {
    assert_eq!(
        std::mem::align_of::<PipelineSlot>(),
        64,
        "PipelineSlot MUST be aligned to 64 bytes (L1 CPU cacheline length)"
    );
}

#[test]
fn test_total_slots_matches_pipe_kind() {
    assert_eq!(
        TOTAL_PIPELINE_SLOTS, 64,
        "Total pipeline slots must be exactly PipeKind::COUNT = 64"
    );
}

#[test]
fn test_slot_index_roundtrip() {
    let mut seen_indices = std::collections::HashSet::new();

    for &pipe in &PipeKind::ALL {
        let idx = MetricsStore::slot_index(pipe);
        assert!(
            idx < TOTAL_PIPELINE_SLOTS,
            "Slot index {idx} out of range [0, 64)"
        );

        assert!(
            seen_indices.insert(idx),
            "Duplicate slot index {idx} encountered!"
        );

        let dec = MetricsStore::decode_slot_index(idx);
        assert_eq!(dec, Some(pipe));
    }

    assert_eq!(seen_indices.len(), 64);
}

#[test]
fn test_status_bitmask_activation_and_snapshot_isolation() {
    let store = MetricsStore::with_cores(2);

    store.record_stream_completion_on_core(
        0,
        PipeKind::BufferedH1H1,
        StreamCompletion::new(InternalStatus::Completed, 10, 20, 1_000_000, 0),
    );
    store.record_stream_completion_on_core(
        1,
        PipeKind::BufferedH1H1,
        StreamCompletion::new(InternalStatus::UpstreamStale, 0, 0, 500_000, 0),
    );

    let snap = store.snapshot();
    let p = snap.find_pipeline(PipeKind::BufferedH1H1).unwrap();
    let expected_mask =
        (1u64 << InternalStatus::Completed as u64) | (1u64 << InternalStatus::UpstreamStale as u64);
    assert_eq!(p.status_mask, expected_mask);
    assert_eq!(p.count(InternalStatus::Completed), 1);
    assert_eq!(p.count(InternalStatus::UpstreamStale), 1);
    assert_eq!(p.count(InternalStatus::RouteNotFound), 0);

    // Untouched pipeline must have an empty bitmask
    let idle = snap.find_pipeline(PipeKind::DuplexH3H3).unwrap();
    assert_eq!(idle.status_mask, 0);
    assert_eq!(idle.total_requests(), 0);
}

#[test]
fn test_record_completion_and_snapshot() {
    let store = MetricsStore::new();

    store.record_stream_start(PipeKind::ServerStreamH2H1);

    let initial_snap = store.snapshot();
    let initial_pipe = initial_snap
        .find_pipeline(PipeKind::ServerStreamH2H1)
        .expect("pipeline slot must exist");
    assert_eq!(initial_pipe.active_streams, 1);
    assert_eq!(initial_pipe.total_requests(), 0);

    store.record_stream_completion(
        PipeKind::ServerStreamH2H1,
        StreamCompletion::new(InternalStatus::Completed, 128, 1024, 3_000_000, 1_000_000),
    );

    let snap = store.snapshot();
    let pipe = snap.find_pipeline(PipeKind::ServerStreamH2H1).unwrap();

    assert_eq!(pipe.active_streams, 0);
    assert_eq!(pipe.count(InternalStatus::Completed), 1);
    assert_eq!(pipe.count(InternalStatus::UpstreamError), 0);
    assert_eq!(pipe.status_mask, 1u64 << InternalStatus::Completed as u64);
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
                let status = if i % 10 == 0 {
                    InternalStatus::InternalError
                } else {
                    InternalStatus::Completed
                };
                store_clone.record_stream_start(PipeKind::BufferedH1H1);
                store_clone.record_stream_completion(
                    PipeKind::BufferedH1H1,
                    StreamCompletion::new(status, 64, 256, 2_000_000, 1_000_000),
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let snap = store.snapshot();
    let pipe = snap.find_pipeline(PipeKind::BufferedH1H1).unwrap();

    let expected_total = threads * requests_per_thread;
    assert_eq!(pipe.total_requests(), expected_total as u64);
    assert_eq!(pipe.active_streams, 0);
    assert_eq!(
        pipe.count(InternalStatus::InternalError),
        (expected_total / 10) as u64
    );
    assert_eq!(
        pipe.count(InternalStatus::Completed),
        (expected_total - (expected_total / 10)) as u64
    );
    assert_eq!(pipe.bytes_rx, expected_total as u64 * 64);
    assert_eq!(pipe.bytes_tx, expected_total as u64 * 256);
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
fn test_pool_hit_miss_lifecycle() {
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
fn test_pool_eviction_and_lifespan() {
    let store = MetricsStore::new();
    let pool = store.pool();

    // Record various evictions with lifespans:
    // 1. Idle timeout after 3s (<= 5s)
    pool.record_eviction(EvictionReason::IdleTimeout, 3_000_000_000);
    // 2. Unhealthy socket after 20s (<= 30s)
    pool.record_eviction(EvictionReason::Unhealthy, 20_000_000_000);
    // 3. Capacity exceeded after 120s (<= 300s / 5m)
    pool.record_eviction(EvictionReason::CapacityExceeded, 120_000_000_000);
    // 4. Goaway frame after 1500s (<= 1800s / 30m)
    pool.record_eviction(EvictionReason::Goaway, 1500 * 1_000_000_000);
    // 5. Max lifetime reached after 3500s (<= 3600s / 1h)
    pool.record_eviction(EvictionReason::MaxLifetime, 3500 * 1_000_000_000);
    // 6. Long-lived connection evicted after 7200s (> 1h)
    pool.record_eviction(EvictionReason::IdleTimeout, 7200 * 1_000_000_000);

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
fn test_pool_containers_and_gauges() {
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
                    pool.record_eviction(EvictionReason::IdleTimeout, 2_000_000_000);
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
fn test_pipeline_aggregation_bps() {
    let store = MetricsStore::new();

    // Pipeline 1: 100 requests (90 completed, 10 internal_error)
    for _ in 0..90 {
        store.record_stream_completion(
            PipeKind::BufferedH1H1,
            StreamCompletion {
                internal_status: InternalStatus::Completed,
                bytes_rx: 100,
                bytes_tx: 500,
                duration_ns: 2_000_000,
                ttfb_ns: 1_000_000,
            },
        );
    }
    for _ in 0..10 {
        store.record_stream_completion(
            PipeKind::BufferedH1H1,
            StreamCompletion {
                internal_status: InternalStatus::InternalError,
                bytes_rx: 50,
                bytes_tx: 200,
                duration_ns: 50_000_000,
                ttfb_ns: 20_000_000,
            },
        );
    }

    // Pipeline 2: 50 requests (50 completed)
    for _ in 0..50 {
        store.record_stream_completion(
            PipeKind::DuplexH2H2,
            StreamCompletion {
                internal_status: InternalStatus::Completed,
                bytes_rx: 200,
                bytes_tx: 1000,
                duration_ns: 8_000_000,
                ttfb_ns: 3_000_000,
            },
        );
    }

    let snap = store.snapshot();
    let p1 = snap.find_pipeline(PipeKind::BufferedH1H1).unwrap();

    assert_eq!(p1.total_requests(), 100);
    assert_eq!(p1.success_rate_basis_points(), 9_000); // 90.00%
    assert!((p1.success_rate() - 0.90).abs() < f64::EPSILON);

    // Vectorized aggregation across all 64 slots
    let totals = snap.aggregate_pipelines();
    assert_eq!(totals.total_requests(), 150);
    assert_eq!(totals.count(InternalStatus::Completed), 140);
    assert_eq!(totals.count(InternalStatus::InternalError), 10);
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
            PipeKind::BufferedH1H1,
            StreamCompletion {
                internal_status: InternalStatus::Completed,
                bytes_rx: 100,
                bytes_tx: 500,
                duration_ns: (core_id as u64 + 1) * 1_000_000,
                ttfb_ns: 1_000_000,
            },
        );
    }

    let snap = store.snapshot();
    let p = snap.find_pipeline(PipeKind::BufferedH1H1).unwrap();

    assert_eq!(p.total_requests(), 4);
    assert_eq!(p.count(InternalStatus::Completed), 4);
    assert_eq!(p.bytes_rx, 400);
    assert_eq!(p.bytes_tx, 2000);
    assert_eq!(p.success_rate_basis_points(), 10_000);
}

#[test]
fn test_sharded_store_snapshot_aggregation() {
    let store = MetricsStore::with_cores(4);

    // Seed diverse completions across all 4 core shards
    let test_cases = [
        (
            0,
            PipeKind::BufferedH1H1,
            InternalStatus::Completed,
            100,
            500,
            1_000_000,
            1_000_000,
        ),
        (
            1,
            PipeKind::BufferedH1H1,
            InternalStatus::ClientCanceled,
            50,
            150,
            4_000_000,
            2_000_000,
        ),
        (
            2,
            PipeKind::BufferedH1H1,
            InternalStatus::RouteNotFound,
            80,
            200,
            8_000_000,
            4_000_000,
        ),
        (
            3,
            PipeKind::BufferedH1H1,
            InternalStatus::InternalError,
            20,
            100,
            45_000_000,
            25_000_000,
        ),
        (
            0,
            PipeKind::DuplexH2H2,
            InternalStatus::Completed,
            1024,
            4096,
            150_000_000,
            80_000_000,
        ),
        (
            1,
            PipeKind::DuplexH2H2,
            InternalStatus::UpstreamError,
            30,
            80,
            800_000_000,
            200_000_000,
        ),
        (
            2,
            PipeKind::DuplexH2H2,
            InternalStatus::Completed,
            5000,
            12000,
            1_500_000_000,
            400_000_000,
        ),
        (
            3,
            PipeKind::ServerStreamH3H2,
            InternalStatus::Completed,
            256,
            1024,
            3_000_000,
            1_000_000,
        ),
    ];

    for (core_id, pipe, status, rx, tx, dur_ns, ttfb_ns) in test_cases {
        store.record_stream_completion_on_core(
            core_id,
            pipe,
            StreamCompletion {
                internal_status: status,
                bytes_rx: rx,
                bytes_tx: tx,
                duration_ns: dur_ns,
                ttfb_ns,
            },
        );
    }

    let snap = store.snapshot();

    // Verify all 64 pipeline snapshots
    assert_eq!(snap.pipelines.len(), 64);

    let tot = snap.aggregate_pipelines();
    assert_eq!(tot.total_requests(), 8);
    assert_eq!(tot.count(InternalStatus::Completed), 4);
    assert_eq!(tot.count(InternalStatus::ClientCanceled), 1);
    assert_eq!(tot.count(InternalStatus::RouteNotFound), 1);
    assert_eq!(tot.count(InternalStatus::UpstreamError), 1);
    assert_eq!(tot.count(InternalStatus::InternalError), 1);
}

#[test]
fn test_aggregate_pipelines() {
    let store = MetricsStore::new();

    store.record_stream_completion(
        PipeKind::BufferedH1H1,
        StreamCompletion {
            internal_status: InternalStatus::Completed,
            bytes_rx: 200,
            bytes_tx: 800,
            duration_ns: 5_000_000,
            ttfb_ns: 2_000_000,
        },
    );

    let snap = store.snapshot();
    let res = snap.aggregate_pipelines();

    assert_eq!(res.total_requests(), 1);
    assert_eq!(res.count(InternalStatus::Completed), 1);
    assert_eq!(res.bytes_rx, 200);
    assert_eq!(res.bytes_tx, 800);
}

#[test]
fn test_add_counters_arbitrary_slices() {
    let mut small_dst = [5u64; 3];
    let small_src = [2u64; 3];
    add_counters(&mut small_dst, &small_src);
    assert_eq!(small_dst, [7, 7, 7]);

    let mut single_dst = [42u64; 1];
    let single_src = [8u64; 1];
    add_counters(&mut single_dst, &single_src);
    assert_eq!(single_dst[0], 50);

    let mut empty_dst: [u64; 0] = [];
    let empty_src: [u64; 0] = [];
    add_counters(&mut empty_dst, &empty_src);

    let mut full_dst = [10u64; PIPELINE_COUNTERS_LEN];
    let full_src = [5u64; PIPELINE_COUNTERS_LEN];
    add_counters(&mut full_dst, &full_src);
    assert_eq!(full_dst, [15u64; PIPELINE_COUNTERS_LEN]);
}
