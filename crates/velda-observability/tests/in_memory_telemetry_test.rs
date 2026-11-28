//! Comprehensive In-Memory Telemetry Tests for `velda-observability`.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use velda_core::{HttpMethod, InternalStatus, PipeKind, UaFamily};
use velda_observability::*;

#[test]
fn test_multi_exporter_fanout() {
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(2, 128));

    let sub_stdout = store.subscribe(32);
    let sub_loki = store.subscribe(32);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 2);

    let mut rec = TelemetryRecord::zeroed();
    rec.request_id = 0x1234_5678_9abc_def0;
    rec.ingress_time_nanos = 1_700_000_000_000_000_000;
    rec.duration_ns = 450;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128("10.0.0.12".parse().unwrap());
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.1.99".parse().unwrap());
    rec.upstream_port = 8080;
    rec.bytes_rx = 256;
    rec.bytes_tx = 1024;
    rec.route_id = 7;
    rec.pipe = PipeKind::BufferedH2H1;
    rec.internal_status = InternalStatus::Completed;
    rec.method = HttpMethod::Get;
    let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.5.0");
    rec.ua_family = fam;
    rec.ua_os = os;
    rec.ua_major_version = ver;

    assert!(store.emit_on_core(0, &rec));
    assert_eq!(store.flush_lane(0, 64), 1);

    let batch1 = sub_stdout.try_recv_batch().expect("stdout receives batch");
    let batch2 = sub_loki.try_recv_batch().expect("loki receives batch");

    assert_eq!(batch1.lease_count(), 2);
    assert_eq!(batch1[0].request_id, 0x1234_5678_9abc_def0);
    assert_eq!(batch2[0].internal_status, InternalStatus::Completed);
    assert_eq!(batch2[0].ua_family, UaFamily::Curl);

    drop(batch1);
    assert_eq!(batch2.lease_count(), 1);

    drop(batch2);
    // Verifies that slot was returned to pool without deallocations (2 cores -> 2 SubPools = 128)
    let total_slots = store.batch_pool.total_slots();
    assert_eq!(total_slots, 2 * SUBPOOL_BATCHES);
    assert_eq!(store.batch_pool.available_count(), total_slots);

    drop(sub_stdout);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 1);
    drop(sub_loki);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
}

#[test]
fn test_slow_subscriber_eviction_preserves_rotating_pool_slots() {
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 256));
    let sub_fast = store.subscribe(256);
    let sub_slow = store.subscribe(256);

    let total_slots = store.batch_pool.total_slots();
    let rec = TelemetryRecord::zeroed();
    let total_flushes = total_slots + 12;

    for i in 0..total_flushes {
        let mut r = rec;
        r.request_id = i as u64;
        assert!(store.emit_on_core(0, &r));
        assert_eq!(store.flush_lane(0, 64), 1);

        // Fast exporter drains & releases its lease on every tick; slow exporter stays idle.
        let batch = sub_fast.try_recv_batch().expect("fast receives batch");
        assert!(matches!(batch, TelemetryBatch::Pooled(_)));
        drop(batch);
    }

    // Slow exporter capped at pool/2 queued batches, so >= half of all slots stayed free.
    assert!(sub_slow.dropped_batches() > 0);
    assert!(store.batch_pool.available_count() >= total_slots / 2);

    drop(sub_slow);
    drop(sub_fast);
    assert_eq!(store.batch_pool.available_count(), total_slots);
}

#[test]
fn test_subpool_strict_core_isolation_and_batch_summary() {
    // 2 cores map strictly 1:1 to 2 SubPools (2 * 64 = 128 batches) with zero cross-core stealing.
    let store = Arc::new(TelemetryStore::with_hardware(
        2,
        256,
        4 * 1024 * 1024 * 1024,
    ));
    assert_eq!(store.batch_pool.subpool_count(), 2);
    assert_eq!(store.batch_pool.total_slots(), 128);

    let sub = store.subscribe(256);
    let mut held_batches = Vec::with_capacity(SUBPOOL_BATCHES + 4);

    // Exhaust all 64 batches of SubPool[0]; Core 0 falls back to heap Arc without stealing SubPool[1].
    for i in 0..(SUBPOOL_BATCHES + 4) {
        let mut r = TelemetryRecord::zeroed();
        r.request_id = i as u64;
        r.ingress_time_nanos = 1_000_000 + i as u64;
        r.bytes_rx = 128;
        r.bytes_tx = 256;
        r.set_url("/api/test");
        if i % 2 == 1 {
            r.internal_status = InternalStatus::UpstreamError;
        }
        assert!(store.emit_on_core(0, &r));
        assert_eq!(store.flush_lane(0, 64), 1);

        let batch = sub.try_recv_batch().expect("subscriber receives batch");
        if i < SUBPOOL_BATCHES {
            assert!(matches!(batch, TelemetryBatch::Pooled(_)));
        } else {
            assert!(matches!(batch, TelemetryBatch::Arc(_, _)));
        }
        let summary = batch.summary();
        assert_eq!(summary.first_ts_ns, 1_000_000 + i as u64);
        assert_eq!(summary.last_ts_ns, 1_000_000 + i as u64);
        assert_eq!(summary.payload_bytes, 384);
        assert_eq!(summary.url_bytes_sum, 9);
        assert_eq!(summary.error_count, u32::from(i % 2 == 1));
        held_batches.push(batch);
    }

    assert_eq!(store.batch_pool.subpool_bitmap(0), Some(u64::MAX));
    // SubPool[1] remains untouched (100% free) for Core 1 despite Core 0 saturation.
    assert_eq!(store.batch_pool.subpool_bitmap(1), Some(0));
    assert_eq!(store.batch_pool.available_count(), SUBPOOL_BATCHES);

    drop(held_batches);
    assert_eq!(store.batch_pool.subpool_bitmap(0), Some(0));
    assert_eq!(store.batch_pool.subpool_bitmap(1), Some(0));
    assert_eq!(store.batch_pool.available_count(), 128);
}

#[test]
fn test_hardware_geometry_scaling_and_ram_cap() {
    // 1 core keeps 1 SubPool (64 slots) with 1024-record batch capacity (16 MiB floor)
    assert_eq!(
        compute_telemetry_pool_geometry(1, 8 * 1024 * 1024 * 1024),
        (1, SINGLE_CORE_FLOOR_BATCH_CAPACITY)
    );
    // 8 cores with ample RAM allocates 8 subpools (512 batches × 512)
    assert_eq!(
        compute_telemetry_pool_geometry(8, 16 * 1024 * 1024 * 1024),
        (8, DEFAULT_BATCH_CAPACITY)
    );
    // Strict 256 MB cgroup RAM clamps batch capacity & subpools safely
    let (subs, cap) = compute_telemetry_pool_geometry(16, 256 * 1024 * 1024);
    assert!(subs >= MIN_SUBPOOLS);
    assert_eq!(cap, MIN_BATCH_CAPACITY);
}

#[test]
fn test_flush_lane_budget_and_threshold_waker() {
    let store = Arc::new(TelemetryStore::with_cores_and_capacity(4, 2048));
    let sub = store.subscribe(128);
    assert_eq!(sub.trigger_threshold(), DEFAULT_BATCH_TRIGGER_THRESHOLD);

    // Emit 1,100 records into core 2 and drain via 32-batch budget (512 + 512 + 76).
    for i in 0..1100u64 {
        let mut r = TelemetryRecord::zeroed();
        r.request_id = i;
        assert!(store.emit_on_core(2, &r));
    }

    let drained = store.flush_lane_budget(2, 512, 32);
    assert_eq!(drained, 1100);
    assert_eq!(sub.queued_batches(), 3);

    let batches = sub.drain_batches(64);
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].len(), 512);
    assert_eq!(batches[1].len(), 512);
    assert_eq!(batches[2].len(), 76);
}

#[test]
fn test_batch_threshold_future_wakes_at_32_batches() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll, Wake, Waker};

    struct FlagWake(AtomicBool);
    impl Wake for FlagWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    let store = Arc::new(TelemetryStore::with_cores_and_capacity(2, 256));
    let sub = store.subscribe(128);
    let wake_flag = Arc::new(FlagWake(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&wake_flag));
    let mut cx = Context::from_waker(&waker);

    let mut fut = sub.wait_for_batch_threshold();
    assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

    let rec = TelemetryRecord::zeroed();
    for i in 0..(DEFAULT_BATCH_TRIGGER_THRESHOLD - 1) {
        let mut r = rec;
        r.request_id = i as u64;
        assert!(store.emit_on_core(0, &r));
        assert_eq!(store.flush_lane(0, 64), 1);
    }
    assert!(!wake_flag.0.load(Ordering::Acquire));

    // Pushing the 32nd batch triggers immediate waker notification.
    assert!(store.emit_on_core(0, &rec));
    assert_eq!(store.flush_lane(0, 64), 1);
    assert!(wake_flag.0.load(Ordering::Acquire));
    assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Ready(())));
}
