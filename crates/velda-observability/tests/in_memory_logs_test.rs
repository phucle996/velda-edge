//! Comprehensive In-Memory Access Log Tests for `velda-observability`.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use velda_core::{CompletionReason, HttpMethod, PipeKind, UaFamily};
use velda_observability::*;

#[test]
fn test_multi_exporter_fanout() {
    let store = Arc::new(LogStore::with_cores_and_capacity(2, 128));

    let sub_stdout = store.subscribe(32);
    let sub_loki = store.subscribe(32);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 2);

    let mut rec = AccessLogRecord::zeroed();
    rec.request_id = 0x1234_5678_9abc_def0;
    rec.ingress_time_nanos = 1_700_000_000_000_000_000;
    rec.duration_ns = 450;
    rec.peer_ip = velda_core::lut::ip::encode_ip_u128("10.0.0.12".parse().unwrap());
    rec.upstream_ip = velda_core::lut::ip::encode_upstream_ip_u32("10.0.1.99".parse().unwrap());
    rec.upstream_port = 8080;
    rec.bytes_rx = 256;
    rec.bytes_tx = 1024;
    rec.status_code = 200;
    rec.route_id = 7;
    rec.pipe = PipeKind::BufferedH2H1;
    rec.completion = CompletionReason::Complete;
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
    assert_eq!(batch2[0].status_code, 200);
    assert_eq!(batch2[0].ua_family, UaFamily::Curl);

    drop(batch1);
    assert_eq!(batch2.lease_count(), 1);

    drop(batch2);
    // Verifies that slot was returned to pool without deallocations
    assert_eq!(
        store.batch_pool.available_count(),
        velda_observability::DEFAULT_POOL_SLOTS
    );

    drop(sub_stdout);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 1);
    drop(sub_loki);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
}
