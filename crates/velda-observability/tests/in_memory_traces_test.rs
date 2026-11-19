use std::sync::Arc;
use std::sync::atomic::Ordering;
use velda_observability::traces::{SpanRecord, TraceStore};

#[test]
fn emit_and_drain() {
    let store = Arc::new(TraceStore::with_cores_and_capacity(2, 64));
    let sub1 = store.subscribe(16);
    let sub2 = store.subscribe(16);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 2);

    let mut span = SpanRecord::empty();
    span.start_time_unix_nano = 1_000_000;
    span.end_time_unix_nano = 2_000_000;
    span.status_code = 200;
    span.route_id = 1;
    span.trace_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    span.span_id = [10, 20, 30, 40, 50, 60, 70, 80];

    assert!(store.emit_on_core(0, &span));
    assert_eq!(store.flush_lane(0, 64), 1);

    let batch1 = sub1.drain_batches(1).pop().expect("sub1 received batch");
    let batch2 = sub2.drain_batches(1).pop().expect("sub2 received batch");

    assert_eq!(batch1.len(), 1);
    assert_eq!(batch1[0].status_code, 200);
    assert_eq!(batch1[0].route_id, 1);
    assert!(Arc::ptr_eq(&batch1, &batch2));
    assert_eq!(Arc::strong_count(&batch1), 2);

    drop(batch1);
    assert_eq!(Arc::strong_count(&batch2), 1);
    drop(batch2);

    drop(sub1);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 1);
    drop(sub2);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
}

#[test]
fn flush_zero_subscribers() {
    let store = Arc::new(TraceStore::with_cores_and_capacity(1, 64));
    let mut span = SpanRecord::empty();
    span.status_code = 500;
    span.route_id = 2;
    store.emit_on_core(0, &span);

    assert_eq!(store.flush_lane(0, 64), 1);
}
