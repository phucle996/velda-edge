//! Comprehensive In-Memory Access Log Tests for `velda-observability`.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use velda_observability::*;

#[test]
fn test_record_size_and_ipv4_ipv6_encoding() {
    assert_eq!(std::mem::size_of::<AccessLogRecord>(), 64);
    assert_eq!(std::mem::align_of::<AccessLogRecord>(), 64);

    let ipv4: IpAddr = "192.168.1.42".parse().unwrap();
    let bits_v4 = encode_ip_u128(ipv4);
    assert_eq!(decode_ip_u128(bits_v4), ipv4);

    let ipv6: IpAddr = "2001:db8:85a3::8a2e:370:7334".parse().unwrap();
    let bits_v6 = encode_ip_u128(ipv6);
    assert_eq!(decode_ip_u128(bits_v6), ipv6);
}

#[test]
fn test_user_agent_classification() {
    let (fam, os, ver) = classify_user_agent(
        b"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/134.0.0.0 Safari/537.36",
    );
    assert_eq!(fam, UaFamily::Chrome);
    assert_eq!(os, UaOs::Linux);
    assert_eq!(ver, 134);

    let (fam, os, ver) = classify_user_agent(b"curl/8.7.1");
    assert_eq!(fam, UaFamily::Curl);
    assert_eq!(os, UaOs::BotOrCli);
    assert_eq!(ver, 8);

    let (fam, os, ver) = classify_user_agent(b"grpc-go/1.64.0");
    assert_eq!(fam, UaFamily::GrpcGo);
    assert_eq!(os, UaOs::BotOrCli);
    assert_eq!(ver, 1);

    let (fam, _, _) = classify_user_agent(b"MyProprietaryClient/4.2");
    assert_eq!(fam, UaFamily::Custom);
}

#[test]
fn test_ref_counted_multi_exporter_fanout_and_auto_drop() {
    let store = Arc::new(LogStore::with_cores_and_capacity(2, 128));

    let sub_stdout = store.subscribe(32);
    let sub_loki = store.subscribe(32);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 2);

    let rec = AccessLogRecord::from_scalars(
        0x1234_5678_9abc_def0,
        1_700_000_000,
        450,
        "10.0.0.12:54321".parse().unwrap(),
        Some("10.0.1.99:8080".parse().unwrap()),
        256,
        1024,
        200,
        7,
        0,
        LogProtocol::H2,
        LogProtocol::H1,
        PipeStrategy::Buffered,
        LogCompletionReason::Complete,
        LogHttpMethod::Get,
        LogGrpcStatus::NonGrpc,
        b"curl/8.5.0",
    );

    assert!(store.emit_on_core(0, &rec));
    assert_eq!(store.flush_lane(0, 64), 1);

    let batch1 = sub_stdout.try_recv_batch().expect("stdout receives batch");
    let batch2 = sub_loki.try_recv_batch().expect("loki receives batch");

    assert_eq!(Arc::strong_count(&batch1), 2);
    assert_eq!(batch1[0].request_id, 0x1234_5678_9abc_def0);
    assert_eq!(batch2[0].status_code, 200);
    assert_eq!(UaFamily::from_u8(batch2[0].ua_family), UaFamily::Curl);

    drop(batch1);
    assert_eq!(Arc::strong_count(&batch2), 1);

    drop(batch2);

    drop(sub_stdout);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 1);
    drop(sub_loki);
    assert_eq!(store.active_subscriber_count.load(Ordering::Acquire), 0);
}
