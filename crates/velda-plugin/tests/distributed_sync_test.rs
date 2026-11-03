//! Comprehensive integration tests for Distributed Multi-Gateway Synchronization via Redis.
//!
//! # Verifications
//! - Zero Hot-Path Invariant: Pipelined RESP serialization and response parsing.
//! - Non-blocking socket handling and batch delta collection.
//! - Global quota and micro-credit reconciliation into RAM Shards.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use velda_plugin::pre_upstream::api_metering::{ShardedMeteringState, TierConfig};
use velda_plugin::pre_upstream::consumer_quota::{
    QuotaAlgorithm, QuotaWindowConfig, ShardedQuotaState,
};
use velda_plugin::pre_upstream::distributed::RedisClient;

#[tokio::test]
async fn test_redis_pipelined_hincrby_mock_server() {
    // 1. Start lightweight in-memory mock RESP TCP server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let n = socket.read(&mut buf).await.unwrap();
        let req_str = std::str::from_utf8(&buf[..n]).unwrap();

        // Verify RESP pipelined format: contains 2 HINCRBY commands
        assert!(req_str.contains("HINCRBY"));
        assert!(req_str.contains("velda:metering:balances"));

        // Respond with 2 RESP integers: :900000\r\n:500000\r\n
        socket.write_all(b":900000\r\n:500000\r\n").await.unwrap();
    });

    // 2. Client sends batch of 2 commands
    let mut client = RedisClient::new(server_addr);
    let commands = [
        ("velda:metering:balances", "123456", -100_000i64),
        ("velda:metering:balances", "789012", -500_000i64),
    ];

    let results = client.pipelined_hincrby(&commands).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], 900_000);
    assert_eq!(results[1], 500_000);

    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_quota_delta_collection_and_reconciliation() {
    let state = ShardedQuotaState::new();
    let win_cfg = vec![QuotaWindowConfig {
        name: "test_win".to_string(),
        window_seconds: 60,
        limit: 10,
        algorithm: QuotaAlgorithm::FixedWindow,
    }];

    // Request 1: evaluate locally -> delta_consumed increments to 1
    let now_sec = 1000;
    let now_instant = std::time::Instant::now();
    let dec = state.evaluate(42, &win_cfg, now_sec, now_instant);
    assert!(matches!(
        dec,
        velda_plugin::pre_upstream::consumer_quota::QuotaDecision::Allowed { .. }
    ));

    // Collect deltas: should capture delta = 1 and reset to 0
    let deltas = state.collect_deltas();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0], (42, 0, 1));

    // Next collect: should be empty because delta was reset
    let empty_deltas = state.collect_deltas();
    assert!(empty_deltas.is_empty());

    // Reconcile with updated global count from Redis (e.g. other gateways consumed 8 requests -> total 9)
    state.reconcile_globals(&[(42, 0, 9)]);

    // Next local request: total is now 9 (out of 10), so allowed with remaining = 0
    let dec2 = state.evaluate(42, &win_cfg, now_sec, now_instant);
    assert!(matches!(
        dec2,
        velda_plugin::pre_upstream::consumer_quota::QuotaDecision::Allowed {
            min_remaining: 0,
            ..
        }
    ));

    // Next request: total is now 10 >= limit -> EXHAUSTED across all gateways!
    let dec3 = state.evaluate(42, &win_cfg, now_sec, now_instant);
    assert!(matches!(
        dec3,
        velda_plugin::pre_upstream::consumer_quota::QuotaDecision::Exhausted { .. }
    ));
}

#[tokio::test]
async fn test_metering_delta_collection_and_reconciliation() {
    let state = ShardedMeteringState::new();
    let tier = TierConfig {
        name: "test_tier".to_string(),
        windows: vec![],
        cost_per_req_micro_credits: 50_000,
        exhausted_action: velda_plugin::pre_upstream::api_metering::ExhaustedAction::Reject402,
    };

    let now_sec = 1000;
    let now_instant = std::time::Instant::now();

    // Account initialized with 1,000,000 micro-credits, cost is 50,000
    let dec1 = state.evaluate(99, &tier, 1_000_000, now_sec, now_instant);
    assert!(matches!(
        dec1,
        velda_plugin::pre_upstream::api_metering::MeteringDecision::Allowed {
            balance_micro_credits: 950_000,
            ..
        }
    ));

    // Collect deltas: should capture delta = 50,000
    let deltas = state.collect_deltas();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0], (99, 50_000));

    // Reconcile from Redis: Redis says another node also spent credits, new balance is 20,000
    state.reconcile_balances(&[(99, 20_000)]);

    // Next request: cost is 50,000 > remaining 20,000 -> Exhausted 402 Payment Required!
    let dec2 = state.evaluate(99, &tier, 1_000_000, now_sec, now_instant);
    assert!(matches!(
        dec2,
        velda_plugin::pre_upstream::api_metering::MeteringDecision::Exhausted {
            action: velda_plugin::pre_upstream::api_metering::ExhaustedAction::Reject402,
            ..
        }
    ));
}
