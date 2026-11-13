//! Comprehensive Chaos & Invariant Verification Suite for StatsD / DogStatsD Exporter.
//!
//! Validates:
//! 1. Full coverage of all 36 pipeline slots in both DogStatsD and Standard dialects.
//! 2. Strict MTU boundary invariant (512, 1432, 8192 bytes): 0 bytes lost, 0 split lines.
//! 3. Extreme edge values (0, 1, u64::MAX, negative numbers for active gauges).
//! 4. Stress stability across 10,000 snapshot cycles with zero memory leaks.

use std::time::Duration;

use velda_observability::{
    DownstreamProtocol, MetricsSnapshot, MetricsStore, PipeStrategy, StreamCompletion,
    UpstreamProtocol,
};
use velda_plugin::background::statsd_exporter::dialect::{
    DogStatsdTemplate, StandardTemplate, StatsdDialect, format_statsd_snapshot,
};

/// Helper to populate all 36 pipeline slots in a MetricsStore.
fn populate_all_36_pipeline_slots(store: &MetricsStore) {
    for down_idx in 0..DownstreamProtocol::COUNT {
        let down = DownstreamProtocol::from_discriminant(down_idx as u8).unwrap();
        for up_idx in 0..UpstreamProtocol::COUNT {
            let up = UpstreamProtocol::from_discriminant(up_idx as u8).unwrap();
            for pipe_idx in 0..PipeStrategy::COUNT {
                let pipe = PipeStrategy::from_discriminant(pipe_idx as u8).unwrap();

                // Start 5 streams for 5 completions + 1 active stream remaining
                for _ in 0..6 {
                    store.record_stream_start(down, up, pipe);
                }

                // Record completions across status categories and durations (each ends 1 stream)
                store.record_stream_completion(
                    down,
                    up,
                    pipe,
                    StreamCompletion::new(200, 100, 200, Duration::from_micros(500), None),
                );
                store.record_stream_completion(
                    down,
                    up,
                    pipe,
                    StreamCompletion::new(301, 150, 250, Duration::from_millis(3), None),
                );
                store.record_stream_completion(
                    down,
                    up,
                    pipe,
                    StreamCompletion::new(404, 200, 300, Duration::from_millis(8), None),
                );
                store.record_stream_completion(
                    down,
                    up,
                    pipe,
                    StreamCompletion::new(503, 250, 350, Duration::from_millis(25), None),
                );
                store.record_stream_completion(
                    down,
                    up,
                    pipe,
                    StreamCompletion::new(999, 300, 400, Duration::from_millis(150), None),
                );
            }
        }
    }
}

#[test]
fn test_chaos_all_36_pipeline_slots_dogstatsd() {
    let store = MetricsStore::new();
    populate_all_36_pipeline_slots(&store);
    let snapshot = store.snapshot();

    assert_eq!(snapshot.pipelines.len(), 36);

    let mut packet_buf = Vec::new();
    let mut collected = Vec::new();

    let (packets, metrics) = format_statsd_snapshot(
        &snapshot,
        "velda_chaos",
        StatsdDialect::DogStatsd,
        &mut packet_buf,
        1432,
        |pkt| {
            collected.extend_from_slice(pkt);
        },
    );

    assert!(packets > 0);
    // 8 global + 10 pool + 5 evictions + (36 slots * 15 lines) = 563 total lines
    assert_eq!(metrics, 563);

    let text = String::from_utf8(collected).expect("Output must be valid UTF-8");

    // Verify global metrics
    assert!(text.contains("velda_chaos.connections.total:0|c\n"));
    assert!(text.contains("velda_chaos.connections.active:0|g\n"));
    assert!(text.contains("velda_chaos.pool.containers.total:0|g\n"));
    assert!(text.contains("velda_chaos.pool.evictions.total:0|c|#reason:idle_timeout\n"));

    // Verify all 36 combinations have their specific tags
    for down_idx in 0..DownstreamProtocol::COUNT {
        let d = DownstreamProtocol::from_discriminant(down_idx as u8)
            .unwrap()
            .as_str();
        for up_idx in 0..UpstreamProtocol::COUNT {
            let u = UpstreamProtocol::from_discriminant(up_idx as u8)
                .unwrap()
                .as_str();
            for pipe_idx in 0..PipeStrategy::COUNT {
                let p = PipeStrategy::from_discriminant(pipe_idx as u8)
                    .unwrap()
                    .as_str();

                let expected_tag = format!("#downstream:{d},upstream:{u},pipe:{p}");
                assert!(
                    text.contains(&expected_tag),
                    "Missing expected DogStatsD tag '{expected_tag}'"
                );

                // Verify specific status lines
                let expected_2xx =
                    format!("velda_chaos.requests.total:1|c|{expected_tag},status:2xx\n");
                let expected_3xx =
                    format!("velda_chaos.requests.total:1|c|{expected_tag},status:3xx\n");
                let expected_4xx =
                    format!("velda_chaos.requests.total:1|c|{expected_tag},status:4xx\n");
                let expected_5xx =
                    format!("velda_chaos.requests.total:1|c|{expected_tag},status:5xx\n");
                let expected_other =
                    format!("velda_chaos.requests.total:1|c|{expected_tag},status:other\n");

                assert!(text.contains(&expected_2xx), "Missing: {expected_2xx}");
                assert!(text.contains(&expected_3xx), "Missing: {expected_3xx}");
                assert!(text.contains(&expected_4xx), "Missing: {expected_4xx}");
                assert!(text.contains(&expected_5xx), "Missing: {expected_5xx}");
                assert!(text.contains(&expected_other), "Missing: {expected_other}");

                // Active streams gauge
                let expected_streams = format!("velda_chaos.streams.active:1|g|{expected_tag}\n");
                assert!(
                    text.contains(&expected_streams),
                    "Missing: {expected_streams}"
                );
            }
        }
    }
}

#[test]
fn test_chaos_all_36_pipeline_slots_standard() {
    let store = MetricsStore::new();
    populate_all_36_pipeline_slots(&store);
    let snapshot = store.snapshot();

    assert_eq!(snapshot.pipelines.len(), 36);

    let mut packet_buf = Vec::new();
    let mut collected = Vec::new();

    let (packets, metrics) = format_statsd_snapshot(
        &snapshot,
        "velda_chaos",
        StatsdDialect::Standard,
        &mut packet_buf,
        1432,
        |pkt| {
            collected.extend_from_slice(pkt);
        },
    );

    assert!(packets > 0);
    assert_eq!(metrics, 563);

    let text = String::from_utf8(collected).expect("Output must be valid UTF-8");

    // Standard dialect MUST NEVER contain '#' tag delimiters
    assert!(
        !text.contains('#'),
        "Standard dialect must not contain DogStatsD tags ('#')"
    );

    // Verify global metrics
    assert!(text.contains("velda_chaos.connections.total:0|c\n"));
    assert!(text.contains("velda_chaos.pool.evictions.idle_timeout:0|c\n"));

    // Verify all 36 combinations have their specific dot notation paths
    for down_idx in 0..DownstreamProtocol::COUNT {
        let d = DownstreamProtocol::from_discriminant(down_idx as u8)
            .unwrap()
            .as_str();
        for up_idx in 0..UpstreamProtocol::COUNT {
            let u = UpstreamProtocol::from_discriminant(up_idx as u8)
                .unwrap()
                .as_str();
            for pipe_idx in 0..PipeStrategy::COUNT {
                let p = PipeStrategy::from_discriminant(pipe_idx as u8)
                    .unwrap()
                    .as_str();

                let expected_2xx = format!("velda_chaos.requests.{d}.{u}.{p}.2xx:1|c\n");
                let expected_5xx = format!("velda_chaos.requests.{d}.{u}.{p}.5xx:1|c\n");
                let expected_streams = format!("velda_chaos.streams.active.{d}.{u}.{p}:1|g\n");
                let expected_rx = format!("velda_chaos.bytes.rx.{d}.{u}.{p}:1000|c\n");

                assert!(text.contains(&expected_2xx), "Missing: {expected_2xx}");
                assert!(text.contains(&expected_5xx), "Missing: {expected_5xx}");
                assert!(
                    text.contains(&expected_streams),
                    "Missing: {expected_streams}"
                );
                assert!(text.contains(&expected_rx), "Missing: {expected_rx}");
            }
        }
    }
}

#[test]
fn test_chaos_mtu_boundaries_no_split_no_loss() {
    let store = MetricsStore::new();
    populate_all_36_pipeline_slots(&store);
    let snapshot = store.snapshot();

    let mtu_sizes = [256, 512, 1024, 1432, 2048, 8192];
    let dialects = [StatsdDialect::DogStatsd, StatsdDialect::Standard];

    for dialect in dialects {
        let mut baseline_lines: Option<Vec<String>> = None;

        for &mtu in &mtu_sizes {
            let mut packet_buf = Vec::new();
            let mut packets_received = 0;
            let mut all_lines: Vec<String> = Vec::new();

            let (emitted_packets, emitted_metrics) = format_statsd_snapshot(
                &snapshot,
                "mtu_test",
                dialect,
                &mut packet_buf,
                mtu,
                |pkt| {
                    packets_received += 1;
                    // Invariant 1: Packet length must never exceed MTU bound
                    assert!(
                        pkt.len() <= mtu,
                        "Packet length {} exceeded MTU {} for dialect {:?}",
                        pkt.len(),
                        mtu,
                        dialect
                    );
                    // Invariant 2: Packet must end strictly with newline
                    assert!(
                        pkt.ends_with(b"\n"),
                        "Packet does not end with complete line delimiter (\\n)"
                    );

                    // Parse packet lines
                    let s = std::str::from_utf8(pkt).expect("Packet must be valid UTF-8");
                    for line in s.split('\n') {
                        if !line.is_empty() {
                            // Invariant 3: Line must be fully formed
                            assert!(
                                line.starts_with("mtu_test."),
                                "Malformed line fragment: {line}"
                            );
                            assert!(
                                line.contains(':'),
                                "Line missing colon value separator: {line}"
                            );
                            assert!(
                                line.contains("|c") || line.contains("|g"),
                                "Line missing metric type indicator: {line}"
                            );
                            all_lines.push(line.to_string());
                        }
                    }
                },
            );

            assert_eq!(emitted_packets, packets_received);
            assert_eq!(emitted_metrics, all_lines.len());
            assert_eq!(emitted_metrics, 563);

            // Invariant 4: 0 byte / 0 line loss across all MTUs
            if let Some(ref base) = baseline_lines {
                assert_eq!(
                    &all_lines, base,
                    "Metric stream content diverged across MTU {mtu} for dialect {:?}",
                    dialect
                );
            } else {
                baseline_lines = Some(all_lines);
            }
        }
    }
}

#[test]
fn test_chaos_extreme_boundary_values() {
    let mut snapshot = MetricsSnapshot {
        captured_at: std::time::Instant::now(),
        pipelines: Vec::new(),
        global: velda_observability::GlobalSnapshot {
            connections_total: u64::MAX,
            connections_active: -100, // Negative gauge
            connections_rejected: 0,
            tls_handshakes_total: 1,
            tls_handshake_errors: u64::MAX,
            h2_rst_stream_total: 0,
            config_reloads_total: 42,
            config_reload_errors: 0,
        },
        pool: velda_observability::PoolSnapshot {
            pool_containers_total: 0,
            connections_idle: 0,
            connections_leased: -42, // Negative leased
            connections_multiplexed: 1,
            multiplexed_active_streams: -1, // Negative streams
            lease_hits_total: u64::MAX,
            lease_misses_total: 0,
            lease_timeouts_total: 0,
            lease_releases_total: 1,
            connections_created_total: u64::MAX,
            evictions_idle_timeout: 0,
            evictions_max_lifetime: u64::MAX,
            evictions_goaway: 0,
            evictions_unhealthy: 1,
            evictions_capacity_exceeded: 0,
            lifespan_le_5s: 0,
            lifespan_le_30s: 0,
            lifespan_le_5m: 0,
            lifespan_le_30m: 0,
            lifespan_le_1h: 0,
            lifespan_gt_1h: 0,
        },
    };

    // Add a pipeline slot with boundary values
    snapshot
        .pipelines
        .push(velda_observability::PipelineSnapshot {
            downstream: DownstreamProtocol::H1,
            upstream: UpstreamProtocol::H1,
            pipe: PipeStrategy::Buffered,
            requests_2xx: u64::MAX,
            requests_3xx: 0,
            requests_4xx: 1,
            requests_5xx: 999_999_999,
            requests_other: 0,
            active_streams: -999, // Negative gauge
            bytes_rx: u64::MAX,
            bytes_tx: 0,
            duration_le_1ms: 0,
            duration_le_5ms: 1,
            duration_le_10ms: u64::MAX,
            duration_le_50ms: 0,
            duration_le_250ms: 0,
            duration_le_1000ms: 0,
            duration_gt_1000ms: 0,
            ttfb_le_1ms: 0,
            ttfb_le_5ms: 0,
            ttfb_le_10ms: 0,
            ttfb_le_50ms: 0,
            ttfb_le_250ms: 0,
            ttfb_gt_250ms: 0,
        });

    let mut buf = Vec::new();
    let mut collected = Vec::new();

    // 1. DogStatsD check
    format_statsd_snapshot(
        &snapshot,
        "boundary",
        StatsdDialect::DogStatsd,
        &mut buf,
        4096,
        |pkt| collected.extend_from_slice(pkt),
    );
    let dog_str = String::from_utf8(collected).unwrap();

    assert!(dog_str.contains("boundary.connections.total:18446744073709551615|c\n"));
    assert!(dog_str.contains("boundary.connections.active:-100|g\n"));
    assert!(dog_str.contains("boundary.connections.rejected:0|c\n"));
    assert!(dog_str.contains("boundary.tls.handshakes.total:1|c\n"));
    assert!(dog_str.contains("boundary.pool.connections.leased:-42|g\n"));
    assert!(dog_str.contains("boundary.pool.multiplexed_active_streams:-1|g\n"));
    assert!(dog_str.contains("boundary.requests.total:18446744073709551615|c|#downstream:h1,upstream:h1,pipe:buffered,status:2xx\n"));
    assert!(
        dog_str
            .contains("boundary.streams.active:-999|g|#downstream:h1,upstream:h1,pipe:buffered\n")
    );

    // 2. Standard StatsD check
    let mut std_collected = Vec::new();
    buf.clear();
    format_statsd_snapshot(
        &snapshot,
        "boundary",
        StatsdDialect::Standard,
        &mut buf,
        4096,
        |pkt| std_collected.extend_from_slice(pkt),
    );
    let std_str = String::from_utf8(std_collected).unwrap();

    assert!(std_str.contains("boundary.connections.active:-100|g\n"));
    assert!(std_str.contains("boundary.requests.h1.h1.buffered.2xx:18446744073709551615|c\n"));
    assert!(std_str.contains("boundary.streams.active.h1.h1.buffered:-999|g\n"));
}

#[test]
fn test_chaos_stress_10000_records_zero_leak() {
    let store = MetricsStore::new();
    populate_all_36_pipeline_slots(&store);
    let snapshot = store.snapshot();

    let dogstatsd = DogStatsdTemplate::new("stress");
    let standard = StandardTemplate::new("stress");

    let mut packet_buf = Vec::with_capacity(2048);
    const ITERATIONS: usize = 10_000;

    for i in 0..ITERATIONS {
        let mut packet_count = 0;
        let mut metric_count = 0;

        if i % 2 == 0 {
            let (pkts, metrics) = dogstatsd.format(&snapshot, &mut packet_buf, 1432, |_pkt| {
                packet_count += 1;
            });
            assert_eq!(pkts, packet_count);
            assert_eq!(metrics, 563);
            metric_count += metrics;
        } else {
            let (pkts, metrics) = standard.format(&snapshot, &mut packet_buf, 1432, |_pkt| {
                packet_count += 1;
            });
            assert_eq!(pkts, packet_count);
            assert_eq!(metrics, 563);
            metric_count += metrics;
        }

        assert_eq!(metric_count, 563);
        // Ensure packet buffer capacity did not balloon
        assert!(
            packet_buf.capacity() <= 4096,
            "Memory leak detected: packet_buf capacity grew to {}",
            packet_buf.capacity()
        );
    }
}
