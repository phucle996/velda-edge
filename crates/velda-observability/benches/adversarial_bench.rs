//! Adversarial, Fault-Tolerance & Stress Benchmark Suite for `velda-observability`.
//!
//! Evaluates resilience under hostile, erratic, and extreme traffic patterns:
//! 1. HTTP/2 Rapid Reset Flood Attack (5,000,000 RST_STREAM frames)
//! 2. Extreme Status Code & Pipeline Hopping Fuzzing (5,000,000 unpredictable ops)
//! 3. Mass Eviction Storm & Extreme Lifespan Skew (2,000,000 evictions)
//! 4. Rapid Flapping & Connection Oscillation Storm (2,000,000 chaotic cycles)

mod common;

use std::time::{Duration, Instant};

use common::{FastRng, format_throughput};
use velda_observability::MetricsStore;
use velda_observability::pool::EvictionReason;
use velda_observability::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};

// ============================================================================
// Stage 1: HTTP/2 Rapid Reset Flood Attack Simulation
// ============================================================================

fn bench_rapid_reset_flood() {
    println!("### 1. HTTP/2 Rapid Reset Flood Attack Simulation (5,000,000 ops)\n");
    println!(
        "> Simulating CVE-2023-44487: rapid stream starts immediately followed by RST_STREAM...\n"
    );
    println!(
        "| Target Attack | Operations | Elapsed Time | Latency / op | Throughput | Invariant Target | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 5_000_000u64;
    let store = MetricsStore::new();

    let start = Instant::now();

    for _ in 0..iters {
        // Stream begins
        store.record_stream_start(
            DownstreamProto::H2,
            UpstreamProto::H1,
            PipeStrategy::ServerStream,
        );

        // Immediate cancellation / RST_STREAM received
        store.global().h2_rst_stream();

        // Stream completed with 499 (Client Closed Request) and 0 bytes transferred
        store.record_stream_completion(
            DownstreamProto::H2,
            UpstreamProto::H1,
            PipeStrategy::ServerStream,
            StreamCompletion::new(499, 0, 0, Duration::from_micros(50), None),
        );
    }

    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let snap = store.snapshot();
    let pipe = snap
        .find_pipeline(
            DownstreamProto::H2,
            UpstreamProto::H1,
            PipeStrategy::ServerStream,
        )
        .unwrap();

    assert_eq!(snap.global.h2_rst_stream_total, iters);
    assert_eq!(pipe.active_streams, 0);
    assert_eq!(pipe.requests_4xx, iters);
    assert_eq!(pipe.bytes_rx, 0);
    assert_eq!(pipe.bytes_tx, 0);

    println!(
        "| **H2 Rapid Reset Storm** | {} ops | {:.3} s | **{:.2} ns** | **{}** | Zero active leak, exact counters | **PASS** |",
        iters,
        elapsed.as_secs_f64(),
        ns_op,
        format_throughput(iters, elapsed)
    );
    println!();
}

// ============================================================================
// Stage 2: Extreme Status Code & Pipeline Hopping Fuzzing
// ============================================================================

fn bench_erratic_status_and_pipeline_hopping() {
    println!("### 2. Extreme Status Code & Pipeline Hopping Fuzzing (5,000,000 ops)\n");
    println!(
        "> Hostile PRNG fuzzing status codes (100..599) and jumping across all 36 pipelines...\n"
    );
    println!(
        "| Fuzzing Profile | Operations | Elapsed Time | Latency / op | Throughput | Invariant Target | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 5_000_000u64;
    let store = MetricsStore::new();
    let mut rng = FastRng::new(0x1337_c001_cafe_d00d);

    let start = Instant::now();

    for _ in 0..iters {
        let (down, up, pipe) = rng.next_pipeline_coords();
        let status = (rng.next_usize(500) + 100) as u16; // Random 100..599
        let bytes_rx = rng.next_usize(8192) as u64;
        let bytes_tx = rng.next_usize(131072) as u64;
        let dur = Duration::from_millis((rng.next_usize(2000) + 1) as u64);
        let ttfb = if rng.next_usize(2) == 0 {
            Some(Duration::from_millis((rng.next_usize(500) + 1) as u64))
        } else {
            None
        };

        store.record_stream_start(down, up, pipe);
        store.record_stream_completion(
            down,
            up,
            pipe,
            StreamCompletion::new(status, bytes_rx, bytes_tx, dur, ttfb),
        );
    }

    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let snap = store.snapshot();

    assert_eq!(snap.total_requests(), iters);
    assert_eq!(snap.total_active_streams(), 0);

    println!(
        "| **Erratic Multi-Pipe Fuzz** | {} ops | {:.3} s | **{:.2} ns** | **{}** | Zero drift across 36 slots | **PASS** |",
        iters,
        elapsed.as_secs_f64(),
        ns_op,
        format_throughput(iters, elapsed)
    );
    println!();
}

// ============================================================================
// Stage 3: Mass Eviction Storm & Extreme Lifespan Skew
// ============================================================================

fn bench_mass_eviction_storm() {
    println!("### 3. Mass Eviction Storm & Extreme Lifespan Skew (2,000,000 evictions)\n");
    println!(
        "> Simulating upstream collapse: mass evictions across all 5 reasons with skewed lifespans...\n"
    );
    println!(
        "| Workload Profile | Evictions | Elapsed Time | Latency / op | Throughput | Invariant Target | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 2_000_000u64;
    let store = MetricsStore::new();
    let pool = store.pool();
    let mut rng = FastRng::new(0xbeef_f00d_dead_beef);

    let start = Instant::now();

    for _ in 0..iters {
        let reason = match rng.next_usize(5) {
            0 => EvictionReason::IdleTimeout,
            1 => EvictionReason::MaxLifetime,
            2 => EvictionReason::Goaway,
            3 => EvictionReason::Unhealthy,
            _ => EvictionReason::CapacityExceeded,
        };

        // Skewed lifespan from 1 second to 10 hours
        let lifespan_secs = match rng.next_usize(100) {
            0..=40 => rng.next_usize(5) + 1,        // 40% <= 5s
            41..=70 => rng.next_usize(25) + 6,      // 30% <= 30s
            71..=85 => rng.next_usize(270) + 31,    // 15% <= 5m
            86..=94 => rng.next_usize(1500) + 301,  // 9% <= 30m
            95..=98 => rng.next_usize(1800) + 1801, // 4% <= 1h
            _ => rng.next_usize(36000) + 3601,      // 2% > 1h
        };

        pool.record_eviction(reason, Duration::from_secs(lifespan_secs as u64));
    }

    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let snap = store.snapshot().pool;

    assert_eq!(snap.total_evictions(), iters);
    assert_eq!(snap.total_lifespan_recorded(), iters);

    println!(
        "| **Mass Eviction Burst** | {} evictions | {:.3} s | **{:.2} ns** | **{}** | Cumulative buckets match exact total | **PASS** |",
        iters,
        elapsed.as_secs_f64(),
        ns_op,
        format_throughput(iters, elapsed)
    );
    println!();
}

// ============================================================================
// Stage 4: Rapid Flapping & Connection Oscillation Storm
// ============================================================================

fn bench_rapid_flapping_oscillation() {
    println!("### 4. Rapid Flapping & Connection Oscillation Storm (2,000,000 cycles)\n");
    println!("> Simulating erratic upstream flapping: hit, timeout, miss, drop, reconnect...\n");
    println!(
        "| Flapping Mode | Cycles | Elapsed Time | Latency / op | Throughput | Invariant Target | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 2_000_000u64;
    let store = MetricsStore::new();
    let pool = store.pool();
    let mut rng = FastRng::new(0x9876_5432_10fe_dcba);

    let start = Instant::now();

    for _ in 0..iters {
        match rng.next_usize(4) {
            0 => {
                // Normal hit + return
                pool.record_hit();
                pool.record_release();
            }
            1 => {
                // Miss + new connection + return
                pool.record_miss();
                pool.record_connection_created();
                pool.record_release();
            }
            2 => {
                // Timeout waiting for queue
                pool.record_timeout();
            }
            _ => {
                // Miss + socket crash during request -> dropped + eviction
                pool.record_miss();
                pool.record_connection_created();
                pool.record_lease_dropped();
                pool.record_eviction(EvictionReason::Unhealthy, Duration::from_secs(1));
            }
        }
    }

    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let snap = store.snapshot().pool;

    // Verify leased gauge is strictly zero after all balanced releases/drops
    assert_eq!(snap.connections_leased, 0);

    println!(
        "| **Erratic Pool Flapping** | {} cycles | {:.3} s | **{:.2} ns** | **{}** | Leased gauge perfectly 0 | **PASS** |",
        iters,
        elapsed.as_secs_f64(),
        ns_op,
        format_throughput(iters, elapsed)
    );
    println!();
}

fn main() {
    println!("================================================================================");
    println!("Velda Observability — Adversarial & Fault-Tolerance Benchmark Suite");
    println!("================================================================================\n");

    bench_rapid_reset_flood();
    bench_erratic_status_and_pipeline_hopping();
    bench_mass_eviction_storm();
    bench_rapid_flapping_oscillation();

    println!("Adversarial benchmark suite completed successfully.");
}
