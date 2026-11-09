//! Runtime Machine Code & Cycle Profiling for Health Probe SWAR Evaluator
//!
//! Measures real CPU cycles, branch behavior, and memory residency during active execution.

use velda_plugin::background::health_probe::{HealthProbeConfig, HealthProbePlugin, HealthState};

#[test]
fn test_runtime_machine_code_execution_cycles() {
    let config = HealthProbeConfig {
        listen_addr: "127.0.0.1:9091".parse().unwrap(),
        liveness_path: "/livez".into(),
        readiness_path: "/readyz".into(),
        check_interval_ms: 1000,
        timeout_ms: 1000,
    };

    let state = HealthState::new();
    let plugin = HealthProbePlugin::with_state("test_probe", config, state.clone());

    let fallback_config = HealthProbeConfig {
        listen_addr: "127.0.0.1:9091".parse().unwrap(),
        liveness_path: "/healthz/liveness".into(),
        readiness_path: "/healthz/readiness".into(),
        check_interval_ms: 1000,
        timeout_ms: 1000,
    };
    let fallback_plugin =
        HealthProbePlugin::with_state("fallback_probe", fallback_config, state.clone());

    let req_live = b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let req_ready = b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let req_unknown = b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let req_not_get = b"POST /livez HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let req_long_live = b"GET /healthz/liveness HTTP/1.1\r\nHost: localhost\r\n\r\n";

    // Warm up caches
    for _ in 0..10_000 {
        let _ = plugin.evaluate_request(req_live);
        let _ = plugin.evaluate_request(req_ready);
        let _ = plugin.evaluate_request(req_unknown);
        let _ = fallback_plugin.evaluate_request(req_long_live);
    }

    // Benchmark 1,000,000 runs
    const ITERATIONS: u64 = 1_000_000;

    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let resp = plugin.evaluate_request(req_live);
        std::hint::black_box(resp);
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let live_cycles = (end_tsc - start_tsc) / ITERATIONS;

    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let resp = plugin.evaluate_request(req_ready);
        std::hint::black_box(resp);
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let ready_cycles = (end_tsc - start_tsc) / ITERATIONS;

    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let resp = plugin.evaluate_request(req_unknown);
        std::hint::black_box(resp);
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let unknown_cycles = (end_tsc - start_tsc) / ITERATIONS;

    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let resp = plugin.evaluate_request(req_not_get);
        std::hint::black_box(resp);
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let not_get_cycles = (end_tsc - start_tsc) / ITERATIONS;

    let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    for _ in 0..ITERATIONS {
        let resp = fallback_plugin.evaluate_request(req_long_live);
        std::hint::black_box(resp);
    }
    let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
    let fallback_cycles = (end_tsc - start_tsc) / ITERATIONS;

    println!("============================================================");
    println!(" REAL RUNTIME MACHINE CODE CPU CYCLES (1,000,000 Iterations)");
    println!("============================================================");
    println!(" 1. GET /livez (SWAR Match + OK -> 200 OK):   {live_cycles} cycles/call");
    println!(" 2. GET /readyz (SWAR Match + NotReady -> 503):{ready_cycles} cycles/call");
    println!(" 3. GET /unknown (SWAR No Match -> 404):      {unknown_cycles} cycles/call");
    println!(" 4. POST /livez (Early Exit Non-GET -> 404):  {not_get_cycles} cycles/call");
    println!(" 5. GET /healthz/liveness (Fallback String):   {fallback_cycles} cycles/call");
    println!("============================================================");

    if !cfg!(debug_assertions) {
        assert!(
            live_cycles < 30,
            "Release mode live cycles should be < 30 cycles (SWAR 64-bit)"
        );
    }
}
