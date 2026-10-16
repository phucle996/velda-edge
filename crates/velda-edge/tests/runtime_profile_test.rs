use std::fs;

use velda_core::hardware::HardwareTopology;
use velda_edge::{RuntimeProfile, resolve_runtime_profile};

#[test]
fn test_profile_continuous_scaling() {
    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * MB;

    let test_cases = [
        (256 * MB, 1),
        (1024 * MB, 2),
        (4 * GB, 4),
        (16 * GB, 8),
        (48 * GB, 16),
        (96 * GB, 32),
        (256 * GB, 64),
    ];

    for (ram_bytes, cores) in test_cases {
        let hardware = HardwareTopology::with_workers_and_memory(cores, ram_bytes);
        let profile = RuntimeProfile::from_hardware(&hardware);

        assert_eq!(profile.transport.io_workers, cores);
        assert_eq!(
            profile.transport.max_active_connections,
            hardware.sizing.tcp_max_connections
        );
        assert_eq!(
            profile.discovery.max_dns_cache_capacity,
            hardware.sizing.dns_cache_capacity
        );
        assert!(profile.discovery.max_lkg_capacity <= profile.discovery.max_dns_cache_capacity);

        // TLS profile assertions
        assert_eq!(
            profile.tls.session_cache_capacity,
            hardware.sizing.tls_session_cache_capacity
        );
        assert_eq!(
            profile.tls.max_early_data_size,
            hardware.sizing.tls_max_early_data_size
        );

        let tls_params = profile.to_tls_server_params();
        assert_eq!(
            tls_params.session_cache_capacity,
            hardware.sizing.tls_session_cache_capacity
        );
        assert_eq!(
            tls_params.max_early_data_size,
            hardware.sizing.tls_max_early_data_size
        );

        let dns_cfg = profile.to_dns_resolver_config();
        assert_eq!(dns_cfg.cache_capacity, hardware.sizing.dns_cache_capacity);

        let tcp_cfg = profile.to_tcp_listener_config();
        assert!(tcp_cfg.backlog >= 1024);

        let udp_cfg = profile.to_udp_socket_config();
        assert!(udp_cfg.recv_buffer_size.is_some());

        assert_eq!(
            profile.overload,
            velda_core::OverloadConfig::from_memory_bytes(hardware.memory_bytes())
        );
    }
}

#[test]
fn test_operator_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    // Custom operator override with explicitly tuned values
    let custom_json = r#"{
        "version": 1,
        "discovery": {
            "max_dns_cache_capacity": 42000,
            "max_lkg_capacity": 7777,
            "max_negative_ttl_secs": 120,
            "query_timeout_ms": 1500,
            "negative_ttl_secs": 25,
            "positive_ttl_secs": 45
        }
    }"#;

    fs::write(&runtime_json_path, custom_json).unwrap();

    let hardware = HardwareTopology::with_workers_and_memory(2, 256 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.discovery.max_dns_cache_capacity, 42_000);
    assert_eq!(resolved.discovery.max_lkg_capacity, 7_777);
    assert_eq!(resolved.discovery.max_negative_ttl_secs, 120);
    assert_eq!(resolved.transport.io_workers, 2);
}

#[test]
fn test_fallback_writeback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    assert!(!runtime_json_path.exists());

    let hardware = HardwareTopology::with_workers_and_memory(2, 160 * 1024 * 1024 * 1024); // 160GB -> Ultra
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.transport.io_workers, 2);

    // Verify write-back occurred and omits hardware and io_workers
    assert!(runtime_json_path.exists());
    let content = fs::read_to_string(&runtime_json_path).unwrap();
    assert!(
        !content.contains("\"hardware\""),
        "runtime.json must never contain hardware"
    );
    assert!(
        !content.contains("\"io_workers\""),
        "runtime.json must never contain io_workers"
    );
    let parsed: RuntimeProfile = serde_json::from_str(&content).unwrap();
    assert_eq!(
        parsed.discovery.max_dns_cache_capacity,
        hardware.sizing.dns_cache_capacity
    );
}

#[test]
fn test_partial_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    // Operator only overrides 1 single field!
    let partial_json = r#"{
        "discovery": {
            "max_dns_cache_capacity": 99999
        }
    }"#;

    fs::write(&runtime_json_path, partial_json).unwrap();

    // Hardware is Medium (4 GB RAM, 4 cores)
    let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    // Explicitly overridden field MUST match
    assert_eq!(resolved.discovery.max_dns_cache_capacity, 99_999);

    // All missing fields MUST cleanly fallback to hardware defaults!
    let expected_lkg = (hardware.sizing.dns_cache_capacity / 5).max(100);
    assert_eq!(resolved.discovery.max_lkg_capacity, expected_lkg);
    assert_eq!(resolved.discovery.max_negative_ttl_secs, 60);
    assert_eq!(resolved.transport.io_workers, 4);
    assert_eq!(
        resolved.transport.max_active_connections,
        hardware.sizing.tcp_max_connections
    );

    // Verify that the file was updated with the full merged state
    let updated_content = fs::read_to_string(&runtime_json_path).unwrap();
    let updated_profile: RuntimeProfile = serde_json::from_str(&updated_content).unwrap();
    assert_eq!(updated_profile.discovery.max_dns_cache_capacity, 99_999);
    assert_eq!(updated_profile.discovery.max_lkg_capacity, expected_lkg);
}

#[test]
fn test_grouped_transport_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    // Even if legacy or rogue JSON specifies io_workers: 12, hardware (4 cores) MUST win!
    let json = r#"{
        "version": 1,
        "transport": {
            "io_workers": 12,
            "max_active_connections": 750000,
            "tcp": {
                "backlog": 9999,
                "copy_buffer_size": 49152
            },
            "udp": {
                "recv_buffer_size": 5242880
            }
        }
    }"#;

    fs::write(&runtime_json_path, json).unwrap();
    let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.transport.io_workers, 4); // Ignored from JSON, bound to 4 cores!
    assert_eq!(resolved.transport.max_active_connections, 750_000);
    assert_eq!(resolved.transport.tcp.backlog, 9999);
    assert_eq!(resolved.transport.tcp.copy_buffer_size, 49_152);
    assert_eq!(resolved.transport.udp.recv_buffer_size, Some(5_242_880));
}

#[test]
fn test_tls_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    let json = r#"{
        "version": 1,
        "tls": {
            "session_cache_capacity": 50000,
            "session_shards": 64,
            "max_early_data_size": 65536,
            "send_tls13_tickets": 7,
            "handshake_timeout_secs": 12
        }
    }"#;

    fs::write(&runtime_json_path, json).unwrap();
    let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.tls.session_cache_capacity, 50_000);
    assert_eq!(resolved.tls.session_shards, 64);
    assert_eq!(resolved.tls.max_early_data_size, 65_536);
    assert_eq!(resolved.tls.send_tls13_tickets, 7);
    assert_eq!(resolved.tls.handshake_timeout_secs, 12);

    let params = resolved.to_tls_server_params();
    assert_eq!(params.session_cache_capacity, 50_000);
    assert_eq!(params.session_shards, 64);
    assert_eq!(params.max_early_data_size, 65_536);
    assert_eq!(params.send_tls13_tickets, 7);
    assert_eq!(params.handshake_timeout.as_secs(), 12);
}

#[test]
fn test_parse_example_runtime_json() {
    let example_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../velda-sync/example/runtime.json");
    assert!(example_path.exists(), "example/runtime.json must exist");

    let content = fs::read_to_string(&example_path).unwrap();
    assert!(
        !content.contains("\"hardware\""),
        "example runtime.json must not contain hardware"
    );
    assert!(
        !content.contains("\"io_workers\""),
        "example runtime.json must not contain io_workers"
    );
    let profile: RuntimeProfile = serde_json::from_str(&content)
        .expect("example/runtime.json must be valid according to RuntimeProfile schema");

    assert_eq!(profile.version, 1);
    assert_eq!(profile.tls.session_cache_capacity, 8192);
    assert_eq!(profile.tls.send_tls13_tickets, 4);
}

#[test]
fn test_hardware_probe_isolation_from_stale_json() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    // Hostile or stale legacy JSON attempts to inject fake hardware, legacy tiers, and worker count
    let stale_json = r#"{
        "version": 1,
        "hardware": {
            "tier": "ultra",
            "memory_tier": "ultra",
            "cpu_tier": "ultra",
            "detected_cores": 64,
            "worker_threads": 64
        },
        "transport": {
            "io_workers": 64,
            "max_active_connections": 10000000
        }
    }"#;

    fs::write(&runtime_json_path, stale_json).unwrap();
    let hardware = HardwareTopology::with_workers_and_memory(2, 1024 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    // Stale hardware & io_workers MUST be stripped; actual physical hardware wins!
    assert_eq!(resolved.memory_bytes(), 1024 * 1024 * 1024);
    assert_eq!(resolved.available_cores(), 2);
    assert_eq!(resolved.transport.io_workers, 2);
    assert_eq!(resolved.transport.max_active_connections, 10_000_000); // legitimate operator override honored
}

#[test]
fn test_overload_config_override_in_runtime_json() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    let json = r#"{
        "version": 1,
        "overload": {
            "shedding_high_watermark": 0.85,
            "shedding_low_watermark": 0.75,
            "critical_high_watermark": 0.95,
            "critical_low_watermark": 0.88
        }
    }"#;

    fs::write(&runtime_json_path, json).unwrap();
    let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.overload.shedding_high_watermark, 0.85);
    assert_eq!(resolved.overload.shedding_low_watermark, 0.75);
    assert_eq!(resolved.overload.critical_high_watermark, 0.95);
    assert_eq!(resolved.overload.critical_low_watermark, 0.88);
}

#[test]
fn test_exact_cores_pinning_for_odd_topologies() {
    // 1-core VPS (e.g. cheapest droplet/instance)
    let hw_1 = HardwareTopology::with_workers_and_memory(1, 1024 * 1024 * 1024);
    let p_1 = RuntimeProfile::from_hardware(&hw_1);
    assert_eq!(p_1.transport.io_workers, 1);
    assert_eq!(p_1.transport.tcp.concurrency_shards, 1);
    assert_eq!(p_1.to_udp_socket_config().concurrency_shards, 1);

    // 3-cores instance (e.g. Hetzner cloud or custom cloud quota)
    let hw_3 = HardwareTopology::with_workers_and_memory(3, 4 * 1024 * 1024 * 1024);
    let p_3 = RuntimeProfile::from_hardware(&hw_3);
    assert_eq!(p_3.transport.io_workers, 3);
    assert_eq!(p_3.transport.tcp.concurrency_shards, 3);
    assert_eq!(p_3.to_udp_socket_config().concurrency_shards, 3);

    // 6-cores instance
    let hw_6 = HardwareTopology::with_workers_and_memory(6, 16 * 1024 * 1024 * 1024);
    let p_6 = RuntimeProfile::from_hardware(&hw_6);
    assert_eq!(p_6.transport.io_workers, 6);
    assert_eq!(p_6.transport.tcp.concurrency_shards, 6);
    assert_eq!(p_6.to_udp_socket_config().concurrency_shards, 6);
}
