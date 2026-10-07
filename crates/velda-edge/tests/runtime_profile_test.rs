use std::fs;

use velda_core::hardware::HardwareTopology;
use velda_edge::{RuntimeProfile, resolve_runtime_profile};

#[test]
fn test_profile_across_all_tiers() {
    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * MB;

    let test_cases = [
        (256 * MB, "constrained", 1_000, 500, 10_000, 1024, 0),
        (1024 * MB, "small", 10_000, 2_000, 50_000, 2048, 4096),
        (4 * GB, "medium", 50_000, 10_000, 200_000, 8192, 8192),
        (16 * GB, "large", 150_000, 30_000, 500_000, 16384, 8192),
        (48 * GB, "xlarge", 400_000, 80_000, 1_000_000, 32768, 16384),
        (
            96 * GB,
            "2xlarge",
            1_000_000,
            200_000,
            2_000_000,
            65536,
            16384,
        ),
        (
            256 * GB,
            "ultra",
            2_500_000,
            500_000,
            4_000_000,
            131072,
            32768,
        ),
    ];

    for (
        ram_bytes,
        expected_tier,
        expected_cache,
        expected_lkg,
        expected_conns,
        expected_tls_cache,
        expected_early_data,
    ) in test_cases
    {
        let hardware = HardwareTopology::with_workers_and_memory(4, ram_bytes);
        let profile = RuntimeProfile::from_hardware(&hardware);

        assert_eq!(profile.hardware.tier, expected_tier);
        assert_eq!(profile.hardware.memory_tier, expected_tier);
        assert_eq!(profile.hardware.cpu_tier, "small"); // 4 cores = small
        assert_eq!(profile.transport.io_workers, 2); // small = 2 io_workers
        assert_eq!(profile.transport.max_active_connections, expected_conns);
        assert_eq!(profile.discovery.max_dns_cache_capacity, expected_cache);
        assert_eq!(profile.discovery.max_lkg_capacity, expected_lkg);

        // TLS profile assertions
        assert_eq!(profile.tls.session_cache_capacity, expected_tls_cache);
        assert_eq!(profile.tls.max_early_data_size, expected_early_data);
        assert_eq!(profile.tls.send_tls13_tickets, 2); // small cpu = 2 tickets
        assert_eq!(profile.tls.handshake_timeout_secs, 8); // small cpu = 8s

        let tls_params = profile.to_tls_server_params();
        assert_eq!(tls_params.session_cache_capacity, expected_tls_cache);
        assert_eq!(tls_params.max_early_data_size, expected_early_data);
        assert_eq!(tls_params.send_tls13_tickets, 2);
        assert_eq!(tls_params.handshake_timeout.as_secs(), 8);

        let dns_cfg = profile.to_dns_resolver_config();
        assert_eq!(dns_cfg.cache_capacity, expected_cache);
        assert_eq!(dns_cfg.lkg_capacity, expected_lkg);

        let tcp_cfg = profile.to_tcp_listener_config();
        assert!(tcp_cfg.backlog >= 512);
        assert_eq!(tcp_cfg.copy_buffer_size, 16 * 1024); // small cpu = 16KB

        let udp_cfg = profile.to_udp_socket_config();
        assert!(udp_cfg.recv_buffer_size.is_some());
    }
}

#[test]
fn test_operator_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    // Custom operator override with explicitly tuned values
    let custom_json = r#"{
        "version": 1,
        "hardware": {
            "detected_ram_bytes": 1073741824,
            "detected_cores": 8,
            "worker_threads": 8,
            "tier": "custom"
        },
        "discovery": {
            "max_dns_cache_capacity": 42000,
            "max_lkg_capacity": 7777,
            "max_negative_ttl_secs": 120,
            "query_timeout_ms": 1500,
            "negative_ttl_secs": 25,
            "positive_ttl_secs": 45
        },
        "transport": {
            "io_workers": 8
        }
    }"#;

    fs::write(&runtime_json_path, custom_json).unwrap();

    let hardware = HardwareTopology::with_workers_and_memory(2, 256 * 1024 * 1024);
    // Even though hardware is constrained (256MB), operator's runtime.json MUST win!
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.hardware.tier, "custom");
    assert_eq!(resolved.discovery.max_dns_cache_capacity, 42_000);
    assert_eq!(resolved.discovery.max_lkg_capacity, 7_777);
    assert_eq!(resolved.discovery.max_negative_ttl_secs, 120);
    assert_eq!(resolved.transport.io_workers, 8);
}

#[test]
fn test_fallback_writeback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    assert!(!runtime_json_path.exists());

    let hardware = HardwareTopology::with_workers_and_memory(2, 160 * 1024 * 1024 * 1024); // 160GB -> Ultra
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.hardware.tier, "ultra");
    assert_eq!(resolved.discovery.max_dns_cache_capacity, 2_500_000);
    assert_eq!(resolved.discovery.max_lkg_capacity, 500_000);
    assert_eq!(resolved.transport.max_active_connections, 4_000_000);

    // Verify write-back occurred
    assert!(runtime_json_path.exists());
    let content = fs::read_to_string(&runtime_json_path).unwrap();
    let parsed: RuntimeProfile = serde_json::from_str(&content).unwrap();
    assert_eq!(parsed, resolved);
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

    // All missing fields MUST cleanly fallback to Medium tier defaults!
    assert_eq!(resolved.hardware.tier, "medium");
    assert_eq!(resolved.discovery.max_lkg_capacity, 10_000);
    assert_eq!(resolved.discovery.max_negative_ttl_secs, 60);
    assert_eq!(resolved.discovery.query_timeout_ms, 2000);
    assert_eq!(resolved.transport.io_workers, 2); // 4 cores = small cpu tier (2 workers)
    assert_eq!(resolved.transport.max_active_connections, 200_000);

    // Verify that the file was updated with the full merged state
    let updated_content = fs::read_to_string(&runtime_json_path).unwrap();
    let updated_profile: RuntimeProfile = serde_json::from_str(&updated_content).unwrap();
    assert_eq!(updated_profile.discovery.max_dns_cache_capacity, 99_999);
    assert_eq!(updated_profile.discovery.max_lkg_capacity, 10_000);
}

#[test]
fn test_grouped_transport_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

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

    assert_eq!(resolved.transport.io_workers, 12);
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
    let profile: RuntimeProfile = serde_json::from_str(&content)
        .expect("example/runtime.json must be valid according to RuntimeProfile schema");

    assert_eq!(profile.version, 1);
    assert_eq!(profile.hardware.tier, "medium");
    assert_eq!(profile.transport.io_workers, 4);
    assert_eq!(profile.tls.session_cache_capacity, 8192);
    assert_eq!(profile.tls.send_tls13_tickets, 4);
    assert_eq!(profile.memory_tier(), velda_core::MemoryTier::Medium);
    assert_eq!(profile.cpu_tier(), velda_core::CpuTier::Medium);
}

#[test]
fn test_effective_tier_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let runtime_json_path = temp_dir.path().join("runtime.json");

    // Machine hardware is small (1GB RAM, 2 cores), but operator sets tier to ultra
    let json = r#"{
        "version": 1,
        "hardware": {
            "tier": "ultra",
            "memory_tier": "ultra",
            "cpu_tier": "ultra"
        }
    }"#;

    fs::write(&runtime_json_path, json).unwrap();
    let hardware = HardwareTopology::with_workers_and_memory(2, 1024 * 1024 * 1024);
    let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

    assert_eq!(resolved.memory_tier(), velda_core::MemoryTier::Ultra);
    assert_eq!(resolved.cpu_tier(), velda_core::CpuTier::Ultra);
    assert_eq!(resolved.transport.max_active_connections, 4_000_000);
    assert_eq!(resolved.discovery.max_dns_cache_capacity, 2_500_000);
    assert_eq!(resolved.tls.session_cache_capacity, 131_072);
}
