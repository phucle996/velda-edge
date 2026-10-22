use super::*;
use tempfile::tempdir;

fn cfg(id: &str, addr: &str, transport_proto: &str, app_proto: &str) -> ListenerConfig {
    ListenerConfig {
        id: id.into(),
        address: addr.into(),
        transport: ListenerTransportConfig {
            protocol: transport_proto.into(),
        },
        application: ListenerApplicationConfig {
            protocol: app_proto.into(),
            version: None,
            streaming: velda_core::StreamingMode::DISABLED,
        },
        tls: Default::default(),
        http1: None,
        http2: None,
        grpc: None,
        http3: None,
        raw: None,
    }
}

#[test]
fn test_parse_valid_listeners() {
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "http",
            "address": " 0.0.0.0:80 ",
            "transport": { "protocol": "TCP" },
            "application": { "protocol": "HTTP1", "version": "1.1", "streaming": [] },
            "tls": { "enabled": false },
            "http1": {
                "max_body_size": 10485760,
                "max_header_size": 65536,
                "max_headers": 64,
                "idle_timeout_ms": 30000
            }
        }]
    }"#;

    let mut listeners = parse_listeners(json.as_bytes()).unwrap();
    assert_eq!(listeners.len(), 1);
    validate_listeners(&mut listeners).unwrap();
    assert_eq!(listeners[0].address, "0.0.0.0:80");
    assert_eq!(listeners[0].transport.protocol, "tcp");
    assert_eq!(listeners[0].application.protocol, "http1");
    assert_eq!(listeners[0].application.version.as_deref(), Some("1.1"));
    assert_eq!(
        listeners[0].http1.as_ref().unwrap().max_header_size,
        Some(65536)
    );
}

#[test]
fn test_omitted_protocol_block_is_valid() {
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "http",
            "address": "0.0.0.0:80",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http1", "streaming": [] },
            "tls": { "enabled": false }
        }]
    }"#;

    let mut listeners = parse_listeners(json.as_bytes()).unwrap();
    assert_eq!(listeners.len(), 1);
    assert!(validate_listeners(&mut listeners).is_ok());
    assert!(listeners[0].http1.is_none());
}

#[test]
fn test_conflicting_protocols_rejected() {
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "http",
            "address": "0.0.0.0:80",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http1", "streaming": [] },
            "tls": { "enabled": false },
            "http2": {
                "max_concurrent_streams": 256
            }
        }]
    }"#;

    let mut listeners = parse_listeners(json.as_bytes()).unwrap();
    assert!(validate_listeners(&mut listeners).is_err());
}

#[test]
fn test_missing_streaming_fails() {
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "http",
            "address": "0.0.0.0:80",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http1" },
            "tls": { "enabled": false }
        }]
    }"#;

    assert!(
        parse_listeners(json.as_bytes()).is_err(),
        "Missing mandatory streaming must fail deserialization"
    );
}

#[test]
fn test_unsupported_protocol_fails() {
    let mut listeners = vec![cfg(
        "bad-transport",
        "0.0.0.0:80",
        "unsupported_proto",
        "http1",
    )];
    assert!(matches!(
        validate_listeners(&mut listeners),
        Err(SyncError::Validation { .. })
    ));

    let mut listeners2 = vec![cfg("bad-app", "0.0.0.0:80", "tcp", "unsupported_app")];
    assert!(matches!(
        validate_listeners(&mut listeners2),
        Err(SyncError::Validation { .. })
    ));
}

#[test]
fn test_duplicate_listener_id_fails() {
    let mut listeners = vec![
        cfg("http", "0.0.0.0:80", "tcp", "http1"),
        cfg("http", "0.0.0.0:8080", "tcp", "http1"),
    ];
    assert!(matches!(
        validate_listeners(&mut listeners),
        Err(SyncError::Validation { .. })
    ));
}

#[test]
fn test_internal_port_conflict_fails() {
    let mut listeners = vec![
        cfg("http-1", "127.0.0.1:18080", "tcp", "http1"),
        cfg("tcp-1", "127.0.0.1:18080", "tcp", "raw"), // both use L4 TCP
    ];
    let err = validate_listeners(&mut listeners).unwrap_err();
    assert!(err.to_string().contains("Internal port conflict"));
}

#[test]
fn test_port_wildcard_overlap_fails() {
    let mut listeners = vec![
        cfg("all", "0.0.0.0:18081", "tcp", "http1"),
        cfg("loopback", "127.0.0.1:18081", "tcp", "raw"),
    ];
    let err = validate_listeners(&mut listeners).unwrap_err();
    assert!(err.to_string().contains("Internal port conflict"));
}

#[test]
fn test_tcp_and_udp_coexist_on_same_port() {
    let mut listeners = vec![
        cfg("dns-tcp", "127.0.0.1:18082", "tcp", "raw"),
        cfg("dns-udp", "127.0.0.1:18082", "udp", "raw"),
    ];
    assert!(validate_listeners(&mut listeners).is_ok());
}

#[test]
fn test_host_os_port_conflict_detected() {
    let host_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = host_socket.local_addr().unwrap().port();
    let mut listeners = vec![cfg(
        "host-conflict",
        &format!("127.0.0.1:{port}"),
        "tcp",
        "raw",
    )];

    let err = validate_listeners(&mut listeners).unwrap_err();
    assert!(err.to_string().contains("Host OS port conflict"));

    drop(host_socket);
    assert!(validate_listeners(&mut listeners).is_ok());
}

#[test]
fn test_listener_binary_roundtrip() {
    let tmp = tempdir().unwrap();
    let listeners = vec![cfg("http", "0.0.0.0:80", "tcp", "http1")];
    let binary = compile_listeners_to_binary(&listeners, 5, [0x11u8; 32]).unwrap();

    let (header, restored) = unpack_listeners_from_binary(&binary).unwrap();
    assert_eq!(header.revision, 5);
    assert_eq!(restored, listeners);

    persist_listeners(tmp.path(), b"{\"listeners\": []}", &binary).unwrap();
    assert!(tmp.path().join("config/listeners.json").exists());
    assert!(tmp.path().join("runtime/listeners.bin").exists());
}

#[test]
fn test_listener_streaming_validation() {
    let mut invalid_raw = vec![cfg("raw-stream", "127.0.0.1:18090", "tcp", "raw")];
    invalid_raw[0].application.streaming = velda_core::StreamingMode::SERVER;
    let err = validate_listeners(&mut invalid_raw).unwrap_err();
    assert!(err.to_string().contains("does not support L7 streaming"));

    let mut valid_h2 = vec![cfg("h2-stream", "127.0.0.1:18091", "tcp", "http2")];
    valid_h2[0].application.streaming = velda_core::StreamingMode::SERVER;
    assert!(validate_listeners(&mut valid_h2).is_ok());
}

#[test]
fn test_http_family_listener_validation() {
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "web-in",
            "address": "127.0.0.1:61443",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http", "streaming": ["server"] },
            "tls": { "enabled": true },
            "http1": {
                "max_body_size": 10485760,
                "max_header_size": 65536
            },
            "http2": {
                "max_body_size": 10485760,
                "max_concurrent_streams": 256
            }
        }]
    }"#;

    let mut listeners = parse_listeners(json.as_bytes()).unwrap();
    assert_eq!(listeners.len(), 1);
    assert!(validate_listeners(&mut listeners).is_ok());
    assert_eq!(listeners[0].application.protocol, "http");
    assert!(listeners[0].http1.is_some());
    assert!(listeners[0].http2.is_some());

    // Conflicting block with grpc should fail
    let invalid_json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "web-bad",
            "address": "127.0.0.1:61443",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http", "streaming": [] },
            "tls": { "enabled": true },
            "grpc": { "max_message_size": 8388608 }
        }]
    }"#;
    let mut bad_listeners = parse_listeners(invalid_json.as_bytes()).unwrap();
    let err = validate_listeners(&mut bad_listeners).unwrap_err();
    assert!(err.to_string().contains("conflicting protocol blocks"));
}

#[test]
fn test_parse_example_listeners_json() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = std::path::Path::new(manifest_dir).join("example/listeners.json");
    let content = std::fs::read(&path).expect("example/listeners.json must exist");
    let mut listeners = parse_listeners(&content).expect("must parse listeners.json");
    assert_eq!(listeners.len(), 5);
    validate_listeners(&mut listeners).expect("example/listeners.json must be valid");
    assert_eq!(listeners[0].application.protocol, "http");
    assert_eq!(listeners[1].application.protocol, "http");
    assert_eq!(listeners[2].application.protocol, "grpc");
    assert_eq!(listeners[3].application.protocol, "http");
    assert_eq!(listeners[4].application.protocol, "raw");
}

#[test]
fn test_alt_svc_config_parsing_and_validation() {
    // Test structured alt_svc
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "web-altsvc",
            "address": "127.0.0.1:61445",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http", "streaming": [] },
            "tls": { "enabled": true },
            "http1": {
                "alt_svc": {
                    "port": 8443,
                    "max_age": 3600,
                    "persist": true
                }
            },
            "http2": {
                "alt_svc": {
                    "port": 443
                }
            }
        }]
    }"#;
    let mut listeners = parse_listeners(json.as_bytes()).unwrap();
    assert!(validate_listeners(&mut listeners).is_ok());

    let h1_alt = listeners[0]
        .http1
        .as_ref()
        .unwrap()
        .effective_alt_svc()
        .unwrap();
    assert_eq!(h1_alt.port, 8443);
    assert_eq!(h1_alt.max_age, 3600);
    assert!(h1_alt.persist);
    assert_eq!(
        h1_alt.to_header_value_string(),
        "h3=\":8443\"; ma=3600; persist=1"
    );

    let h2_alt = listeners[0]
        .http2
        .as_ref()
        .unwrap()
        .effective_alt_svc()
        .unwrap();
    assert_eq!(h2_alt.port, 443);
    assert_eq!(h2_alt.max_age, 86400); // default
    assert!(!h2_alt.persist);
    assert_eq!(h2_alt.to_header_value_string(), "h3=\":443\"; ma=86400");

    // Test invalid port 0
    let bad_json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "bad-port",
            "address": "127.0.0.1:61446",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http", "streaming": [] },
            "tls": { "enabled": true },
            "http1": { "alt_svc": { "port": 0 } }
        }]
    }"#;
    let mut bad_listeners = parse_listeners(bad_json.as_bytes()).unwrap();
    let err = validate_listeners(&mut bad_listeners).unwrap_err();
    assert!(err.to_string().contains("invalid alt_svc port 0"));

    // Test invalid max_age 0
    let bad_json2 = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "bad-ma",
            "address": "127.0.0.1:61447",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http", "streaming": [] },
            "tls": { "enabled": true },
            "http2": { "alt_svc": { "port": 443, "max_age": 0 } }
        }]
    }"#;
    let mut bad_listeners2 = parse_listeners(bad_json2.as_bytes()).unwrap();
    let err2 = validate_listeners(&mut bad_listeners2).unwrap_err();
    assert!(err2.to_string().contains("invalid alt_svc max_age 0"));
}
