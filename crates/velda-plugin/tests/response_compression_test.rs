//! Integration Tests for `response_compression` Post-Response Plugin.

use http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, VARY};
use http::{HeaderValue, StatusCode};
use velda_core::{Body, HeaderWorkspace, L7Response};
use velda_plugin::config::PluginConfig;
use velda_plugin::post_response::PostResponsePlugin;
use velda_plugin::post_response::response_compression::dictionary::{
    BUILTIN_COMMON_JSON_REST_DICT, BuiltinDictionary, ZstdDictionaryConfig,
};
use velda_plugin::post_response::response_compression::{
    CompressionAlgorithm, ResponseCompressionConfig, ResponseCompressionPlugin,
    decode_zstd_with_dict, encode_zstd,
};

fn standard_test_config() -> ResponseCompressionConfig {
    ResponseCompressionConfig {
        algorithms: vec![
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Brotli,
            CompressionAlgorithm::Gzip,
        ],
        min_length: 64,
        gzip_level: 1,
        brotli_level: 2,
        zstd_level: 1,
        zstd_dict: ZstdDictionaryConfig::None,
        content_types: vec!["application/json".into(), "text/".into()],
    }
}

#[test]
fn test_compression_gzip_application_json() {
    let plugin = ResponseCompressionPlugin::new("compress-gzip", standard_test_config());

    let mut req_headers = HeaderWorkspace::default();
    req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

    let original_json = b"{\"message\":\"hello world from velda-edge post_response plugin compression test\",\"status\":\"active\",\"code\":200}".repeat(5);
    let mut resp = L7Response::from_bytes(StatusCode::OK, original_json.clone());
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
    assert_eq!(resp.headers.get(VARY).unwrap(), "Accept-Encoding");

    match resp.body {
        Body::Bytes(b) => {
            assert!(b.len() < original_json.len());
            let mut decoder = flate2::read::GzDecoder::new(&b[..]);
            let mut decompressed = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut decompressed).unwrap();
            assert_eq!(decompressed, original_json);
        }
        _ => panic!("Expected Body::Bytes"),
    }
}

#[test]
fn test_compression_zstd_with_builtin_dict() {
    let config = ResponseCompressionConfig {
        algorithms: vec![CompressionAlgorithm::Zstd],
        zstd_dict: ZstdDictionaryConfig::Builtin(BuiltinDictionary::CommonJsonRest),
        ..standard_test_config()
    };
    let plugin = ResponseCompressionPlugin::new("compress-builtin-dict", config);

    let mut req_headers = HeaderWorkspace::default();
    req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("zstd"));

    let payload = br#"{"status":"success","code":200,"message":"Resource retrieved successfully","data":[{"id":"usr_101","username":"dev_user","email":"dev@example.com","role":"admin","status":"active"}]}"#;
    let mut resp = L7Response::from_bytes(StatusCode::OK, payload.to_vec());
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "zstd");
    match resp.body {
        Body::Bytes(b) => {
            let decompressed =
                decode_zstd_with_dict(&b, Some(BUILTIN_COMMON_JSON_REST_DICT)).unwrap();
            assert_eq!(decompressed, payload[..]);
        }
        _ => panic!("Expected Body::Bytes"),
    }
}

#[test]
fn test_from_config_strict_validation() {
    let valid_json = serde_json::json!({
        "algorithms": ["zstd", "brotli", "gzip"],
        "min_length_bytes": 256,
        "gzip_level": 1,
        "brotli_level": 2,
        "zstd_level": 1,
        "zstd_dict": null,
        "content_types": ["text/", "application/json"]
    });

    let p_cfg = PluginConfig {
        id: "comp_test".into(),
        config: valid_json,
    };

    let plugin = ResponseCompressionPlugin::from_config(&p_cfg).unwrap();
    assert_eq!(plugin.config.algorithms.len(), 3);
    assert_eq!(plugin.config.min_length, 256);

    let builtin_json = serde_json::json!({
        "algorithms": ["zstd"],
        "min_length_bytes": 256,
        "gzip_level": 1,
        "brotli_level": 2,
        "zstd_level": 1,
        "zstd_dict": "common_json_rest",
        "content_types": ["application/json"]
    });
    let builtin_cfg = PluginConfig {
        id: "comp_builtin".into(),
        config: builtin_json,
    };
    let builtin_plugin = ResponseCompressionPlugin::from_config(&builtin_cfg).unwrap();
    assert_eq!(
        builtin_plugin.config.zstd_dict.as_bytes().unwrap(),
        BUILTIN_COMMON_JSON_REST_DICT
    );
}

#[test]
fn test_guards_and_stack_vary_merge() {
    let plugin = ResponseCompressionPlugin::new("compress-guard", standard_test_config());
    let mut req_headers = HeaderWorkspace::default();
    req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

    // Guard 1: Status code 204
    let mut resp = L7Response::from_bytes(StatusCode::NO_CONTENT, b"some content".to_vec());
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    plugin.on_response(&req_headers, &mut resp);
    assert!(!resp.headers.contains_key(CONTENT_ENCODING));

    // Guard 2: Already compressed
    let mut resp = L7Response::from_bytes(StatusCode::OK, b"compressed".repeat(20));
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp.headers
        .insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
    plugin.on_response(&req_headers, &mut resp);
    assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "br");

    // Guard 4: Non-matching content type
    let mut resp = L7Response::from_bytes(StatusCode::OK, b"compressed".repeat(20));
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("image/png"));
    plugin.on_response(&req_headers, &mut resp);
    assert!(!resp.headers.contains_key(CONTENT_ENCODING));

    // Existing Vary header preserved and appended via stack buffer
    let payload = b"{\"k\":\"velda edge response compression vary preservation test\"}".repeat(4);
    let mut resp = L7Response::from_bytes(StatusCode::OK, payload);
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp.headers
        .insert(VARY, HeaderValue::from_static("Origin"));
    plugin.on_response(&req_headers, &mut resp);
    assert_eq!(resp.headers.get(VARY).unwrap(), "Origin, Accept-Encoding");
}

#[test]
fn test_zstd_level_switching_rebuilds_thread_local_compressor() {
    let payload = b"{\"alpha\":123456,\"beta\":\"repetitive-json-payload-data\"}".repeat(12);
    let c1 = encode_zstd(&payload, 1).unwrap();
    let c19 = encode_zstd(&payload, 19).unwrap();
    assert!(c19.len() <= c1.len());
    assert_eq!(decode_zstd_with_dict(&c1, None).unwrap(), payload[..]);
    assert_eq!(decode_zstd_with_dict(&c19, None).unwrap(), payload[..]);
}
