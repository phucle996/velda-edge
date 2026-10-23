//! End-to-End Test Suite for `hmac_auth` Pre-Upstream Plugin.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::header::HeaderValue;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use ring::hmac;
use velda_core::{Action, Body, L7Request, L7Response};
use velda_plugin::PreUpstreamPlugin;
use velda_plugin::pre_upstream::hmac_auth::canonical::{
    CanonicalRequest, SignedHeadersSpec, compute_sha256, sign_canonical_request, to_base64,
};
use velda_plugin::pre_upstream::{HmacAuthConfig, HmacAuthPlugin, HmacCredentialConfig};

fn to_hex(bytes: &[u8]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX_CHARS[(b >> 4) as usize] as char);
        out.push(HEX_CHARS[(b & 0x0f) as usize] as char);
    }
    out
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn extract_body_str(resp: &L7Response) -> String {
    String::from_utf8_lossy(resp.body.as_slice()).to_string()
}

fn build_test_plugin(
    algo: &str,
    encoding: &str,
    validate_body: bool,
    enforce_nonce: bool,
) -> HmacAuthPlugin {
    let config = HmacAuthConfig {
        access_key_header: "x-access-key-id".to_string(),
        signature_header: "x-signature".to_string(),
        signature_algorithm: algo.to_string(),
        signature_encoding: encoding.to_string(),
        timestamp_header: "x-timestamp".to_string(),
        clock_skew_seconds: 60,
        signed_headers: vec![
            "host".to_string(),
            "x-timestamp".to_string(),
            "x-nonce".to_string(),
        ],
        validate_request_body: validate_body,
        content_sha256_header: "x-content-sha256".to_string(),
        nonce_header: "x-nonce".to_string(),
        enforce_nonce,
        credentials: vec![
            HmacCredentialConfig {
                access_key_id: "partner_fintech_01".to_string(),
                secret_key: "sec_k89f2a019482bca881726c0981273412".to_string(),
                consumer_id: Some("fintech-prod-tenant".to_string()),
            },
            HmacCredentialConfig {
                access_key_id: "partner_sha512_02".to_string(),
                secret_key: "sec_sha512_secret_key_long_enough_12345".to_string(),
                consumer_id: Some("sha512-tenant".to_string()),
            },
        ],
    };

    HmacAuthPlugin::new("hmac-test", config).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn make_test_signed_request(
    algo: &str,
    encoding: &str,
    access_key: &str,
    secret_key: &[u8],
    method: Method,
    uri_str: &str,
    body_bytes: &'static [u8],
    nonce: &str,
    ts_offset: i64,
) -> L7Request {
    let now = (current_epoch_secs() as i64 + ts_offset) as u64;
    let payload_hash = to_hex(&compute_sha256(body_bytes));

    let uri: Uri = uri_str.parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("host", HeaderValue::from_static("api.velda.io"));
    headers.insert(
        "x-timestamp",
        HeaderValue::from_str(&now.to_string()).unwrap(),
    );
    headers.insert("x-nonce", HeaderValue::from_str(nonce).unwrap());
    headers.insert(
        "x-access-key-id",
        HeaderValue::from_str(access_key).unwrap(),
    );

    let signed_names = vec![
        "host".to_string(),
        "x-timestamp".to_string(),
        "x-nonce".to_string(),
    ];
    let spec = SignedHeadersSpec::compile(&signed_names, "test").unwrap();
    let canonical_req = CanonicalRequest {
        method: method.as_str(),
        path: uri.path(),
        query: uri.query(),
        headers: &headers,
        authority: None,
        payload_hash: payload_hash.as_bytes(),
    };

    let hmac_algo = if algo == "hmac-sha512" {
        hmac::HMAC_SHA512
    } else {
        hmac::HMAC_SHA256
    };
    let key = hmac::Key::new(hmac_algo, secret_key);
    let tag = sign_canonical_request(&key, &canonical_req, &spec);

    let sig_val = if encoding == "base64" {
        to_base64(tag.as_ref())
    } else {
        to_hex(tag.as_ref())
    };
    headers.insert("x-signature", HeaderValue::from_str(&sig_val).unwrap());

    L7Request::new(
        method,
        uri,
        Version::HTTP_11,
        headers,
        Body::Bytes(bytes::Bytes::from_static(body_bytes)),
    )
}

#[test]
fn test_hmac_auth_e2e_valid_hex() {
    let plugin = build_test_plugin("hmac-sha256", "hex", true, true);
    let mut req = make_test_signed_request(
        "hmac-sha256",
        "hex",
        "partner_fintech_01",
        b"sec_k89f2a019482bca881726c0981273412",
        Method::POST,
        "/v1/transfers?currency=VND",
        b"{\"recipient\":\"Bob\",\"amount\":500000}",
        "uuid-hex-1111",
        0,
    );
    let mut target = Arc::from("fintech-upstream");
    assert!(matches!(
        plugin.on_upstream_request(&mut req, &mut target),
        Action::Continue
    ));
    assert_eq!(
        req.headers.get("x-consumer-id").unwrap(),
        "fintech-prod-tenant"
    );
}

#[test]
fn test_hmac_auth_e2e_valid_base64() {
    let plugin = build_test_plugin("hmac-sha256", "base64", true, false);
    let mut req = make_test_signed_request(
        "hmac-sha256",
        "base64",
        "partner_fintech_01",
        b"sec_k89f2a019482bca881726c0981273412",
        Method::GET,
        "/v1/health",
        b"{\"action\":\"ping\"}",
        "uuid-b64-2222",
        0,
    );
    let mut target = Arc::from("fintech-upstream");
    assert!(matches!(
        plugin.on_upstream_request(&mut req, &mut target),
        Action::Continue
    ));
}

#[test]
fn test_hmac_auth_e2e_sha512() {
    let plugin = build_test_plugin("hmac-sha512", "hex", true, false);
    let mut req = make_test_signed_request(
        "hmac-sha512",
        "hex",
        "partner_sha512_02",
        b"sec_sha512_secret_key_long_enough_12345",
        Method::POST,
        "/v1/query",
        b"{\"query\":\"all\"}",
        "uuid-sha512-3333",
        0,
    );
    let mut target = Arc::from("sha512-upstream");
    assert!(matches!(
        plugin.on_upstream_request(&mut req, &mut target),
        Action::Continue
    ));
}

#[test]
fn test_hmac_auth_missing_access_key() {
    let plugin = build_test_plugin("hmac-sha256", "hex", true, true);
    let mut req = L7Request::new(
        Method::POST,
        Uri::from_static("/v1/transfers"),
        Version::HTTP_11,
        HeaderMap::new(),
        Body::Empty,
    );

    let mut target = Arc::from("fintech-upstream");
    let action = plugin.on_upstream_request(&mut req, &mut target);

    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("Missing access key"));
        }
        _ => panic!("Expected Missing access key rejection"),
    }
}

#[test]
fn test_hmac_auth_invalid_access_key() {
    let plugin = build_test_plugin("hmac-sha256", "hex", true, true);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-access-key-id",
        HeaderValue::from_static("unknown_caller_999"),
    );

    let mut req = L7Request::new(
        Method::POST,
        Uri::from_static("/v1/transfers"),
        Version::HTTP_11,
        headers,
        Body::Empty,
    );

    let mut target = Arc::from("fintech-upstream");
    let action = plugin.on_upstream_request(&mut req, &mut target);

    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("Invalid access key"));
        }
        _ => panic!("Expected Invalid access key rejection"),
    }
}

#[test]
fn test_hmac_auth_expired_timestamp() {
    let plugin = build_test_plugin("hmac-sha256", "hex", true, true);
    let mut req = make_test_signed_request(
        "hmac-sha256",
        "hex",
        "partner_fintech_01",
        b"sec_k89f2a019482bca881726c0981273412",
        Method::POST,
        "/v1/transfers",
        b"{}",
        "nonce-exp-01",
        -120, // 2 minutes ago (skew max is 60s)
    );

    let mut target = Arc::from("fintech-upstream");
    let action = plugin.on_upstream_request(&mut req, &mut target);

    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("clock skew exceeded"));
        }
        _ => panic!("Expected Expired timestamp rejection"),
    }
}

#[test]
fn test_hmac_auth_tampered_header() {
    let plugin = build_test_plugin("hmac-sha256", "hex", true, false);
    let mut req = make_test_signed_request(
        "hmac-sha256",
        "hex",
        "partner_fintech_01",
        b"sec_k89f2a019482bca881726c0981273412",
        Method::POST,
        "/v1/transfers",
        b"{\"amount\":100}",
        "nonce-tamper-01",
        0,
    );

    // Attacker modifies Host header after signing
    req.headers
        .insert("host", HeaderValue::from_static("evil.attacker.com"));

    let mut target = Arc::from("fintech-upstream");
    let action = plugin.on_upstream_request(&mut req, &mut target);

    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("Invalid HMAC signature"));
        }
        _ => panic!("Expected Tampered header rejection"),
    }
}

#[test]
fn test_hmac_auth_replay_attack_prevention() {
    let plugin = build_test_plugin("hmac-sha256", "hex", true, true);
    let mut req = make_test_signed_request(
        "hmac-sha256",
        "hex",
        "partner_fintech_01",
        b"sec_k89f2a019482bca881726c0981273412",
        Method::POST,
        "/v1/transfers",
        b"{\"amount\":500}",
        "replay-guard-nonce-001",
        0,
    );

    let mut target = Arc::from("fintech-upstream");

    // First request: valid
    assert!(matches!(
        plugin.on_upstream_request(&mut req, &mut target),
        Action::Continue
    ));

    // Second request: exact replay with same nonce
    let action2 = plugin.on_upstream_request(&mut req, &mut target);
    match action2 {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("nonce already used"));
        }
        _ => panic!("Expected Replay rejection on repeated nonce"),
    }
}
