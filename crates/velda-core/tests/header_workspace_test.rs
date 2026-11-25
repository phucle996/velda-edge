//! Integration tests for the flat single-allocation [`HeaderWorkspace`].

use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use velda_core::buffer::header::{DEFAULT_HEADER_CAPACITY, MAX_WIRE_HEADERS};
use velda_core::{HeaderWorkspace, TAG_BYPASS_BIT, header_key};

#[test]
fn test_header_workspace_recycle_and_shrink_from_spike() {
    let mut ws = HeaderWorkspace::new(DEFAULT_HEADER_CAPACITY);
    assert_eq!(ws.capacity(), DEFAULT_HEADER_CAPACITY);
    assert!(ws.get(&HeaderWorkspace::X_REQUEST_ID).is_some());
    assert!(ws.ingress_time_nanos > 1_700_000_000_000_000_000);
    assert!(ws.ingress_time_sec() > 1_700_000_000);

    ws.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    ws.insert(AUTHORIZATION, HeaderValue::from_static("Bearer token123"));
    assert_eq!(ws.len(), 3);
    assert!(ws.remove(&HeaderWorkspace::X_REQUEST_ID).is_some());
    assert_eq!(ws.len(), 2);

    // Trigger cold grow 32 -> 64
    for i in 0..40 {
        let name = HeaderName::from_bytes(format!("x-spike-{i}").as_bytes()).unwrap();
        ws.insert(name, HeaderValue::from_static("1"));
    }
    assert_eq!(ws.capacity(), 64);

    // Reset restores clean 32-slot block
    ws.reset();
    assert_eq!(ws.len(), 1);
    assert_eq!(ws.capacity(), DEFAULT_HEADER_CAPACITY);
}

#[test]
fn test_header_workspace_hard_ceiling_256() {
    let mut ws = HeaderWorkspace::default();
    for i in 0..300 {
        let name = HeaderName::from_bytes(format!("x-h-{i}").as_bytes()).unwrap();
        ws.append(name, HeaderValue::from_static("v"));
    }
    assert_eq!(ws.len(), MAX_WIRE_HEADERS);
}

#[test]
fn test_header_workspace_direct_reads_and_writes() {
    let mut ws = HeaderWorkspace::new(32);
    let req_id_u128 = ws
        .get_hex128(&HeaderWorkspace::X_REQUEST_ID)
        .expect("x-request-id must be valid 128-bit hex");
    let ts = ws.ingress_time_sec();

    let h_limit = HeaderName::from_static("x-ratelimit-limit");
    let h_rem = HeaderName::from_static("x-ratelimit-remaining");
    let h_trace = HeaderName::from_static("x-trace-id");
    let h_ip = HeaderName::from_static("x-real-ip");
    let h_ts = HeaderName::from_static("x-edge-ts");
    let h_proto = HeaderName::from_static("x-forwarded-proto");

    let ip_v4 = velda_core::lut::ip::encode_ipv4_u128(0x0A18_0005);
    ws.set_u64(h_limit.clone(), 100);
    ws.set_u64(h_rem.clone(), 99);
    ws.set_hex128(h_trace.clone(), req_id_u128);
    ws.set_ip(h_ip.clone(), ip_v4);
    ws.set_u64(h_ts.clone(), ts);
    ws.set_inline_ascii(h_proto.clone(), b"https");

    // Directly readable via both `.get(...)` and `.get_*` with zero render step
    assert_eq!(ws.get(&h_limit).unwrap(), "100");
    assert_eq!(ws.get_u64(&h_limit), Some(100));
    assert_eq!(ws.get_u64(&h_rem), Some(99));
    assert_eq!(ws.get_hex128(&h_trace), Some(req_id_u128));
    assert_eq!(ws.get(&h_ip).unwrap(), "10.24.0.5");
    assert_eq!(ws.get_ip_u128(&h_ip), Some(ip_v4));
    assert_eq!(ws.get_u64(&h_ts), Some(ts));
    assert_eq!(ws.get(&h_proto).unwrap(), "https");

    ws.set_ip(h_ip.clone(), 1);
    assert_eq!(ws.get(&h_ip).unwrap(), "::1");
    assert_eq!(ws.get_ip_u128(&h_ip), Some(1));
}

#[test]
fn test_header_workspace_bypass_bit_and_retain_bypass_only() {
    let mut ws = HeaderWorkspace::new(32);
    let req_id_str = ws.request_id().to_str().unwrap().to_string();

    ws.insert(http::header::HOST, HeaderValue::from_static("example.com"));
    ws.insert(
        http::header::COOKIE,
        HeaderValue::from_static("session=abc"),
    );
    ws.insert(
        http::header::USER_AGENT,
        HeaderValue::from_static("curl/8.0"),
    );
    let h_ratelimit = HeaderName::from_static("x-ratelimit-remaining");
    ws.insert_bypass_u64(h_ratelimit.clone(), 0);
    assert_eq!(ws.bypass_headers().count(), 2);
    assert_ne!(header_key(b"host") & TAG_BYPASS_BIT, TAG_BYPASS_BIT);

    ws.retain_bypass_only();

    assert_eq!(ws.len(), 2);
    assert!(!ws.contains_key(http::header::HOST));
    assert!(!ws.contains_key(http::header::COOKIE));
    assert!(!ws.contains_key(http::header::USER_AGENT));
    assert_eq!(
        ws.get(&HeaderWorkspace::X_REQUEST_ID).unwrap(),
        req_id_str.as_str()
    );
    assert_eq!(ws.get(&h_ratelimit).unwrap(), "0");
}

#[test]
fn test_header_workspace_prehashed_key_and_simd_boundaries() {
    use velda_core::HeaderKey;

    const KEY_SFS: HeaderKey = HeaderKey::from_static("sec-fetch-site");
    const KEY_007: HeaderKey = HeaderKey::from_static("x-boundary-007");
    const KEY_031: HeaderKey = HeaderKey::from_static("x-boundary-031");
    const KEY_063: HeaderKey = HeaderKey::from_static("x-boundary-063");
    const KEY_255: HeaderKey = HeaderKey::from_static("x-boundary-255");
    const KEY_MISS: HeaderKey = HeaderKey::from_static("x-boundary-missing");
    const KEY_COOKIE: HeaderKey = HeaderKey::from_static("cookie");

    let mut ws = HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY);
    for i in 0..256usize {
        let name_str = format!("x-boundary-{i:03}");
        let name = HeaderName::from_bytes(name_str.as_bytes()).unwrap();
        let val = HeaderValue::from_str(&format!("v-{i}")).unwrap();
        assert!(ws.push_prehashed(header_key(name_str.as_bytes()), name, val));
    }

    assert_eq!(ws.capacity(), MAX_WIRE_HEADERS);
    assert_eq!(ws.get_prehashed(&KEY_007).unwrap(), "v-7");
    assert_eq!(ws.get_prehashed(&KEY_031).unwrap(), "v-31");
    assert_eq!(ws.get_prehashed(&KEY_063).unwrap(), "v-63");
    assert_eq!(ws.get_prehashed(&KEY_255).unwrap(), "v-255");
    assert!(!ws.contains_prehashed(&KEY_MISS));
    assert!(!ws.contains_prehashed(&KEY_SFS));

    // Overwrite and duplicate cleanup via insert_prehashed
    let prev = ws.insert_prehashed(&KEY_031, HeaderValue::from_static("updated-31"));
    assert_eq!(prev.unwrap(), "v-31");
    assert_eq!(ws.get_prehashed(&KEY_031).unwrap(), "updated-31");

    // Multi-value get_all_prehashed across slots
    let mut multi = HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY);
    multi.append(http::header::COOKIE, HeaderValue::from_static("a=1"));
    multi.append(http::header::HOST, HeaderValue::from_static("example.com"));
    multi.append(http::header::COOKIE, HeaderValue::from_static("b=2"));
    let cookies: Vec<&str> = multi
        .get_all_prehashed(&KEY_COOKIE)
        .map(|v| v.to_str().unwrap())
        .collect();
    assert_eq!(cookies, vec!["a=1", "b=2"]);

    // Verify clear() followed by repopulation masks stale tail tags cleanly
    multi.clear();
    assert!(!multi.contains_prehashed(&KEY_COOKIE));
    multi.insert_prehashed(&KEY_SFS, HeaderValue::from_static("same-origin"));
    assert_eq!(multi.get_prehashed(&KEY_SFS).unwrap(), "same-origin");
    assert!(!multi.contains_prehashed(&KEY_COOKIE));
}

#[test]
fn test_header_workspace_7bit_collision_safety() {
    use velda_core::HeaderKey;

    // Intentionally craft a bogus HeaderKey whose tag7 matches "authorization"'s tag7
    // while `name` differs, ensuring exact HeaderName verification rejects false positives.
    let mut ws = HeaderWorkspace::empty(DEFAULT_HEADER_CAPACITY);
    ws.insert(
        HeaderName::from_static("x-decoy"),
        HeaderValue::from_static("decoy-secret"),
    );
    let decoy_tag7 = velda_core::fold_tag_hash7(header_key(b"x-decoy"));
    let forged_key = HeaderKey {
        hash64: header_key(b"x-decoy"),
        tag7: decoy_tag7,
        name: AUTHORIZATION.clone(),
    };
    assert!(
        ws.get_prehashed(&forged_key).is_none(),
        "7-bit tag match alone must never return a mismatched header name"
    );
}
