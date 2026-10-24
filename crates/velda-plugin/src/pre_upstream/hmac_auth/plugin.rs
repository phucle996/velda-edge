//! Pre-Upstream HMAC Authentication Plugin (`hmac_auth`).
//!
//! Enforces zero-IO, zero-heap-allocation, hardware-accelerated constant-time
//! Hash-based Message Authentication Code verification (RFC 2104).
//! Protects upstream services against request tampering, identity spoofing, and replay attacks
//! via strict timestamp drift windows, 128-bit SIMD constant-time comparison, and hardware-sharded
//! lock-striped nonce tracking.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::header::{HeaderName, HeaderValue};
use ring::hmac;
use rustc_hash::FxHashMap;
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::HmacAuthConfig;
use velda_sync::post_sync::plugin::PluginConfig;

use super::canonical::{
    CanonicalRequest, SignedHeadersSpec, compute_sha256, constant_time_eq, format_base64_to_buf,
    format_hex_to_buf, sign_canonical_h1, sign_canonical_h2_h3,
};
use super::nonce::NonceCache;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Standard SHA-256 hex digest of an empty byte array `b""`.
const EMPTY_SHA256_HEX: &[u8; 64] =
    b"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Helper to get current Unix epoch seconds.
#[inline(always)]
fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Zero-allocation, branchless ASCII decimal digit parser for timestamps and lengths.
#[inline(always)]
fn parse_u64_bytes(bytes: &[u8]) -> Option<u64> {
    let trimmed = bytes.trim_ascii();
    if trimmed.is_empty() {
        return None;
    }
    let mut val = 0u64;
    for &b in trimmed {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(val)
}

/// Constant-time verification of the client-supplied `content_sha256_header` against expected hex.
#[inline(always)]
fn verify_content_sha256(header_val: Option<&HeaderValue>, expected_hex: &[u8; 64]) -> bool {
    match header_val {
        Some(val) => {
            let client_hash = val.as_bytes().trim_ascii();
            if client_hash.is_empty() {
                return true;
            }
            if client_hash.len() != 64 {
                return false;
            }
            let mut client_buf = [0u8; 64];
            for (i, &b) in client_hash.iter().enumerate() {
                client_buf[i] = b.to_ascii_lowercase();
            }
            constant_time_eq(&client_buf, expected_hex)
        }
        None => true,
    }
}

/// Pre-Upstream HMAC Authentication Plugin.
pub struct HmacAuthPlugin {
    pub id: String,
    pub config: HmacAuthConfig,
    /// Pre-compiled HMAC key map: access_key_id -> (precompiled_key, pre_materialized_consumer_header)
    credentials: FxHashMap<Box<[u8]>, (hmac::Key, Option<HeaderValue>)>,
    /// Thread-safe hardware-sharded nonce cache for anti-replay tracking
    nonce_cache: Arc<NonceCache>,
    /// Pre-parsed HeaderNames for zero-allocation header retrieval
    access_key_header: HeaderName,
    signature_header: HeaderName,
    timestamp_header: HeaderName,
    content_sha256_header: HeaderName,
    nonce_header: HeaderName,
    /// Pre-compiled signed headers specification
    signed_headers: SignedHeadersSpec,
    /// Pre-parsed flags to avoid string comparison on hot path
    is_base64_encoding: bool,
    is_sha512: bool,
}

impl HmacAuthPlugin {
    /// Constructs a new `HmacAuthPlugin` instance with full validation and pre-materialization.
    pub fn new(id: impl Into<String>, config: HmacAuthConfig) -> Result<Self, PluginError> {
        let id_str = id.into();

        let parse_header = |name: &str, field: &str| -> Result<HeaderName, PluginError> {
            HeaderName::from_str(name).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid {field} '{name}' for plugin '{id_str}': {e}"
                ))
            })
        };
        let access_key_header = parse_header(&config.access_key_header, "access_key_header")?;
        let signature_header = parse_header(&config.signature_header, "signature_header")?;
        let timestamp_header = parse_header(&config.timestamp_header, "timestamp_header")?;
        let content_sha256_header =
            parse_header(&config.content_sha256_header, "content_sha256_header")?;
        let nonce_header = parse_header(&config.nonce_header, "nonce_header")?;

        let is_sha512 = config.signature_algorithm == "hmac-sha512";
        let is_base64_encoding = config.signature_encoding == "base64";

        let hmac_algo = if is_sha512 {
            hmac::HMAC_SHA512
        } else {
            hmac::HMAC_SHA256
        };

        // Pre-compile HMAC keys and pre-materialize consumer HeaderValues into RAM
        let mut credentials =
            FxHashMap::with_capacity_and_hasher(config.credentials.len(), Default::default());
        for cred in &config.credentials {
            let key = hmac::Key::new(hmac_algo, cred.secret_key.as_bytes());
            let consumer_hval = match &cred.consumer_id {
                Some(cid) => Some(HeaderValue::from_str(cid).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid consumer_id '{cid}' for plugin '{id_str}': {e}"
                    ))
                })?),
                None => None,
            };
            credentials.insert(
                cred.access_key_id.as_bytes().to_vec().into_boxed_slice(),
                (key, consumer_hval),
            );
        }

        let signed_headers = SignedHeadersSpec::compile(&config.signed_headers, &id_str)?;

        Ok(Self {
            id: id_str,
            config,
            credentials,
            nonce_cache: Arc::new(NonceCache::new()),
            access_key_header,
            signature_header,
            timestamp_header,
            content_sha256_header,
            nonce_header,
            signed_headers,
            is_base64_encoding,
            is_sha512,
        })
    }

    #[inline(always)]
    fn reject_unauthorized(req: &mut L7Request) -> Action {
        req.headers.insert_bypass(
            http::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("HMAC realm=\"velda-edge\""),
        );
        Action::Reject(RejectReason::Unauthorized)
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_hmac_auth_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(e.to_string()))?;
        Self::new(&config.id, cfg)
    }

    /// Computes and verifies the payload hash bytes without heap allocations.
    ///
    /// SECURITY PATCH (Scenario A3 - Body Tampering Bypass):
    /// When request payload is present in memory (`Body::Bytes`), we MUST NEVER blindly trust
    /// the client-supplied `content_sha256_header`. We always compute the SHA-256 digest of
    /// the actual request body and verify in constant time that the client header matches
    /// the calculated digest. Any mismatch or tampered body is rejected immediately.
    ///
    /// For unbuffered/streaming requests where `Body::Empty` but `content-length` or
    /// `transfer-encoding` indicates an in-flight payload, the client-supplied `content_sha256_header`
    /// is used for canonical HMAC signing (as specified by AWS SigV4 / RFC 2104).
    #[inline(always)]
    fn compute_payload_hash_bytes<'buf>(
        &self,
        req: &L7Request,
        stack_buf: &'buf mut [u8; 64],
    ) -> Option<&'buf [u8]> {
        if !self.config.validate_request_body {
            return Some(b"UNSIGNED-PAYLOAD");
        }

        let header_val = req.headers.get(&self.content_sha256_header);

        if !req.body.is_empty() {
            // Actual body bytes are available: ALWAYS compute true SHA-256 digest!
            let digest = compute_sha256(req.body.as_slice());
            format_hex_to_buf(&digest, stack_buf);

            // Security check: If client also provided x-content-sha256 header,
            // strictly verify in constant time that it matches the actual body digest!
            if !verify_content_sha256(header_val, stack_buf) {
                return None;
            }

            Some(stack_buf.as_slice())
        } else {
            // If request header declares an unbuffered body in streaming mode,
            // use client-supplied content_sha256_header if provided.
            let has_unbuffered_payload = req
                .headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| parse_u64_bytes(v.as_bytes()))
                .is_some_and(|len| len > 0)
                || req.headers.contains_key(http::header::TRANSFER_ENCODING);

            if has_unbuffered_payload && let Some(val) = header_val {
                let client_hash = val.as_bytes().trim_ascii();
                if client_hash.len() == 64 {
                    for (i, &b) in client_hash.iter().enumerate() {
                        stack_buf[i] = b.to_ascii_lowercase();
                    }
                    return Some(stack_buf.as_slice());
                }
            }

            // Truly empty body: verify client header matches empty SHA-256 in constant time
            if !verify_content_sha256(header_val, EMPTY_SHA256_HEX) {
                return None;
            }

            Some(EMPTY_SHA256_HEX.as_slice())
        }
    }
}

impl PreUpstreamPlugin for HmacAuthPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let now = current_epoch_secs();

        // 1. Extract Access Key ID (Pure byte trimming, zero UTF-8 validation overhead!)
        let access_key_id = match req.headers.get(&self.access_key_header) {
            Some(v) => v.as_bytes().trim_ascii(),
            None => return Self::reject_unauthorized(req),
        };

        // 2. Lookup Pre-Compiled Secret Key in RAM (Zero key-derivation overhead!)
        let (key, consumer_hval_opt) = match self.credentials.get(access_key_id) {
            Some((k, cid)) => (k, cid),
            None => return Self::reject_unauthorized(req),
        };

        // 3. Extract and Validate Timestamp (Zero-allocation byte parsing)
        let ts_bytes = match req.headers.get(&self.timestamp_header) {
            Some(v) => v.as_bytes(),
            None => return Self::reject_unauthorized(req),
        };

        let req_timestamp = match parse_u64_bytes(ts_bytes) {
            Some(t) => t,
            None => return Self::reject_unauthorized(req),
        };

        let diff = now.abs_diff(req_timestamp);
        if diff > self.config.clock_skew_seconds {
            return Self::reject_unauthorized(req);
        }

        // 4. Nonce Phase 1 (Peek): Extract Nonce Header & Check Replay in ~10ns BEFORE doing any crypto
        let nonce_fp_opt = if self.config.enforce_nonce {
            match req.headers.get(&self.nonce_header) {
                Some(v) => {
                    let trimmed = v.as_bytes().trim_ascii();
                    if trimmed.is_empty() {
                        return Self::reject_unauthorized(req);
                    }
                    let fp = super::nonce::nonce_fingerprint_128(trimmed);
                    if self.nonce_cache.is_replayed_fp(fp, now) {
                        return Self::reject_unauthorized(req);
                    }
                    Some(fp)
                }
                None => return Self::reject_unauthorized(req),
            }
        } else {
            None
        };

        // 5. Extract Client Signature & Pre-Crypto Length Guard
        let client_sig = match req.headers.get(&self.signature_header) {
            Some(v) => {
                let trimmed = v.as_bytes().trim_ascii();
                if trimmed.is_empty() {
                    return Self::reject_unauthorized(req);
                }
                trimmed
            }
            None => return Self::reject_unauthorized(req),
        };

        let expected_sig_len = match (self.is_sha512, self.is_base64_encoding) {
            (false, false) => 64, // SHA-256 Hex
            (false, true) => 44,  // SHA-256 Base64
            (true, false) => 128, // SHA-512 Hex
            (true, true) => 88,   // SHA-512 Base64
        };
        if client_sig.len() != expected_sig_len {
            return Self::reject_unauthorized(req);
        }

        // 6. Compute and verify Payload Hash (using stack buffer if calculated from raw body)
        let mut payload_buf = [0u8; 64];
        let payload_hash_bytes = match self.compute_payload_hash_bytes(req, &mut payload_buf) {
            Some(h) => h,
            None => return Self::reject_unauthorized(req),
        };

        // 7. Extract Authority for HTTP/2 & HTTP/3 (:authority)
        let authority = req.uri.authority().map(|a| a.as_str().as_bytes());

        let canonical_req = CanonicalRequest {
            method: req.method.as_str(),
            path: req.uri.path(),
            query: req.uri.query(),
            headers: &req.headers,
            authority,
            payload_hash: payload_hash_bytes,
        };

        // Zero-branching protocol-specialized signing
        let tag = if req.version <= http::Version::HTTP_11 {
            sign_canonical_h1(key, &canonical_req, &self.signed_headers)
        } else {
            sign_canonical_h2_h3(key, &canonical_req, &self.signed_headers)
        };

        // 8. Format Computed Signature on Stack and Verify via 128-bit SIMD constant_time_eq
        let is_valid = if self.is_base64_encoding {
            let mut b64_buf = [0u8; 88];
            let expected_sig = format_base64_to_buf(tag.as_ref(), &mut b64_buf);
            constant_time_eq(client_sig, expected_sig)
        } else if self.is_sha512 {
            let mut hex_buf = [0u8; 128];
            format_hex_to_buf(tag.as_ref(), &mut hex_buf);
            constant_time_eq(client_sig, &hex_buf)
        } else {
            let mut hex_buf = [0u8; 64];
            format_hex_to_buf(tag.as_ref(), &mut hex_buf);
            constant_time_eq(client_sig, &hex_buf)
        };

        if !is_valid {
            return Self::reject_unauthorized(req);
        }

        // 9. Nonce Phase 2 (Commit): Record Authentic Nonce to Sharded NonceCache
        if let Some(fp) = nonce_fp_opt {
            let expiry = req_timestamp + self.config.clock_skew_seconds;
            if !self.nonce_cache.check_and_record_fp(fp, expiry, now) {
                return Self::reject_unauthorized(req);
            }
        }

        // 10. Inject Pre-materialized Consumer ID Header (Zero string parsing on hot path)
        if let Some(hval) = consumer_hval_opt {
            req.headers
                .insert(HeaderName::from_static("x-consumer-id"), hval.clone());
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        if self.config.validate_request_body {
            PluginCapability::new(AccessLevel::Write, AccessLevel::Read)
        } else {
            PluginCapability::header_only(AccessLevel::Write)
        }
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;
    use velda_sync::HmacCredentialConfig;

    fn to_hex(bytes: &[u8]) -> String {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX_CHARS[(b >> 4) as usize] as char);
            out.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        out
    }

    fn make_test_plugin(validate_body: bool, enforce_nonce: bool) -> HmacAuthPlugin {
        let config = HmacAuthConfig {
            access_key_header: "x-access-key-id".to_string(),
            signature_header: "x-signature".to_string(),
            signature_algorithm: "hmac-sha256".to_string(),
            signature_encoding: "hex".to_string(),
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
            credentials: vec![HmacCredentialConfig {
                access_key_id: "test_key_01".to_string(),
                secret_key: "sec_secret_key_123456789".to_string(),
                consumer_id: Some("test-consumer".to_string()),
            }],
        };

        HmacAuthPlugin::new("hmac-1", config).unwrap()
    }

    fn make_signed_req(
        method: Method,
        uri_str: &str,
        version: Version,
        body_bytes: &'static [u8],
        nonce: &str,
        signed_header_names: &[&str],
    ) -> L7Request {
        let now = current_epoch_secs();
        let payload_hash = to_hex(&compute_sha256(body_bytes));
        let mut headers = HeaderMap::new();
        let uri: Uri = uri_str.parse().unwrap();

        if version <= Version::HTTP_11 {
            headers.insert("host", HeaderValue::from_static("api.velda.io"));
        }
        headers.insert(
            "x-timestamp",
            HeaderValue::from_str(&now.to_string()).unwrap(),
        );
        headers.insert("x-nonce", HeaderValue::from_str(nonce).unwrap());
        headers.insert("x-access-key-id", HeaderValue::from_static("test_key_01"));

        let key = hmac::Key::new(hmac::HMAC_SHA256, b"sec_secret_key_123456789");
        let signed_strings: Vec<String> =
            signed_header_names.iter().map(|s| s.to_string()).collect();
        let spec = SignedHeadersSpec::compile(&signed_strings, "test").unwrap();

        let authority = uri.authority().map(|a| a.as_str().as_bytes());
        let canonical_req = CanonicalRequest {
            method: method.as_str(),
            path: uri.path(),
            query: uri.query(),
            headers: &headers,
            authority,
            payload_hash: payload_hash.as_bytes(),
        };

        let tag = if version <= Version::HTTP_11 {
            sign_canonical_h1(&key, &canonical_req, &spec)
        } else {
            sign_canonical_h2_h3(&key, &canonical_req, &spec)
        };
        headers.insert(
            "x-signature",
            HeaderValue::from_str(&to_hex(tag.as_ref())).unwrap(),
        );

        L7Request::new(
            method,
            uri,
            version,
            headers,
            Body::Bytes(bytes::Bytes::from_static(body_bytes)),
        )
    }

    #[test]
    fn test_hmac_auth_success_and_replay_protection() {
        let plugin = make_test_plugin(true, true);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut req = make_signed_req(
            Method::POST,
            "/v1/payments",
            Version::HTTP_11,
            b"{\"amount\":100000}",
            "nonce-unique-001",
            &signed_headers,
        );
        let mut target = Arc::from("default-upstream");

        // First attempt: MUST SUCCEED
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
        assert_eq!(req.headers.get("x-consumer-id").unwrap(), "test-consumer");

        // Second attempt with exact same nonce: MUST FAIL (Replay detected!)
        let replay = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(replay, Action::Reject(RejectReason::Unauthorized)));
    }

    #[test]
    fn test_hmac_auth_invalid_signature_does_not_poison_nonce() {
        let plugin = make_test_plugin(true, true);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut forged_req = make_signed_req(
            Method::POST,
            "/v1/payments",
            Version::HTTP_11,
            b"{\"amount\":100000}",
            "nonce-poison-01",
            &signed_headers,
        );
        forged_req
            .headers
            .insert("x-signature", HeaderValue::from_static("forged_bad_sig"));
        let mut target = Arc::from("default-upstream");

        // Attacker sends forged signature -> Rejected
        assert!(matches!(
            plugin.on_upstream_request(&mut forged_req, &mut target),
            Action::Reject(RejectReason::Unauthorized)
        ));

        // Legitimate client sends valid signature with SAME nonce -> MUST SUCCEED (nonce not poisoned)
        let mut valid_req = make_signed_req(
            Method::POST,
            "/v1/payments",
            Version::HTTP_11,
            b"{\"amount\":100000}",
            "nonce-poison-01",
            &signed_headers,
        );
        assert!(matches!(
            plugin.on_upstream_request(&mut valid_req, &mut target),
            Action::Continue
        ));
    }

    #[test]
    fn test_hmac_auth_tampered_payload_rejected() {
        let plugin = make_test_plugin(true, false);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut req = make_signed_req(
            Method::POST,
            "/v1/payments",
            Version::HTTP_11,
            b"{\"amount\":100000}",
            "nonce-unique-002",
            &signed_headers,
        );
        req.body = Body::Bytes(bytes::Bytes::from_static(b"{\"amount\":999999}"));
        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
    }

    /// Verifies the security patch for Scenario A3 (Body Tampering Bypass via client-supplied x-content-sha256).
    #[test]
    fn test_hmac_auth_body_tampering_bypass_with_content_sha256_header_prevented() {
        let plugin = make_test_plugin(true, false);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut req = make_signed_req(
            Method::POST,
            "/v1/payments",
            Version::HTTP_11,
            b"{\"amount\":100000}",
            "nonce-unique-003",
            &signed_headers,
        );
        let original_hash = to_hex(&compute_sha256(b"{\"amount\":100000}"));
        req.headers.insert(
            "x-content-sha256",
            HeaderValue::from_str(&original_hash).unwrap(),
        );
        req.body = Body::Bytes(bytes::Bytes::from_static(b"{\"amount\":9999999}"));
        let mut target = Arc::from("default-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Reject(RejectReason::Unauthorized)));
    }

    /// Verifies that a legitimate request containing x-content-sha256 header (even uppercase) succeeds.
    #[test]
    fn test_hmac_auth_valid_with_content_sha256_header() {
        let plugin = make_test_plugin(true, false);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut req = make_signed_req(
            Method::POST,
            "/v1/payments",
            Version::HTTP_11,
            b"{\"amount\":500000}",
            "nonce-unique-004",
            &signed_headers,
        );
        let upper_hash = to_hex(&compute_sha256(b"{\"amount\":500000}")).to_ascii_uppercase();
        req.headers.insert(
            "x-content-sha256",
            HeaderValue::from_str(&upper_hash).unwrap(),
        );
        let mut target = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
    }

    /// Verifies HTTP/2 and HTTP/3 support where Host is transmitted via :authority in URI
    /// without a physical Host header in HeaderMap (RFC 9113 compliance).
    #[test]
    fn test_hmac_auth_http2_authority_support() {
        let plugin = make_test_plugin(true, false);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut req = make_signed_req(
            Method::POST,
            "https://api.velda.io/v1/payments",
            Version::HTTP_2,
            b"{\"action\":\"h2_auth_test\"}",
            "nonce-h2-001",
            &signed_headers,
        );
        req.headers.remove("host"); // RFC 9113: :authority in URI instead of Host header
        let mut target = Arc::from("default-upstream");
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
    }
}
