//! Pre-Upstream HMAC Authentication Plugin (`hmac_auth`).
//!
//! Validates RFC 2104 HMAC-SHA256 / HMAC-SHA512 request signatures on `SupportedProtocols::ALL_HTTP`:
//! - Credential lookup via static zero-hash AGU bit-shift index ([`CredentialTable`]).
//! - Pre-hashed [`HeaderKey`] lookups on [`velda_core::HeaderWorkspace`].
//! - Stack signature decoding (`hex` case-insensitive or `base64`) before cryptographic work.
//! - Streaming HMAC verification via [`ring::hmac::Context`] (pre-computed `ipad`/`opad`).
//! - Post-verification single-lock [`NonceCache`] replay commit (allocated only when `enforce_nonce == true`).
//! - Authoritative `x-consumer-id` injection or removal to block client header spoofing.
//!
//! ```text
//!   on_upstream_request(req)
//!     ├─► 1. Extract access_key (prehashed) ─► CredentialTable::lookup (bit-shift/AGU, 0 hash)
//!     ├─► 2. Parse timestamp (prehashed)    ─► |now - ts| <= clock_skew_seconds
//!     ├─► 3. Extract & decode signature on stack (hex/b64 length & syntax guard)
//!     ├─► 4. Compute/verify payload SHA-256 (empty LUT / Body::Bytes / streaming header)
//!     ├─► 5. Streaming HMAC verify (ring::hmac::Context + ring::constant_time)
//!     ├─► 6. Single-lock NonceCache::check_and_record_fp (only if enforce_nonce)
//!     └─► 7. Set or scrub `x-consumer-id` ─► Action::Continue (or Reject 401 + WWW-Authenticate)
//! ```

use std::str::FromStr;

use http::header::{HeaderName, HeaderValue};
use ring::hmac;
use velda_core::{Action, HeaderKey, InternalStatus, L7Request};

use super::canonical::{
    CanonicalRequest, SignedHeadersSpec, compute_sha256, constant_time_eq, decode_client_signature,
    format_hex_to_buf, sign_canonical_request,
};
use super::credential::{CredentialEntry, CredentialTable};
use super::nonce::{NonceCache, nonce_fingerprint_128};
use super::validation::HmacAuthConfig;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

const EMPTY_SHA256_HEX: &[u8; 64] =
    b"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

const KEY_CONTENT_LENGTH: HeaderKey = HeaderKey::from_static("content-length");
const KEY_TRANSFER_ENCODING: HeaderKey = HeaderKey::from_static("transfer-encoding");
const KEY_X_CONSUMER_ID: HeaderKey = HeaderKey::from_static("x-consumer-id");

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

#[inline(always)]
fn verify_content_sha256(header_val: Option<&HeaderValue>, expected_hex: &[u8; 64]) -> bool {
    let Some(val) = header_val else {
        return true;
    };
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

/// Pre-Upstream HMAC Authentication Plugin.
#[repr(C, align(64))]
pub struct HmacAuthPlugin {
    is_base64_encoding: bool,
    is_sha512: bool,
    enforce_nonce: bool,
    validate_request_body: bool,
    clock_skew_seconds: u64,
    credentials: CredentialTable,
    nonce_cache: Option<NonceCache>,
    access_key_header: HeaderKey,
    signature_header: HeaderKey,
    timestamp_header: HeaderKey,
    content_sha256_header: HeaderKey,
    nonce_header: HeaderKey,
    signed_headers: SignedHeadersSpec,
    pub id: String,
    pub config: HmacAuthConfig,
}

impl HmacAuthPlugin {
    /// Constructs a validated, pre-compiled [`HmacAuthPlugin`] instance.
    pub fn new(id: impl Into<String>, config: HmacAuthConfig) -> Result<Self, PluginError> {
        let id_str = id.into();

        let parse_key = |name: &str, field: &str| -> Result<HeaderKey, PluginError> {
            let hname = HeaderName::from_str(name.trim()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid {field} '{name}' for plugin '{id_str}': {e}"
                ))
            })?;
            Ok(HeaderKey::from_name(hname))
        };

        let access_key_header = parse_key(&config.access_key_header, "access_key_header")?;
        let signature_header = parse_key(&config.signature_header, "signature_header")?;
        let timestamp_header = parse_key(&config.timestamp_header, "timestamp_header")?;
        let content_sha256_header =
            parse_key(&config.content_sha256_header, "content_sha256_header")?;
        let nonce_header = parse_key(&config.nonce_header, "nonce_header")?;

        let is_sha512 = config
            .signature_algorithm
            .trim()
            .eq_ignore_ascii_case("hmac-sha512");
        let is_base64_encoding = config
            .signature_encoding
            .trim()
            .eq_ignore_ascii_case("base64");
        let hmac_algo = if is_sha512 {
            hmac::HMAC_SHA512
        } else {
            hmac::HMAC_SHA256
        };

        let mut entries = Vec::with_capacity(config.credentials.len());
        for cred in &config.credentials {
            let key = hmac::Key::new(hmac_algo, cred.secret_key.as_bytes());
            let consumer_header = match &cred.consumer_id {
                Some(cid) => Some(HeaderValue::from_str(cid).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid consumer_id '{cid}' for plugin '{id_str}': {e}"
                    ))
                })?),
                None => None,
            };
            entries.push(CredentialEntry {
                access_key_id: cred.access_key_id.as_bytes().to_vec().into_boxed_slice(),
                key,
                consumer_header,
            });
        }
        let credentials = CredentialTable::build(entries);
        let signed_headers = SignedHeadersSpec::compile(&config.signed_headers, &id_str)?;
        let nonce_cache = config.enforce_nonce.then(NonceCache::new);

        Ok(Self {
            is_base64_encoding,
            is_sha512,
            enforce_nonce: config.enforce_nonce,
            validate_request_body: config.validate_request_body,
            clock_skew_seconds: config.clock_skew_seconds,
            credentials,
            nonce_cache,
            access_key_header,
            signature_header,
            timestamp_header,
            content_sha256_header,
            nonce_header,
            signed_headers,
            id: id_str,
            config,
        })
    }

    #[cold]
    #[inline(never)]
    fn reject_unauthorized(req: &mut L7Request) -> Action {
        const WWW_AUTH_HMAC: HeaderValue = HeaderValue::from_static("HMAC realm=\"velda-edge\"");
        req.headers
            .insert_bypass(http::header::WWW_AUTHENTICATE, WWW_AUTH_HMAC);
        Action::Status(InternalStatus::Unauthorized)
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_hmac_auth_config(&config.id, &config.config)
            .map_err(|e| PluginError::Config(e.to_string()))?;
        Self::new(&config.id, cfg)
    }

    #[inline(always)]
    fn compute_payload_hash_bytes<'buf>(
        &self,
        req: &L7Request,
        stack_buf: &'buf mut [u8; 64],
    ) -> Option<&'buf [u8]> {
        if !self.validate_request_body {
            return Some(b"UNSIGNED-PAYLOAD");
        }

        let header_val = req.headers.get_prehashed(&self.content_sha256_header);

        if !req.body.is_empty() {
            let digest = compute_sha256(req.body.as_slice());
            format_hex_to_buf(&digest, stack_buf);
            if !verify_content_sha256(header_val, stack_buf) {
                return None;
            }
            Some(stack_buf.as_slice())
        } else {
            let has_unbuffered_payload = req
                .headers
                .get_prehashed(&KEY_CONTENT_LENGTH)
                .and_then(|v| parse_u64_bytes(v.as_bytes()))
                .is_some_and(|len| len > 0)
                || req.headers.contains_prehashed(&KEY_TRANSFER_ENCODING);

            if has_unbuffered_payload && let Some(val) = header_val {
                let client_hash = val.as_bytes().trim_ascii();
                if client_hash.len() == 64 {
                    for (i, &b) in client_hash.iter().enumerate() {
                        stack_buf[i] = b.to_ascii_lowercase();
                    }
                    return Some(stack_buf.as_slice());
                }
            }

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

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        let now = req.ingress_time_sec();

        // 1. Access key lookup via zero-hash AGU index
        let Some(access_key_val) = req.headers.get_prehashed(&self.access_key_header) else {
            return Self::reject_unauthorized(req);
        };
        let access_key_id = access_key_val.as_bytes().trim_ascii();
        let Some(cred) = self.credentials.lookup(access_key_id) else {
            return Self::reject_unauthorized(req);
        };

        // 2. Timestamp drift window
        let Some(ts_val) = req.headers.get_prehashed(&self.timestamp_header) else {
            return Self::reject_unauthorized(req);
        };
        let Some(req_timestamp) = parse_u64_bytes(ts_val.as_bytes()) else {
            return Self::reject_unauthorized(req);
        };
        if now.abs_diff(req_timestamp) > self.clock_skew_seconds {
            return Self::reject_unauthorized(req);
        }

        // 3. Extract nonce fingerprint (lock-free; recorded only after signature passes)
        let nonce_fp = if self.enforce_nonce {
            let Some(nonce_val) = req.headers.get_prehashed(&self.nonce_header) else {
                return Self::reject_unauthorized(req);
            };
            let trimmed = nonce_val.as_bytes().trim_ascii();
            if trimmed.is_empty() {
                return Self::reject_unauthorized(req);
            }
            Some(nonce_fingerprint_128(trimmed))
        } else {
            None
        };

        // 4. Decode client signature directly onto stack before computing SHA-256 / HMAC
        let Some(sig_val) = req.headers.get_prehashed(&self.signature_header) else {
            return Self::reject_unauthorized(req);
        };
        let client_sig = sig_val.as_bytes().trim_ascii();
        let mut decoded_sig = [0u8; 64];
        let Some(sig_len) = decode_client_signature(
            client_sig,
            self.is_sha512,
            self.is_base64_encoding,
            &mut decoded_sig,
        ) else {
            return Self::reject_unauthorized(req);
        };

        // 5. Payload SHA-256 hash
        let mut payload_buf = [0u8; 64];
        let Some(payload_hash) = self.compute_payload_hash_bytes(req, &mut payload_buf) else {
            return Self::reject_unauthorized(req);
        };

        // 6. Streaming canonical HMAC sign & constant-time raw digest comparison
        let authority = req.uri.authority().map(|a| a.as_str().as_bytes());
        let canonical_req = CanonicalRequest {
            method: req.method.as_str(),
            path: req.uri.path(),
            query: req.uri.query(),
            headers: &req.headers,
            authority,
            payload_hash,
        };
        let tag = sign_canonical_request(&cred.key, &canonical_req, &self.signed_headers);
        if !constant_time_eq(tag.as_ref(), &decoded_sig[..sig_len]) {
            return Self::reject_unauthorized(req);
        }

        // 7. Single-lock nonce replay check & commit (authenticated requests only)
        if let (Some(fp), Some(cache)) = (nonce_fp, self.nonce_cache.as_ref()) {
            let expiry = req_timestamp.saturating_add(self.clock_skew_seconds);
            if !cache.check_and_record_fp(fp, expiry, now) {
                return Self::reject_unauthorized(req);
            }
        }

        // 8. Authoritative consumer identity (scrub client spoofed x-consumer-id when None)
        match &cred.consumer_header {
            Some(hval) => {
                req.headers
                    .insert_prehashed(&KEY_X_CONSUMER_ID, hval.clone());
            }
            None => {
                if req.headers.contains_prehashed(&KEY_X_CONSUMER_ID) {
                    req.headers.remove(&KEY_X_CONSUMER_ID.name);
                }
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        if self.validate_request_body {
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
    use super::super::validation::HmacCredentialConfig;
    use super::*;
    use http::{Method, Uri, Version};
    use velda_core::Body;

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
            credentials: vec![
                HmacCredentialConfig {
                    access_key_id: "test_key_01".to_string(),
                    secret_key: "sec_secret_key_123456789".to_string(),
                    consumer_id: Some("test-consumer".to_string()),
                },
                HmacCredentialConfig {
                    access_key_id: "anon_key_02".to_string(),
                    secret_key: "sec_anon_secret_987654321".to_string(),
                    consumer_id: None,
                },
            ],
        };

        HmacAuthPlugin::new("hmac-1", config).unwrap()
    }

    fn make_signed_req_with_key(
        method: Method,
        uri_str: &str,
        version: Version,
        body_bytes: &'static [u8],
        nonce: &str,
        signed_header_names: &[&str],
        cred: (&'static str, &[u8]),
    ) -> L7Request {
        let (access_key_id, secret) = cred;
        let now = velda_core::clock::coarse_sec();
        let payload_hash = to_hex(&compute_sha256(body_bytes));
        let mut headers = velda_core::HeaderWorkspace::default();
        let uri: Uri = uri_str.parse().unwrap();

        if version <= Version::HTTP_11 {
            headers.insert("host", HeaderValue::from_static("api.velda.io"));
        }
        headers.insert(
            "x-timestamp",
            HeaderValue::from_str(&now.to_string()).unwrap(),
        );
        headers.insert("x-nonce", HeaderValue::from_str(nonce).unwrap());
        headers.insert("x-access-key-id", HeaderValue::from_static(access_key_id));

        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
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

        let tag = sign_canonical_request(&key, &canonical_req, &spec);
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

    fn make_signed_req(
        method: Method,
        uri_str: &str,
        version: Version,
        body_bytes: &'static [u8],
        nonce: &str,
        signed_header_names: &[&str],
    ) -> L7Request {
        make_signed_req_with_key(
            method,
            uri_str,
            version,
            body_bytes,
            nonce,
            signed_header_names,
            ("test_key_01", b"sec_secret_key_123456789"),
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
        let mut target = 1;

        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
        assert_eq!(req.headers.get("x-consumer-id").unwrap(), "test-consumer");

        let replay = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(
            replay,
            Action::Status(InternalStatus::Unauthorized)
        ));
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
        let mut target = 1;

        assert!(matches!(
            plugin.on_upstream_request(&mut forged_req, &mut target),
            Action::Status(InternalStatus::Unauthorized)
        ));

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
        let mut target = 1;
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(
            action,
            Action::Status(InternalStatus::Unauthorized)
        ));
    }

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
        let mut target = 1;
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(
            action,
            Action::Status(InternalStatus::Unauthorized)
        ));
    }

    #[test]
    fn test_hmac_auth_valid_with_content_sha256_header_and_uppercase_sig() {
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
        let upper_sig = req
            .headers
            .get("x-signature")
            .unwrap()
            .to_str()
            .unwrap()
            .to_ascii_uppercase();
        req.headers
            .insert("x-signature", HeaderValue::from_str(&upper_sig).unwrap());

        let mut target = 1;
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
    }

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
        req.headers.remove("host");
        let mut target = 1;
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
    }

    #[test]
    fn test_hmac_auth_scrubs_spoofed_consumer_id_when_none() {
        let plugin = make_test_plugin(true, false);
        let signed_headers = ["host", "x-timestamp", "x-nonce"];
        let mut req = make_signed_req_with_key(
            Method::GET,
            "/v1/ping",
            Version::HTTP_11,
            b"",
            "nonce-scrub-01",
            &signed_headers,
            ("anon_key_02", b"sec_anon_secret_987654321"),
        );
        req.headers.insert(
            "x-consumer-id",
            HeaderValue::from_static("spoofed-super-admin"),
        );

        let mut target = 1;
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
        assert!(!req.headers.contains_key("x-consumer-id"));
    }
}
