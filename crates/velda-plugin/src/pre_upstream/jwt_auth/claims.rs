//! Pre-Compiled Claim Projection, Assertions, and Zero-DOM Streaming Evaluator (`jwt_auth::claims`).
//!
//! ```text
//!   JWT Payload Bytes (&[u8], stack-decoded when <= 2 KiB)
//!     │
//!     ▼  serde_json::Deserializer::from_slice (single pass, 0 Value DOM allocations)
//!   PayloadStreamVisitor (root object)
//!     ├─ Key not in targets ──────────────────► serde::de::IgnoredAny (0 heap alloc)
//!     ├─ "exp" / "nbf" ───────────────────────► NumericDateVisitor (floor / ceil u64)
//!     ├─ "iss" / "aud" ───────────────────────► BorrowedStr / Seq equality check
//!     └─ prefix / terminal in ClaimPath[] ────► Descend nested object or capture HeaderValue/assert
//! ```

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use http::header::HeaderValue;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use velda_core::HeaderKey;

use super::crypto::JwtVerifyError;

/// Pre-split dot-notation claim path compiled once at configuration load time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimPath {
    segments: Box<[Box<str>]>,
}

impl ClaimPath {
    /// Compiles a dot-separated claim path (e.g. `"profile.tenant_id"`).
    pub fn compile(path: &str) -> Self {
        let segments = path
            .split('.')
            .map(|s| s.to_string().into_boxed_str())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { segments }
    }

    /// Walks a `serde_json::Value` tree along pre-compiled dot segments.
    #[inline]
    pub fn walk<'a>(&self, root: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
        let mut current = root;
        for seg in &self.segments {
            match current {
                serde_json::Value::Object(map) => current = map.get(seg.as_ref())?,
                _ => return None,
            }
        }
        Some(current)
    }

    #[inline(always)]
    pub(crate) fn segments(&self) -> &[Box<str>] {
        &self.segments
    }
}

/// Converts a JSON value into an HTTP header string matching RFC/Kong-style claim serialization.
pub fn value_to_header_string(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(if *b { "true".into() } else { "false".into() }),
        serde_json::Value::Array(arr) => {
            let mut out = String::new();
            for item in arr {
                let piece: Cow<'_, str> = match item {
                    serde_json::Value::String(s) => Cow::Borrowed(s.as_str()),
                    serde_json::Value::Number(n) => Cow::Owned(n.to_string()),
                    _ => continue,
                };
                if !out.is_empty() {
                    out.push_str(", ");
                }
                out.push_str(&piece);
            }
            if out.is_empty() { None } else { Some(out) }
        }
        _ => None,
    }
}

/// Resolves a nested claim path from a JSON object using dot notation (e.g., `user.id`).
pub fn resolve_claim_path(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            serde_json::Value::Object(map) => current = map.get(segment)?,
            _ => return None,
        }
    }
    value_to_header_string(current)
}

#[inline]
fn format_u64_ascii(mut n: u64, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[19] = b'0';
        return &buf[19..20];
    }
    let mut pos = 20usize;
    while n > 0 {
        pos -= 1;
        buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    &buf[pos..20]
}

#[inline]
fn format_i64_ascii(n: i64, buf: &mut [u8; 21]) -> &[u8] {
    if n >= 0 {
        let mut sub = [0u8; 20];
        let s = format_u64_ascii(n as u64, &mut sub);
        let start = 21 - s.len();
        buf[start..21].copy_from_slice(s);
        return &buf[start..21];
    }
    let mag = n.unsigned_abs();
    let mut sub = [0u8; 20];
    let s = format_u64_ascii(mag, &mut sub);
    let start = 20 - s.len();
    buf[start] = b'-';
    buf[start + 1..21].copy_from_slice(s);
    &buf[start..21]
}

// ============================================================================
// Streaming Single-Pass Claim Evaluator (Zero Heap DOM)
// ============================================================================

/// Result of single-pass streaming evaluation over JWT JSON payload bytes.
pub(crate) struct EvaluatedJwtPayload {
    pub exp_epoch_secs: u64,
    pub injected_headers: Option<Arc<[(HeaderKey, HeaderValue)]>>,
}

struct EvalState<'a> {
    validate_exp: bool,
    expected_issuer: Option<&'a str>,
    expected_audience: Option<&'a str>,
    claims_to_headers: &'a [(ClaimPath, HeaderKey)],
    assert_claims: &'a [(ClaimPath, Box<str>)],

    exp: Option<Result<u64, JwtVerifyError>>,
    nbf: Option<Result<u64, JwtVerifyError>>,
    iss_matched: Option<bool>,
    aud_matched: Option<bool>,
    header_slots: Vec<Option<HeaderValue>>,
    assert_mask: u64,
    assert_slots: Vec<bool>,
}

/// Evaluates `exp`, `nbf`, `iss`, `aud`, `assert_claims`, and `claims_to_headers`
/// in a single zero-DOM streaming pass without constructing `serde_json::Value`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_jwt_payload_streaming(
    payload_bytes: &[u8],
    validate_exp: bool,
    clock_skew_secs: u64,
    expected_issuer: Option<&str>,
    expected_audience: Option<&str>,
    claims_to_headers: &[(ClaimPath, HeaderKey)],
    assert_claims: &[(ClaimPath, Box<str>)],
    now_epoch: u64,
) -> Result<EvaluatedJwtPayload, JwtVerifyError> {
    let mut state = EvalState {
        validate_exp,
        expected_issuer,
        expected_audience,
        claims_to_headers,
        assert_claims,
        exp: None,
        nbf: None,
        iss_matched: None,
        aud_matched: None,
        header_slots: if claims_to_headers.is_empty() {
            Vec::new()
        } else {
            vec![None; claims_to_headers.len()]
        },
        assert_mask: 0,
        assert_slots: if assert_claims.len() <= 64 {
            Vec::new()
        } else {
            vec![false; assert_claims.len()]
        },
    };

    let mut de = serde_json::Deserializer::from_slice(payload_bytes);
    PayloadRootSeed { state: &mut state }
        .deserialize(&mut de)
        .map_err(|_| JwtVerifyError::MalformedToken)?;
    de.end().map_err(|_| JwtVerifyError::MalformedToken)?;

    let exp_for_cache = if validate_exp {
        let exp = state.exp.ok_or(JwtVerifyError::MissingExp)??;
        let skewed_exp = exp.saturating_add(clock_skew_secs);
        if now_epoch > skewed_exp {
            return Err(JwtVerifyError::TokenExpired);
        }
        skewed_exp
    } else {
        u64::MAX
    };

    if let Some(nbf_res) = state.nbf {
        let nbf = nbf_res?;
        if now_epoch.saturating_add(clock_skew_secs) < nbf {
            return Err(JwtVerifyError::TokenNotBefore);
        }
    }

    if expected_issuer.is_some() && state.iss_matched != Some(true) {
        return Err(JwtVerifyError::IssuerMismatch);
    }

    if expected_audience.is_some() && state.aud_matched != Some(true) {
        return Err(JwtVerifyError::AudienceMismatch);
    }

    let assertions_ok = if assert_claims.len() <= 64 {
        let required = if assert_claims.len() == 64 {
            u64::MAX
        } else {
            (1u64 << assert_claims.len()) - 1
        };
        (state.assert_mask & required) == required
    } else {
        state.assert_slots.iter().all(|&ok| ok)
    };
    if !assertions_ok {
        return Err(JwtVerifyError::ClaimAssertionFailed);
    }

    let injected_headers = if claims_to_headers.is_empty() {
        None
    } else {
        let mut pairs = Vec::with_capacity(claims_to_headers.len());
        for ((_, header_key), hdr_opt) in claims_to_headers.iter().zip(state.header_slots) {
            if let Some(hdr_val) = hdr_opt {
                pairs.push((header_key.clone(), hdr_val));
            }
        }
        if pairs.is_empty() {
            None
        } else {
            Some(Arc::from(pairs))
        }
    };

    Ok(EvaluatedJwtPayload {
        exp_epoch_secs: exp_for_cache,
        injected_headers,
    })
}

/// Zero-copy string key visitor avoiding allocation on unescaped JSON keys.
struct BorrowKeySeed;

impl<'de> DeserializeSeed<'de> for BorrowKeySeed {
    type Value = Cow<'de, str>;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Cow<'de, str>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON object key string")
            }

            #[inline]
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(Cow::Borrowed(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Cow::Owned(v.to_owned()))
            }

            #[inline]
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(Cow::Owned(v))
            }
        }
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct PayloadRootSeed<'a, 's> {
    state: &'a mut EvalState<'s>,
}

impl<'de, 'a, 's> DeserializeSeed<'de> for PayloadRootSeed<'a, 's> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'a, 's> Visitor<'de> for PayloadRootSeed<'a, 's> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JWT JSON payload")
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key_seed(BorrowKeySeed)? {
            let k = key.as_ref();
            let is_builtin = (self.state.validate_exp && k == "exp")
                || k == "nbf"
                || (self.state.expected_issuer.is_some() && k == "iss")
                || (self.state.expected_audience.is_some() && k == "aud");

            let any_hdr = has_prefix_or_terminal(self.state.claims_to_headers, &[], k);
            let any_assert = has_prefix_or_terminal(self.state.assert_claims, &[], k);

            if !is_builtin && !any_hdr && !any_assert {
                map.next_value::<IgnoredAny>()?;
                continue;
            }

            if is_builtin && !any_hdr && !any_assert {
                eval_root_builtin(k, self.state, &mut map)?;
                continue;
            }

            // Key participates in `claims_to_headers` / `assert_claims` (and possibly built-ins too)
            let key_buf = [k];
            map.next_value_seed(ScopedClaimSeed {
                state: self.state,
                prefix: &key_buf,
                at_root_builtin: is_builtin,
            })?;
        }
        Ok(())
    }

    // Valid non-object JSON still drains cleanly with zero matched claims (mirrors Value behavior).
    fn visit_bool<E>(self, _v: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E>(self, _v: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E>(self, _v: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E>(self, _v: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E>(self, _v: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(())
    }
}

#[inline]
fn eval_root_builtin<'de, A: MapAccess<'de>>(
    k: &str,
    state: &mut EvalState<'_>,
    map: &mut A,
) -> Result<(), A::Error> {
    match k {
        "exp" => {
            state.exp = map.next_value_seed(NumericDateSeed { ceil: false })?;
        }
        "nbf" => {
            state.nbf = map.next_value_seed(NumericDateSeed { ceil: true })?;
        }
        "iss" => {
            let expected = state.expected_issuer.unwrap_or("");
            state.iss_matched = Some(map.next_value_seed(ExactStrMatchSeed { expected })?);
        }
        "aud" => {
            let expected = state.expected_audience.unwrap_or("");
            state.aud_matched = Some(map.next_value_seed(AudMatchSeed { expected })?);
        }
        _ => {
            map.next_value::<IgnoredAny>()?;
        }
    }
    Ok(())
}

trait HasClaimPath {
    fn claim_path(&self) -> &ClaimPath;
}

impl<V> HasClaimPath for (ClaimPath, V) {
    #[inline(always)]
    fn claim_path(&self) -> &ClaimPath {
        &self.0
    }
}

#[inline]
fn matches_prefix(path: &ClaimPath, prefix: &[&str]) -> bool {
    let segs = path.segments();
    segs.len() >= prefix.len()
        && segs
            .iter()
            .zip(prefix.iter())
            .all(|(seg, p)| seg.as_ref() == *p)
}

#[inline]
fn has_prefix_or_terminal<T: HasClaimPath>(items: &[T], prefix: &[&str], next_seg: &str) -> bool {
    items.iter().any(|item| {
        let segs = item.claim_path().segments();
        segs.len() > prefix.len()
            && segs[prefix.len()].as_ref() == next_seg
            && segs[..prefix.len()]
                .iter()
                .zip(prefix.iter())
                .all(|(s, p)| s.as_ref() == *p)
    })
}

/// Captures targeted nested/terminal claims along `prefix` cleanly on the stack.
impl EvalState<'_> {
    #[inline(always)]
    fn set_assert(&mut self, idx: usize, ok: bool) {
        if self.assert_claims.len() <= 64 {
            if ok {
                self.assert_mask |= 1u64 << idx;
            } else {
                self.assert_mask &= !(1u64 << idx);
            }
        } else {
            self.assert_slots[idx] = ok;
        }
    }
}

struct ScopedClaimSeed<'a, 's, 'p> {
    state: &'a mut EvalState<'s>,
    prefix: &'p [&'p str],
    at_root_builtin: bool,
}

impl<'a, 's, 'p> ScopedClaimSeed<'a, 's, 'p> {
    fn has_exact_hdr_or_assert(&self) -> (bool, bool) {
        let d = self.prefix.len();
        let hdr = self
            .state
            .claims_to_headers
            .iter()
            .any(|(p, _)| p.segments().len() == d && matches_prefix(p, self.prefix));
        let asrt = self
            .state
            .assert_claims
            .iter()
            .any(|(p, _)| p.segments().len() == d && matches_prefix(p, self.prefix));
        (hdr, asrt)
    }

    fn apply_scalar_str(&mut self, s: &str) {
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" => self.state.exp = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "nbf" => self.state.nbf = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "iss" => {
                    self.state.iss_matched =
                        Some(self.state.expected_issuer.is_some_and(|exp| exp == s));
                }
                "aud" => {
                    self.state.aud_matched =
                        Some(self.state.expected_audience.is_some_and(|exp| exp == s));
                }
                _ => {}
            }
        }
        for (idx, (path, expected)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                let matched = expected.as_ref() == s;
                self.state.set_assert(idx, matched);
            }
        }
        let mut cached_hdr: Option<HeaderValue> = None;
        for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                if cached_hdr.is_none() {
                    cached_hdr = HeaderValue::try_from(s).ok();
                }
                self.state.header_slots[idx] = cached_hdr.clone();
            }
        }
    }

    fn clear_exact_targets(&mut self) {
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" => self.state.exp = None,
                "nbf" => self.state.nbf = None,
                "iss" => self.state.iss_matched = Some(false),
                "aud" => self.state.aud_matched = Some(false),
                _ => {}
            }
        }
        for (idx, (path, _)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                self.state.set_assert(idx, false);
            }
        }
        for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                self.state.header_slots[idx] = None;
            }
        }
    }

    fn apply_number_u64(&mut self, n: u64) {
        let mut buf = [0u8; 20];
        let ascii = format_u64_ascii(n, &mut buf);
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" | "nbf" => {
                    let slot = if self.prefix[0] == "exp" {
                        &mut self.state.exp
                    } else {
                        &mut self.state.nbf
                    };
                    *slot = Some(Ok(n));
                }
                "iss" => self.state.iss_matched = Some(false),
                "aud" => self.state.aud_matched = Some(false),
                _ => {}
            }
        }
        for (idx, (path, expected)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                let matched = expected.as_bytes() == ascii;
                self.state.set_assert(idx, matched);
            }
        }
        let mut cached_hdr: Option<HeaderValue> = None;
        for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                if cached_hdr.is_none() {
                    cached_hdr = HeaderValue::from_bytes(ascii).ok();
                }
                self.state.header_slots[idx] = cached_hdr.clone();
            }
        }
    }

    fn apply_number_i64(&mut self, n: i64) {
        let mut buf = [0u8; 21];
        let ascii = format_i64_ascii(n, &mut buf);
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" | "nbf" => {
                    let clamped = if n <= 0 { 0 } else { n as u64 };
                    let slot = if self.prefix[0] == "exp" {
                        &mut self.state.exp
                    } else {
                        &mut self.state.nbf
                    };
                    *slot = Some(Ok(clamped));
                }
                "iss" => self.state.iss_matched = Some(false),
                "aud" => self.state.aud_matched = Some(false),
                _ => {}
            }
        }
        for (idx, (path, expected)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                let matched = expected.as_bytes() == ascii;
                self.state.set_assert(idx, matched);
            }
        }
        let mut cached_hdr: Option<HeaderValue> = None;
        for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                if cached_hdr.is_none() {
                    cached_hdr = HeaderValue::from_bytes(ascii).ok();
                }
                self.state.header_slots[idx] = cached_hdr.clone();
            }
        }
    }

    fn apply_number_f64(&mut self, f: f64) {
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" => self.state.exp = Some(numeric_date_from_f64(f, false)),
                "nbf" => self.state.nbf = Some(numeric_date_from_f64(f, true)),
                "iss" => self.state.iss_matched = Some(false),
                "aud" => self.state.aud_matched = Some(false),
                _ => {}
            }
        }
        if let Some(num) = serde_json::Number::from_f64(f) {
            let s = num.to_string();
            for (idx, (path, expected)) in self.state.assert_claims.iter().enumerate() {
                if path.segments().len() == d && matches_prefix(path, self.prefix) {
                    let matched = expected.as_ref() == s;
                    self.state.set_assert(idx, matched);
                }
            }
            let mut cached_hdr: Option<HeaderValue> = None;
            for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
                if path.segments().len() == d && matches_prefix(path, self.prefix) {
                    if cached_hdr.is_none() {
                        cached_hdr = HeaderValue::try_from(s.as_str()).ok();
                    }
                    self.state.header_slots[idx] = cached_hdr.clone();
                }
            }
        } else {
            self.clear_exact_targets();
        }
    }
}

impl<'de, 'a, 's, 'p> DeserializeSeed<'de> for ScopedClaimSeed<'a, 's, 'p> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'a, 's, 'p> Visitor<'de> for ScopedClaimSeed<'a, 's, 'p> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON claim value")
    }

    #[inline]
    fn visit_bool<E>(self, v: bool) -> Result<(), E> {
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" => self.state.exp = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "nbf" => self.state.nbf = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "iss" => self.state.iss_matched = Some(false),
                "aud" => self.state.aud_matched = Some(false),
                _ => {}
            }
        }
        let lit = if v { "true" } else { "false" };
        for (idx, (path, expected)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                let matched = expected.as_ref() == lit;
                self.state.set_assert(idx, matched);
            }
        }
        for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                self.state.header_slots[idx] = Some(HeaderValue::from_static(lit));
            }
        }
        Ok(())
    }

    #[inline]
    fn visit_i64<E>(mut self, v: i64) -> Result<(), E> {
        self.apply_number_i64(v);
        Ok(())
    }

    #[inline]
    fn visit_u64<E>(mut self, v: u64) -> Result<(), E> {
        self.apply_number_u64(v);
        Ok(())
    }

    #[inline]
    fn visit_f64<E>(mut self, v: f64) -> Result<(), E> {
        self.apply_number_f64(v);
        Ok(())
    }

    #[inline]
    fn visit_str<E>(mut self, v: &str) -> Result<(), E> {
        self.apply_scalar_str(v);
        Ok(())
    }

    #[inline]
    fn visit_unit<E>(mut self) -> Result<(), E> {
        self.clear_exact_targets();
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        let d = self.prefix.len();
        let (need_hdr, need_assert) = self.has_exact_hdr_or_assert();
        let need_aud = self.at_root_builtin
            && d == 1
            && self.state.expected_audience.is_some()
            && self.prefix[0] == "aud";

        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" => self.state.exp = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "nbf" => self.state.nbf = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "iss" => self.state.iss_matched = Some(false),
                _ => {}
            }
        }

        if !need_hdr && !need_assert && !need_aud {
            while seq.next_element::<IgnoredAny>()?.is_some() {}
            return Ok(());
        }

        let mut aud_ok = false;
        let expected_aud = self.state.expected_audience.unwrap_or("");
        let mut hdr_buf = String::new();

        // Clear any prior assertion matches before reading seq elements (duplicate key reset).
        for (idx, (path, _)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                self.state.set_assert(idx, false);
            }
        }

        // Track assert outcomes per matching assert slot across elements without heap Vec<String>.
        while let Some(()) = seq.next_element_seed(SeqItemSeed {
            need_hdr,
            hdr_buf: &mut hdr_buf,
            need_aud,
            expected_aud,
            aud_ok: &mut aud_ok,
            state: self.state,
            prefix: self.prefix,
        })? {}

        if need_aud {
            self.state.aud_matched = Some(aud_ok);
        }
        if need_hdr {
            let hdr_val = if hdr_buf.is_empty() {
                None
            } else {
                HeaderValue::try_from(hdr_buf).ok()
            };
            for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
                if path.segments().len() == d && matches_prefix(path, self.prefix) {
                    self.state.header_slots[idx] = hdr_val.clone();
                }
            }
        }
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let d = self.prefix.len();
        if self.at_root_builtin && d == 1 {
            match self.prefix[0] {
                "exp" => self.state.exp = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "nbf" => self.state.nbf = Some(Err(JwtVerifyError::InvalidNumericDate)),
                "iss" => self.state.iss_matched = Some(false),
                "aud" => self.state.aud_matched = Some(false),
                _ => {}
            }
        }
        // Reset any exact claim match at this object node (objects aren't scalars/headers)
        for (idx, (path, _)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                self.state.set_assert(idx, false);
            }
        }
        for (idx, (path, _)) in self.state.claims_to_headers.iter().enumerate() {
            if path.segments().len() == d && matches_prefix(path, self.prefix) {
                self.state.header_slots[idx] = None;
            }
        }

        while let Some(key) = map.next_key_seed(BorrowKeySeed)? {
            let k = key.as_ref();
            let has_nested = has_prefix_or_terminal(self.state.claims_to_headers, self.prefix, k)
                || has_prefix_or_terminal(self.state.assert_claims, self.prefix, k);
            if !has_nested {
                map.next_value::<IgnoredAny>()?;
                continue;
            }
            if self.prefix.len() < 8 {
                let mut stack_prefix = [""; 9];
                stack_prefix[..self.prefix.len()].copy_from_slice(self.prefix);
                stack_prefix[self.prefix.len()] = k;
                map.next_value_seed(ScopedClaimSeed {
                    state: self.state,
                    prefix: &stack_prefix[..=self.prefix.len()],
                    at_root_builtin: false,
                })?;
            } else {
                let mut next_prefix = Vec::with_capacity(self.prefix.len() + 1);
                next_prefix.extend_from_slice(self.prefix);
                next_prefix.push(k);
                map.next_value_seed(ScopedClaimSeed {
                    state: self.state,
                    prefix: &next_prefix,
                    at_root_builtin: false,
                })?;
            }
        }
        Ok(())
    }
}

struct SeqItemSeed<'a, 's, 'p, 'b> {
    need_hdr: bool,
    hdr_buf: &'b mut String,
    need_aud: bool,
    expected_aud: &'b str,
    aud_ok: &'b mut bool,
    state: &'a mut EvalState<'s>,
    prefix: &'p [&'p str],
}

impl<'a, 's, 'p, 'b> SeqItemSeed<'a, 's, 'p, 'b> {
    fn check_assert_scalar(&mut self, val: &str) {
        let d = self.prefix.len();
        for (idx, (path, expected)) in self.state.assert_claims.iter().enumerate() {
            if path.segments().len() == d
                && matches_prefix(path, self.prefix)
                && expected.as_ref() == val
            {
                self.state.set_assert(idx, true);
            }
        }
    }
}

impl<'de, 'a, 's, 'p, 'b> DeserializeSeed<'de> for SeqItemSeed<'a, 's, 'p, 'b> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'a, 's, 'p, 'b> Visitor<'de> for SeqItemSeed<'a, 's, 'p, 'b> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON array item")
    }

    fn visit_str<E>(mut self, v: &str) -> Result<(), E> {
        if self.need_aud && v == self.expected_aud {
            *self.aud_ok = true;
        }
        if self.need_hdr {
            if !self.hdr_buf.is_empty() {
                self.hdr_buf.push_str(", ");
            }
            self.hdr_buf.push_str(v);
        }
        self.check_assert_scalar(v);
        Ok(())
    }

    fn visit_u64<E>(mut self, v: u64) -> Result<(), E> {
        let mut buf = [0u8; 20];
        let s = std::str::from_utf8(format_u64_ascii(v, &mut buf)).unwrap_or("");
        if self.need_hdr {
            if !self.hdr_buf.is_empty() {
                self.hdr_buf.push_str(", ");
            }
            self.hdr_buf.push_str(s);
        }
        self.check_assert_scalar(s);
        Ok(())
    }

    fn visit_i64<E>(mut self, v: i64) -> Result<(), E> {
        let mut buf = [0u8; 21];
        let s = std::str::from_utf8(format_i64_ascii(v, &mut buf)).unwrap_or("");
        if self.need_hdr {
            if !self.hdr_buf.is_empty() {
                self.hdr_buf.push_str(", ");
            }
            self.hdr_buf.push_str(s);
        }
        self.check_assert_scalar(s);
        Ok(())
    }

    fn visit_f64<E>(mut self, v: f64) -> Result<(), E> {
        if let Some(num) = serde_json::Number::from_f64(v) {
            let s = num.to_string();
            if self.need_hdr {
                if !self.hdr_buf.is_empty() {
                    self.hdr_buf.push_str(", ");
                }
                self.hdr_buf.push_str(&s);
            }
            self.check_assert_scalar(&s);
        }
        Ok(())
    }

    fn visit_bool<E>(mut self, v: bool) -> Result<(), E> {
        // Matches `value_to_header_string`: booleans inside arrays skip headers, still match assertions.
        self.check_assert_scalar(if v { "true" } else { "false" });
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(())
    }
}

// ============================================================================
// Focused Field Seeds (Zero-Alloc Root Built-ins)
// ============================================================================

#[inline]
fn numeric_date_from_f64(f: f64, ceil: bool) -> Result<u64, JwtVerifyError> {
    if !f.is_finite() {
        return Err(JwtVerifyError::InvalidNumericDate);
    }
    if f <= 0.0 {
        return Ok(0);
    }
    if f >= u64::MAX as f64 {
        return Ok(u64::MAX);
    }
    Ok(if ceil {
        f.ceil() as u64
    } else {
        f.floor() as u64
    })
}

struct NumericDateSeed {
    ceil: bool,
}

impl<'de> DeserializeSeed<'de> for NumericDateSeed {
    type Value = Option<Result<u64, JwtVerifyError>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for NumericDateSeed {
    type Value = Option<Result<u64, JwtVerifyError>>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RFC 7519 NumericDate")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
        Ok(Some(Ok(v)))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
        Ok(Some(Ok(if v <= 0 { 0 } else { v as u64 })))
    }
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
        Ok(Some(numeric_date_from_f64(v, self.ceil)))
    }
    fn visit_bool<E>(self, _v: bool) -> Result<Self::Value, E> {
        Ok(Some(Err(JwtVerifyError::InvalidNumericDate)))
    }
    fn visit_str<E>(self, _v: &str) -> Result<Self::Value, E> {
        Ok(Some(Err(JwtVerifyError::InvalidNumericDate)))
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Some(Err(JwtVerifyError::InvalidNumericDate)))
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(Some(Err(JwtVerifyError::InvalidNumericDate)))
    }
}

struct ExactStrMatchSeed<'a> {
    expected: &'a str,
}

impl<'de, 'a> DeserializeSeed<'de> for ExactStrMatchSeed<'a> {
    type Value = bool;

    fn deserialize<D>(self, deserializer: D) -> Result<bool, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'a> Visitor<'de> for ExactStrMatchSeed<'a> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON string")
    }

    fn visit_str<E>(self, v: &str) -> Result<bool, E> {
        Ok(v == self.expected)
    }
    fn visit_bool<E>(self, _v: bool) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_i64<E>(self, _v: i64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_u64<E>(self, _v: u64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_f64<E>(self, _v: f64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_unit<E>(self) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<bool, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(false)
    }
    fn visit_map<A>(self, mut map: A) -> Result<bool, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(false)
    }
}

struct AudMatchSeed<'a> {
    expected: &'a str,
}

impl<'de, 'a> DeserializeSeed<'de> for AudMatchSeed<'a> {
    type Value = bool;

    fn deserialize<D>(self, deserializer: D) -> Result<bool, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'a> Visitor<'de> for AudMatchSeed<'a> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JWT aud string or array")
    }

    fn visit_str<E>(self, v: &str) -> Result<bool, E> {
        Ok(v == self.expected)
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<bool, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut hit = false;
        while let Some(elem_hit) = seq.next_element_seed(ExactStrMatchSeed {
            expected: self.expected,
        })? {
            if elem_hit {
                hit = true;
            }
        }
        Ok(hit)
    }
    fn visit_bool<E>(self, _v: bool) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_i64<E>(self, _v: i64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_u64<E>(self, _v: u64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_f64<E>(self, _v: f64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_unit<E>(self) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_map<A>(self, mut map: A) -> Result<bool, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderName;

    #[test]
    fn test_streaming_skips_ignored_and_resolves_nested_escaped_claims() {
        let payload = br#"{
            "sub": "user-1",
            "realm_access": { "roles": ["uma", "offline", "unused"] },
            "resource_access": { "account": { "roles": ["manage-account", "view-profile"] } },
            "\u0069ss": "https://kc.local/realms/master",
            "aud": ["other-service", "velda-edge"],
            "exp": 2000000000,
            "nbf": 1700000000.2,
            "profile": { "\u0074enant_id": "corp-\u00399", "active": true },
            "roles": ["admin", 42, true, "editor"]
        }"#;

        let claims_to_headers = vec![
            (
                ClaimPath::compile("profile.tenant_id"),
                HeaderKey::from_name(HeaderName::from_static("x-tenant-id")),
            ),
            (
                ClaimPath::compile("roles"),
                HeaderKey::from_name(HeaderName::from_static("x-roles")),
            ),
            (
                ClaimPath::compile("profile.active"),
                HeaderKey::from_name(HeaderName::from_static("x-active")),
            ),
        ];
        let assert_claims = vec![
            (ClaimPath::compile("roles"), "admin".into()),
            (ClaimPath::compile("roles"), "42".into()),
            (ClaimPath::compile("roles"), "true".into()),
            (ClaimPath::compile("profile.tenant_id"), "corp-99".into()),
        ];

        let res = eval_jwt_payload_streaming(
            payload,
            true,
            60,
            Some("https://kc.local/realms/master"),
            Some("velda-edge"),
            &claims_to_headers,
            &assert_claims,
            1700000001,
        )
        .expect("valid streaming evaluation");

        assert_eq!(res.exp_epoch_secs, 2000000060);
        let hdrs = res.injected_headers.unwrap();
        assert_eq!(hdrs.len(), 3);
        assert_eq!(hdrs[0].1, "corp-99");
        assert_eq!(hdrs[1].1, "admin, 42, editor");
        assert_eq!(hdrs[2].1, "true");
    }

    #[test]
    fn test_streaming_duplicate_keys_trailing_syntax_and_non_object_parity() {
        // Duplicate keys: last key wins for exp & assertions
        let dup_expired = br#"{"exp": 2000000000, "exp": 100}"#;
        assert_eq!(
            eval_jwt_payload_streaming(dup_expired, true, 0, None, None, &[], &[], 1700000000)
                .err(),
            Some(JwtVerifyError::TokenExpired)
        );

        // Trailing garbage or broken array elements must be caught even after an earlier match
        let broken_arr = br#"{"roles":["admin", bad],"exp":2000000000}"#;
        let asserts = vec![(ClaimPath::compile("roles"), "admin".into())];
        assert_eq!(
            eval_jwt_payload_streaming(broken_arr, true, 0, None, None, &[], &asserts, 1000).err(),
            Some(JwtVerifyError::MalformedToken)
        );

        let trailing = br#"{"exp":2000000000} trailing"#;
        assert_eq!(
            eval_jwt_payload_streaming(trailing, true, 0, None, None, &[], &[], 1000).err(),
            Some(JwtVerifyError::MalformedToken)
        );

        // Non-object JSON drains cleanly with MissingExp when validate_exp == true
        assert_eq!(
            eval_jwt_payload_streaming(br#"[1, 2, 3]"#, true, 0, None, None, &[], &[], 1000).err(),
            Some(JwtVerifyError::MissingExp)
        );
    }
}
