//! Pre-Compiled Claim Projection, Assertions, and RFC 7519 NumericDate Evaluators (`jwt_auth::claims`).
//!
//! ```text
//!   JWT JSON Payload
//!     ├──► parse_numeric_date_floor / ceil (`exp` / `nbf` finite-float safe)
//!     ├──► ClaimPath::walk() ──► matches_compiled_assert() (`assert_claims` -> 403)
//!     └──► ClaimPath::walk() ──► value_to_header_string() (`claims_to_headers`)
//! ```

use std::borrow::Cow;

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
}

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
fn scalar_matches_str(val: &serde_json::Value, expected: &str) -> bool {
    match val {
        serde_json::Value::String(s) => s == expected,
        serde_json::Value::Bool(b) => expected == if *b { "true" } else { "false" },
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                let mut buf = [0u8; 20];
                format_u64_ascii(u, &mut buf) == expected.as_bytes()
            } else if let Some(i) = n.as_i64() {
                let mut buf = [0u8; 21];
                format_i64_ascii(i, &mut buf) == expected.as_bytes()
            } else {
                n.to_string() == expected
            }
        }
        _ => false,
    }
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

#[inline]
pub fn matches_compiled_assert(
    payload: &serde_json::Value,
    path: &ClaimPath,
    expected: &str,
) -> bool {
    let Some(current) = path.walk(payload) else {
        return false;
    };
    match current {
        serde_json::Value::Array(arr) => arr.iter().any(|item| scalar_matches_str(item, expected)),
        other => scalar_matches_str(other, expected),
    }
}

#[inline]
pub fn parse_numeric_date_floor(val: &serde_json::Value) -> Result<u64, JwtVerifyError> {
    let Some(n) = val.as_number() else {
        return Err(JwtVerifyError::InvalidNumericDate);
    };
    if let Some(u) = n.as_u64() {
        return Ok(u);
    }
    if let Some(i) = n.as_i64() {
        return Ok(if i <= 0 { 0 } else { i as u64 });
    }
    if let Some(f) = n.as_f64()
        && f.is_finite()
    {
        return Ok(if f <= 0.0 {
            0
        } else if f >= u64::MAX as f64 {
            u64::MAX
        } else {
            f.floor() as u64
        });
    }
    Err(JwtVerifyError::InvalidNumericDate)
}

#[inline]
pub fn parse_numeric_date_ceil(val: &serde_json::Value) -> Result<u64, JwtVerifyError> {
    let Some(n) = val.as_number() else {
        return Err(JwtVerifyError::InvalidNumericDate);
    };
    if let Some(u) = n.as_u64() {
        return Ok(u);
    }
    if let Some(i) = n.as_i64() {
        return Ok(if i <= 0 { 0 } else { i as u64 });
    }
    if let Some(f) = n.as_f64()
        && f.is_finite()
    {
        return Ok(if f <= 0.0 {
            0
        } else if f >= u64::MAX as f64 {
            u64::MAX
        } else {
            f.ceil() as u64
        });
    }
    Err(JwtVerifyError::InvalidNumericDate)
}
