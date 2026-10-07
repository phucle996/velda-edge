//! Downstream HTTP/1.1 URI Path Normalization and Security Inspection (RFC 9112 / RFC 3986).
//!
//! Enforces:
//! - Rejection of prohibited null bytes (`\0`).
//! - Prevention of root directory breakout attacks (`/../`).
//! - In-place fast-path validation for clean paths.
//! - Canonicalization of backslashes, percent-encoded separators (`%2F`, `%2E`), and redundant slashes.

use http::Uri;

use crate::error::Http1Error;

/// Normalizes an HTTP/1.1 URI path in-place according to RFC 9112 and RFC 3986.
///
/// Preserves query parameters while stripping path traversals (`/../`),
/// duplicate slashes (`//`), backslashes (`\`), and percent-encoded separators (`%2F`, `%2E`).
pub fn normalize_path(uri: &mut Uri) -> Result<(), Http1Error> {
    let path = uri.path();
    if is_clean_path(path.as_bytes()) {
        return Ok(());
    }

    let normalized = normalize_path_cold(path)?;
    let mut parts = uri.clone().into_parts();
    let new_pq = match parts.path_and_query {
        Some(ref pq) => {
            if let Some(q) = pq.query() {
                format!("{normalized}?{q}")
                    .parse::<http::uri::PathAndQuery>()
                    .map_err(|e| Http1Error::InvalidUri(e.to_string()))?
            } else {
                normalized
                    .parse::<http::uri::PathAndQuery>()
                    .map_err(|e| Http1Error::InvalidUri(e.to_string()))?
            }
        }
        None => normalized
            .parse::<http::uri::PathAndQuery>()
            .map_err(|e| Http1Error::InvalidUri(e.to_string()))?,
    };
    parts.path_and_query = Some(new_pq);
    *uri = http::Uri::from_parts(parts).map_err(|e| Http1Error::InvalidUri(e.to_string()))?;

    Ok(())
}

/// Fast-path check returning `true` if path contains no traversals, slashes, backslashes, percent encodings, or nulls.
#[inline(always)]
pub fn is_clean_path(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes[0] != b'/' {
        return false;
    }
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'/' {
            if i + 1 < bytes.len() {
                let next = bytes[i + 1];
                if next == b'/' || next == b'.' {
                    return false;
                }
            }
        } else if b == b'\\' || b == b'%' || b == 0 {
            return false;
        }
        i += 1;
    }
    true
}

#[inline(never)]
fn normalize_path_cold(path: &str) -> Result<String, Http1Error> {
    let bytes = path.as_bytes();
    if bytes.contains(&b'\0') {
        return Err(Http1Error::InvalidUri(
            "Prohibited null byte in URI path".into(),
        ));
    }

    let mut decoded = String::with_capacity(bytes.len() + 1);
    if !path.starts_with('/') {
        decoded.push('/');
    }

    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            decoded.push('/');
            i += 1;
        } else if b == b'%' && i + 2 < bytes.len() {
            let h1 = bytes[i + 1];
            let h2 = bytes[i + 2];
            match (hex_val(h1), hex_val(h2)) {
                (Some(v1), Some(v2)) => {
                    let byte = (v1 << 4) | v2;
                    if byte == 0 {
                        return Err(Http1Error::InvalidUri(
                            "Prohibited null byte in URI path".into(),
                        ));
                    }
                    match byte {
                        b'.' => decoded.push('.'),
                        b'/' | b'\\' => decoded.push('/'),
                        _ => {
                            decoded.push('%');
                            decoded.push(h1 as char);
                            decoded.push(h2 as char);
                        }
                    }
                    i += 3;
                }
                _ => {
                    decoded.push('%');
                    i += 1;
                }
            }
        } else {
            decoded.push(b as char);
            i += 1;
        }
    }

    let mut stack: Vec<&str> = Vec::new();
    for seg in decoded.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if stack.pop().is_none() {
                    return Err(Http1Error::InvalidUri(
                        "Root breakout attempt in URI path".into(),
                    ));
                }
            }
            s => stack.push(s),
        }
    }

    if stack.is_empty() {
        Ok("/".into())
    } else {
        let mut res = String::with_capacity(decoded.len());
        for s in stack {
            res.push('/');
            res.push_str(s);
        }
        Ok(res)
    }
}

#[inline(always)]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_clean_path() {
        let mut uri: Uri = "/api/v1/users".parse().unwrap();
        assert!(normalize_path(&mut uri).is_ok());
        assert_eq!(uri.path(), "/api/v1/users");
    }

    #[test]
    fn test_normalize_traversal_path() {
        let mut uri: Uri = "/api/v1/../v2/users".parse().unwrap();
        assert!(normalize_path(&mut uri).is_ok());
        assert_eq!(uri.path(), "/api/v2/users");
    }

    #[test]
    fn test_normalize_percent_encoded_and_slashes() {
        let mut uri: Uri = "/api//v1/%2e%2e/v2/users?page=1".parse().unwrap();
        assert!(normalize_path(&mut uri).is_ok());
        assert_eq!(uri.path(), "/api/v2/users");
        assert_eq!(uri.query(), Some("page=1"));
    }

    #[test]
    fn test_normalize_root_breakout_rejected() {
        let mut uri: Uri = "/../../etc/passwd".parse().unwrap();
        assert!(normalize_path(&mut uri).is_err());
    }

    #[test]
    fn test_normalize_null_byte_rejected() {
        let mut uri: Uri = "/api/v1/%00/admin".parse().unwrap();
        assert!(normalize_path(&mut uri).is_err());
    }
}
