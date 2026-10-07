//! Downstream HTTP/3 URI Path Normalization and Security Inspection (RFC 9114 / RFC 3986).

use http::Uri;

use crate::error::Http3Error;

/// Normalizes an HTTP/3 URI path in-place according to RFC 9114 and RFC 3986.
#[inline]
pub fn normalize_path(uri: &mut Uri) -> Result<(), Http3Error> {
    let path = uri.path();
    if is_clean_path(path.as_bytes()) {
        return Ok(());
    }

    let normalized = normalize_path_cold(path)?;
    let mut parts = uri.clone().into_parts();
    let new_pq = match parts.path_and_query {
        Some(ref pq) => {
            if let Some(q) = pq.query() {
                format!("{normalized}?{q}").parse::<http::uri::PathAndQuery>()
            } else {
                normalized.parse::<http::uri::PathAndQuery>()
            }
        }
        None => normalized.parse::<http::uri::PathAndQuery>(),
    }
    .map_err(|e| Http3Error::InvalidPath(e.to_string()))?;

    parts.path_and_query = Some(new_pq);
    *uri = Uri::from_parts(parts).map_err(|e| Http3Error::InvalidPath(e.to_string()))?;
    Ok(())
}

/// Zero-allocation, single-pass validator for clean HTTP/3 paths.
/// Returns true if path begins with '/', has no double-slashes, no backslashes,
/// no dot-segments ('.', '..'), no '%', and no null-bytes.
#[inline(always)]
pub fn is_clean_path(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes[0] != b'/' {
        return false;
    }
    let mut bad_char_mask: u8 = 0;
    let mut bad_slash_mask: u8 = 0;
    let mut prev = b'/';

    for &b in &bytes[1..] {
        bad_char_mask |= ((b == 0) | (b == b'\\') | (b == b'%')) as u8;
        bad_slash_mask |= ((prev == b'/') & ((b == b'/') | (b == b'.'))) as u8;
        prev = b;
    }

    (bad_char_mask | bad_slash_mask) == 0
}

#[cold]
#[inline(never)]
pub fn normalize_path_cold(path: &str) -> Result<String, Http3Error> {
    let bytes = path.as_bytes();
    if bytes.contains(&b'\0') {
        return Err(Http3Error::InvalidPath(
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
                        return Err(Http3Error::InvalidPath(
                            "Prohibited null byte in URI path".into(),
                        ));
                    }
                    match byte {
                        b'.' => decoded.push('.'),
                        b'/' | b'\\' => decoded.push('/'),
                        other => {
                            decoded.push('%');
                            decoded.push(h1 as char);
                            decoded.push(h2 as char);
                            let _ = other;
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

    let mut segments: Vec<&str> = Vec::with_capacity(8);
    for seg in decoded.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if segments.pop().is_none() {
                    return Err(Http3Error::InvalidPath(
                        "Root breakout attempt in URI path".into(),
                    ));
                }
            }
            s => segments.push(s),
        }
    }

    let mut result = String::with_capacity(decoded.len());
    if segments.is_empty() {
        result.push('/');
    } else {
        for seg in segments {
            result.push('/');
            result.push_str(seg);
        }
    }

    Ok(result)
}

#[inline(always)]
pub const fn hex_val(b: u8) -> Option<u8> {
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
    fn test_normalize_null_byte_rejected() {
        let mut uri: Uri = "/api/v1/%00/users".parse().unwrap();
        assert!(normalize_path(&mut uri).is_err());
    }

    #[test]
    fn test_normalize_root_breakout_rejected() {
        let mut uri: Uri = "/../../etc/passwd".parse().unwrap();
        assert!(normalize_path(&mut uri).is_err());
    }
}
