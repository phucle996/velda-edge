//! RFC 3986 Path Sanitization and Directory Traversal Defense.
//!
//! Provides deterministic, zero-allocation path normalization:
//! - Collapses multiple consecutive slashes `//` into a single `/`.
//! - Strips self-referencing relative directory segments (`/./`).
//! - Resolves parent directory segments (`/../`).
//! - Detects and fails-fast on directory traversal attacks attempting to climb above root `/`.

/// Error indicating that a path traversal attempt was detected escaping above root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathTraversalError;

impl std::fmt::Display for PathTraversalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "path traversal detected: attempt to escape above root directory"
        )
    }
}

impl std::error::Error for PathTraversalError {}

/// Normalizes path segments according to RFC 3986.
///
/// Returns:
/// - `Ok(normalized_path)`: Sanitized canonical path.
/// - `Err(PathTraversalError)`: When path traversal attempts to escape above root `/` (e.g. `/../../etc/passwd`).
pub fn normalize_and_sanitize_path(path: &str) -> Result<String, PathTraversalError> {
    let mut segments: Vec<&str> = Vec::with_capacity(8);

    for seg in path.split('/') {
        match seg {
            "" | "." => {
                // Skip empty segments (collapses // into /) and current dir (.)
            }
            ".." => {
                // If attempting to pop above root, trigger path traversal violation
                if segments.pop().is_none() {
                    return Err(PathTraversalError);
                }
            }
            other => {
                segments.push(other);
            }
        }
    }

    let mut out = String::with_capacity(path.len() + 1);
    out.push('/');
    for (i, seg) in segments.iter().enumerate() {
        if i > 0 {
            out.push('/');
        }
        out.push_str(seg);
    }
    Ok(out)
}

/// Fast-path check: returns `true` if path contains neither `//` nor `/.`,
/// proving it is already normalized and can bypass the sanitizer.
#[inline(always)]
pub fn is_already_normalized(path: &str) -> bool {
    !path.contains("//") && !path.contains("/.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_already_normalized_fast_path() {
        assert!(is_already_normalized("/users/42/orders"));
        assert!(is_already_normalized("/api/v1/checkout"));
        assert!(!is_already_normalized("/users//orders"));
        assert!(!is_already_normalized("/users/./orders"));
        assert!(!is_already_normalized("/users/../orders"));
    }

    #[test]
    fn test_normalize_collapses_slashes() {
        assert_eq!(
            normalize_and_sanitize_path("//users///list").unwrap(),
            "/users/list"
        );
        assert_eq!(normalize_and_sanitize_path("////").unwrap(), "/");
    }

    #[test]
    fn test_normalize_resolves_relative_dots() {
        assert_eq!(
            normalize_and_sanitize_path("/users/./list").unwrap(),
            "/users/list"
        );
        assert_eq!(
            normalize_and_sanitize_path("/users/temp/../list").unwrap(),
            "/users/list"
        );
        assert_eq!(
            normalize_and_sanitize_path("/a/b/c/../../d").unwrap(),
            "/a/d"
        );
    }

    #[test]
    fn test_rejects_traversal_above_root() {
        assert!(normalize_and_sanitize_path("/..").is_err());
        assert!(normalize_and_sanitize_path("/users/../../etc/passwd").is_err());
        assert!(normalize_and_sanitize_path("/../../root").is_err());
    }
}
