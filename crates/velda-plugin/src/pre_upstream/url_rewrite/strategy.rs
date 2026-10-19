//! URL Transformation Strategies and Pre-Compiled Execution Plans.
//!
//! Provides optimized path and query string transformation mechanisms:
//! - Multi-level path strategies: `StripPrefix`, `ReplacePrefix`, `Exact`, `Regex`.
//! - Pre-tokenized replacement templates (`ReplacementToken`) preventing repeated regex template parsing.
//! - Static literal prefix extraction for SIMD-accelerated fast-rejection before DFA execution.
//! - Query string strategies: `Keep`, `Drop`, `Append`.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Strategy for rewriting the request path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PathRewriteStrategy {
    /// Strips leading prefix from path (e.g., `/api/v1/users` -> `/users`).
    StripPrefix { prefix: String },
    /// Replaces matched prefix with another prefix (e.g., `/api/v1/users` -> `/internal/v1/users`).
    ReplacePrefix { prefix: String, replacement: String },
    /// Exact match replacement (e.g., `/legacy-health` -> `/actuator/healthz`).
    Exact { path: String, replacement: String },
    /// Regex match and substitution using capture groups (e.g., `^/service/(.*)$` -> `/$1`).
    Regex {
        pattern: String,
        replacement: String,
    },
}

/// Strategy for handling request query strings during URL rewrite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum QueryStrategy {
    /// Preserves client query string untouched (default).
    #[default]
    Keep,
    /// Drops/strips all query parameters from the request.
    Drop,
    /// Appends static query parameters to existing query (or sets if query is empty).
    Append { query: String },
}

impl QueryStrategy {
    /// Evaluates query string transformation.
    ///
    /// Returns `(Option<new_query_str>, query_changed)`.
    pub fn rewrite_query<'a>(
        &'a self,
        current_query: Option<&'a str>,
    ) -> (Option<Cow<'a, str>>, bool) {
        match self {
            Self::Keep => (current_query.map(Cow::Borrowed), false),
            Self::Drop => {
                let changed = current_query.is_some();
                (None, changed)
            }
            Self::Append { query } => match current_query {
                None | Some("") => (Some(Cow::Borrowed(query.as_str())), true),
                Some(existing) => {
                    let mut combined = String::with_capacity(existing.len() + 1 + query.len());
                    combined.push_str(existing);
                    combined.push('&');
                    combined.push_str(query);
                    (Some(Cow::Owned(combined)), true)
                }
            },
        }
    }
}

/// Tokenized element of a pre-compiled regex replacement template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplacementToken {
    /// Static string literal slice.
    Literal(String),
    /// Numerical capture group reference (e.g., `$1` -> 1).
    Capture(usize),
}

/// Pre-compiled, hot-path optimized path rewrite strategy.
#[derive(Debug, Clone)]
pub enum CompiledPathStrategy {
    StripPrefix {
        prefix: String,
    },
    ReplacePrefix {
        prefix: String,
        replacement: String,
    },
    Exact {
        path: String,
        replacement: String,
    },
    Regex {
        regex: regex::Regex,
        static_prefix: Option<String>,
        template: Vec<ReplacementToken>,
    },
}

impl CompiledPathStrategy {
    /// Compiles a runtime [`PathRewriteStrategy`] into a hot-path execution structure.
    pub fn compile(strategy: &PathRewriteStrategy) -> Result<Self, PluginError> {
        match strategy {
            PathRewriteStrategy::StripPrefix { prefix } => {
                let trimmed = if prefix.is_empty() {
                    "/".to_string()
                } else if !prefix.starts_with('/') {
                    format!("/{prefix}")
                } else {
                    prefix.clone()
                };
                Ok(Self::StripPrefix { prefix: trimmed })
            }
            PathRewriteStrategy::ReplacePrefix {
                prefix,
                replacement,
            } => {
                let norm_prefix = if !prefix.starts_with('/') {
                    format!("/{prefix}")
                } else {
                    prefix.clone()
                };
                let norm_replacement = if !replacement.starts_with('/') {
                    format!("/{replacement}")
                } else {
                    replacement.clone()
                };
                Ok(Self::ReplacePrefix {
                    prefix: norm_prefix,
                    replacement: norm_replacement,
                })
            }
            PathRewriteStrategy::Exact { path, replacement } => {
                let norm_path = if !path.starts_with('/') {
                    format!("/{path}")
                } else {
                    path.clone()
                };
                let norm_replacement = if !replacement.starts_with('/') {
                    format!("/{replacement}")
                } else {
                    replacement.clone()
                };
                Ok(Self::Exact {
                    path: norm_path,
                    replacement: norm_replacement,
                })
            }
            PathRewriteStrategy::Regex {
                pattern,
                replacement,
            } => {
                // Pre-compile Regex with 512KB size limit to protect CPU instruction cache
                let mut builder = regex::RegexBuilder::new(pattern);
                builder.size_limit(512 * 1024);
                let re = builder.build().map_err(|e| {
                    PluginError::Config(format!("Invalid regex pattern '{pattern}': {e}"))
                })?;

                let static_prefix = extract_static_prefix(pattern);
                let template = parse_replacement_template(replacement)?;

                Ok(Self::Regex {
                    regex: re,
                    static_prefix,
                    template,
                })
            }
        }
    }

    /// Evaluates path transformation against the incoming path.
    ///
    /// Returns `(new_path, path_changed)`.
    pub fn rewrite_path<'a>(&'a self, current_path: &'a str) -> (Cow<'a, str>, bool) {
        match self {
            Self::StripPrefix { prefix } => {
                let p = prefix.trim_end_matches('/');
                if current_path == p {
                    // Exact match of prefix -> rewrites to root "/"
                    (Cow::Borrowed("/"), true)
                } else if current_path.starts_with(p) && current_path[p.len()..].starts_with('/') {
                    let remainder = &current_path[p.len()..];
                    (Cow::Borrowed(remainder), true)
                } else {
                    (Cow::Borrowed(current_path), false)
                }
            }
            Self::ReplacePrefix {
                prefix,
                replacement,
            } => {
                let p = prefix.trim_end_matches('/');
                let rep = replacement.trim_end_matches('/');
                if current_path == p {
                    let final_rep = if rep.is_empty() { "/" } else { rep };
                    (Cow::Borrowed(final_rep), true)
                } else if current_path.starts_with(p) && current_path[p.len()..].starts_with('/') {
                    let remainder = &current_path[p.len()..];
                    let mut out = String::with_capacity(rep.len() + remainder.len());
                    out.push_str(rep);
                    out.push_str(remainder);
                    (Cow::Owned(out), true)
                } else {
                    (Cow::Borrowed(current_path), false)
                }
            }
            Self::Exact { path, replacement } => {
                if current_path == path {
                    (Cow::Borrowed(replacement.as_str()), true)
                } else {
                    (Cow::Borrowed(current_path), false)
                }
            }
            Self::Regex {
                regex,
                static_prefix,
                template,
            } => {
                // Optimization: Fast-reject if leading static prefix does not match
                if let Some(sp) = static_prefix
                    && !current_path.starts_with(sp)
                {
                    (Cow::Borrowed(current_path), false)
                } else if let Some(caps) = regex.captures(current_path) {
                    // Pre-calculate exact allocation size
                    let mut total_len = 0;
                    for tok in template {
                        match tok {
                            ReplacementToken::Literal(lit) => total_len += lit.len(),
                            ReplacementToken::Capture(idx) => {
                                if let Some(m) = caps.get(*idx) {
                                    total_len += m.as_str().len();
                                }
                            }
                        }
                    }
                    let mut out = String::with_capacity(total_len);
                    for tok in template {
                        match tok {
                            ReplacementToken::Literal(lit) => out.push_str(lit),
                            ReplacementToken::Capture(idx) => {
                                if let Some(m) = caps.get(*idx) {
                                    out.push_str(m.as_str());
                                }
                            }
                        }
                    }
                    (Cow::Owned(out), true)
                } else {
                    (Cow::Borrowed(current_path), false)
                }
            }
        }
    }
}

/// Extracts static literal prefix from a regex pattern starting with `^`.
///
/// Enables SIMD fast-rejection before DFA execution.
pub fn extract_static_prefix(pattern: &str) -> Option<String> {
    if !pattern.starts_with('^') {
        return None;
    }
    let body = &pattern[1..];
    let mut literal = String::new();

    for c in body.chars() {
        match c {
            // Regex meta-characters terminate the static literal prefix
            '(' | '[' | '{' | '*' | '+' | '?' | '\\' | '.' | '|' | '$' | ')' => break,
            other => literal.push(other),
        }
    }

    if literal.is_empty() {
        None
    } else {
        Some(literal)
    }
}

/// Pre-parses a regex replacement string containing `$1`, `$2` into a sequence of [`ReplacementToken`].
pub fn parse_replacement_template(replacement: &str) -> Result<Vec<ReplacementToken>, PluginError> {
    let mut tokens = Vec::new();
    let mut chars = replacement.char_indices().peekable();
    let mut last_idx = 0;

    while let Some((idx, c)) = chars.next() {
        if c == '$' {
            if idx > last_idx {
                tokens.push(ReplacementToken::Literal(
                    replacement[last_idx..idx].to_string(),
                ));
            }

            if let Some(&(_, next_c)) = chars.peek() {
                if next_c == '$' {
                    // Escaped "$$" -> single literal "$"
                    chars.next();
                    tokens.push(ReplacementToken::Literal("$".to_string()));
                    last_idx = idx + 2;
                } else if next_c.is_ascii_digit() {
                    chars.next();
                    let mut num_str = String::new();
                    num_str.push(next_c);
                    while let Some(&(_, d)) = chars.peek() {
                        if d.is_ascii_digit() {
                            num_str.push(d);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    let group_idx: usize = num_str.parse().map_err(|_| {
                        PluginError::Config(format!(
                            "Invalid capture group index in '{replacement}'"
                        ))
                    })?;
                    tokens.push(ReplacementToken::Capture(group_idx));
                    last_idx = chars.peek().map(|(i, _)| *i).unwrap_or(replacement.len());
                } else {
                    // Trailing single "$" or unrecognized escape
                    tokens.push(ReplacementToken::Literal("$".to_string()));
                    last_idx = idx + 1;
                }
            } else {
                tokens.push(ReplacementToken::Literal("$".to_string()));
                last_idx = idx + 1;
            }
        }
    }

    if last_idx < replacement.len() {
        tokens.push(ReplacementToken::Literal(
            replacement[last_idx..].to_string(),
        ));
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_static_prefix() {
        assert_eq!(
            extract_static_prefix("^/users/([0-9]+)"),
            Some("/users/".into())
        );
        assert_eq!(
            extract_static_prefix("^/api/v1/orders"),
            Some("/api/v1/orders".into())
        );
        assert_eq!(extract_static_prefix("/no-anchor/test"), None);
        assert_eq!(extract_static_prefix("^([0-9]+)/orders"), None);
    }

    #[test]
    fn test_parse_replacement_template() {
        let template = parse_replacement_template("/orders/$2/user/$1").unwrap();
        assert_eq!(
            template,
            vec![
                ReplacementToken::Literal("/orders/".into()),
                ReplacementToken::Capture(2),
                ReplacementToken::Literal("/user/".into()),
                ReplacementToken::Capture(1),
            ]
        );

        let escaped = parse_replacement_template("/price/$$100").unwrap();
        assert_eq!(
            escaped,
            vec![
                ReplacementToken::Literal("/price/".into()),
                ReplacementToken::Literal("$".into()),
                ReplacementToken::Literal("100".into()),
            ]
        );
    }

    #[test]
    fn test_strip_prefix_boundary() {
        let strategy = CompiledPathStrategy::compile(&PathRewriteStrategy::StripPrefix {
            prefix: "/api/v1".into(),
        })
        .unwrap();

        assert_eq!(strategy.rewrite_path("/api/v1/users").0, "/users");
        assert_eq!(strategy.rewrite_path("/api/v1").0, "/");
        assert_eq!(strategy.rewrite_path("/api/v1/").0, "/");
        // /api/v123 boundary protection
        assert_eq!(strategy.rewrite_path("/api/v123").0, "/api/v123");
        assert!(!strategy.rewrite_path("/api/v123").1);
    }

    #[test]
    fn test_replace_prefix() {
        let strategy = CompiledPathStrategy::compile(&PathRewriteStrategy::ReplacePrefix {
            prefix: "/api/v1".into(),
            replacement: "/internal".into(),
        })
        .unwrap();

        assert_eq!(strategy.rewrite_path("/api/v1/users").0, "/internal/users");
        assert_eq!(strategy.rewrite_path("/api/v1").0, "/internal");
    }

    #[test]
    fn test_exact() {
        let strategy = CompiledPathStrategy::compile(&PathRewriteStrategy::Exact {
            path: "/health".into(),
            replacement: "/healthz".into(),
        })
        .unwrap();

        assert_eq!(strategy.rewrite_path("/health").0, "/healthz");
        assert_eq!(strategy.rewrite_path("/health/other").0, "/health/other");
        assert!(!strategy.rewrite_path("/health/other").1);
    }

    #[test]
    fn test_regex() {
        let strategy = CompiledPathStrategy::compile(&PathRewriteStrategy::Regex {
            pattern: r"^/v1/([a-z]+)/([0-9]+)$".into(),
            replacement: "/$1/$2/details".into(),
        })
        .unwrap();

        let (res, changed) = strategy.rewrite_path("/v1/items/42");
        assert!(changed);
        assert_eq!(res, "/items/42/details");

        // Non match
        let (res2, changed2) = strategy.rewrite_path("/v2/items/42");
        assert!(!changed2);
        assert_eq!(res2, "/v2/items/42");
    }

    #[test]
    fn test_query_rewrite() {
        let keep = QueryStrategy::Keep;
        assert_eq!(keep.rewrite_query(Some("a=1")).0.as_deref(), Some("a=1"));

        let drop = QueryStrategy::Drop;
        assert_eq!(drop.rewrite_query(Some("a=1")).0, None);

        let append = QueryStrategy::Append {
            query: "extra=val".into(),
        };
        assert_eq!(
            append.rewrite_query(Some("a=1")).0.as_deref(),
            Some("a=1&extra=val")
        );
        assert_eq!(append.rewrite_query(None).0.as_deref(), Some("extra=val"));
    }
}
