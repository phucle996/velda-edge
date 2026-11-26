//! URL Rewrite Configuration Schema (`url_rewrite::config`).
//!
//! ```text
//!   plugins.json ("url_rewrite")
//!     ├── path_rules[]   : literal { find, replace } rules (single Aho-Corasick LeftmostLongest pass)
//!     ├── query_mode     : "keep" | "drop" | "replace"
//!     ├── query_replace  : literal query string when query_mode == "replace"
//!     ├── remove_params  : exact query parameter names stripped when query_mode == "keep"
//!     └── query_append   : static query suffix appended when query_mode == "keep"
//! ```

use serde::{Deserialize, Serialize};

/// Single literal path substring rewrite rule (`find` -> `replace`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathRewriteRule {
    /// Non-empty literal substring matched verbatim against the URI path.
    pub find: String,
    /// Literal replacement inserted verbatim wherever `find` matches.
    pub replace: String,
}

/// Query string handling policy evaluated after path rewriting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueryRewriteMode {
    /// Preserves incoming query (optionally removing `remove_params` and/or appending `query_append`).
    #[default]
    Keep,
    /// Strips the entire query string.
    Drop,
    /// Replaces the entire query string literally with `query_replace`.
    Replace,
}

/// Declarative configuration for [`super::plugin::UrlRewritePlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UrlRewriteConfig {
    /// Ordered literal path rewrite rules compiled into a single Aho-Corasick automaton.
    pub path_rules: Vec<PathRewriteRule>,
    /// Query handling policy (`keep`, `drop`, or `replace`).
    pub query_mode: QueryRewriteMode,
    /// Literal query string used when `query_mode == "replace"` (without leading `?`).
    pub query_replace: Option<String>,
    /// Exact parameter names removed when `query_mode == "keep"`.
    pub remove_params: Vec<String>,
    /// Static query parameters appended when `query_mode == "keep"` (without leading `?` or `&`).
    pub query_append: Option<String>,
}
