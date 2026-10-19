//! URL Rewrite Plugin Module (`url_rewrite`).

pub mod plugin;
pub mod sanitizer;
pub mod strategy;

pub use plugin::{UrlRewriteConfig, UrlRewritePlugin};
pub use sanitizer::{PathTraversalError, is_already_normalized, normalize_and_sanitize_path};
pub use strategy::{
    CompiledPathStrategy, PathRewriteStrategy, QueryStrategy, ReplacementToken,
    extract_static_prefix, parse_replacement_template,
};
