//! URL Rewrite Pre-Upstream Plugin (`url_rewrite`).
//!
//! Single-pass Aho-Corasick + SIMD URI (`path` + `query`) transformation plugin.

pub mod config;
pub mod plugin;
pub mod validation;

pub use config::{PathRewriteRule, QueryRewriteMode, UrlRewriteConfig};
pub use plugin::UrlRewritePlugin;
