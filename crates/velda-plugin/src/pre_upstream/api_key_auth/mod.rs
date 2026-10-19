//! Pre-Upstream API Key Authentication Plugin Submodule (`api_key_auth`).

pub mod cache;
pub mod plugin;
pub mod source;

pub use cache::ApiKeyCache;
pub use plugin::ApiKeyAuthPlugin;
pub use source::{ApiKeyEntry, KeySource, extract_token};
