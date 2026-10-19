//! Pre-Upstream API Key Authentication Plugin Submodule (`api_key_auth`).

pub mod cache;
pub mod plugin;

pub use cache::ApiKeyCache;
pub use plugin::{ApiKeyAuthPlugin, ApiKeyEntry, KeySource};
