//! Pre-Upstream API Key Authentication Plugin (`api_key_auth`).

pub mod arena;
pub mod config;
pub mod plugin;
pub mod validation;

pub use config::{ApiKeyAuthConfig, ApiKeyEntryConfig, MAX_API_KEY_LEN};
pub use plugin::ApiKeyAuthPlugin;
