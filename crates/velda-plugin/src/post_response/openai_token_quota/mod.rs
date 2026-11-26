//! Post-Response OpenAI Token Quota Plugin (`openai_token_quota`).
//!
//! ```text
//!   openai_token_quota/
//!     ├── config.rs      : OpenAiTokenQuotaConfig (Local / Distributed, hierarchical headers)
//!     ├── validation.rs  : Strict schema validation + power-of-2 page sizing
//!     ├── scanner.rs     : Single-pass zero-alloc tail byte scanner
//!     ├── plugin.rs      : PostResponsePlugin hot-path execution
//!     ├── local/         : Tier 1 — Contiguous k * 4096B SharedUsageArena (128B slots)
//!     └── providers/     : Tier 2 — Isolated Shadow Sync adapters (Redis, Valkey, Dragonfly, ElastiCache, Memorystore, Upstash)
//! ```

pub mod config;
pub mod local;
pub mod plugin;
pub mod providers;
pub mod scanner;
pub mod validation;

pub use config::{OpenAiTokenQuotaConfig, QuotaMode};
pub use local::SharedUsageArena;
pub use plugin::OpenAiTokenQuotaPlugin;
pub use scanner::{TokenUsage, scan_token_usage};
