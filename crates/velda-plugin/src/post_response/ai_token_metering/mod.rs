//! AI Token Metering & Monetization Plugin (`ai_token_metering`).
//!
//! # Objective & Mission
//! Enforces zero-allocation, fixed-point Micro-USD monetization and token measurement
//! for downstream AI coding clients (Cursor, Copilot, Claude Code, Aider, etc.)
//! serving OpenAI-compatible REST and SSE streaming endpoints.

pub mod config;
pub mod plugin;
pub mod pricing;
pub mod scanner;
pub mod state;

pub use config::AiTokenMeteringConfig;
pub use plugin::AiTokenMeteringPlugin;
pub use pricing::ModelPricing;
pub use scanner::{TokenUsage, scan_model, scan_token_usage};
pub use state::{ConsumerAiUsage, ShardedAiUsageLedger};
