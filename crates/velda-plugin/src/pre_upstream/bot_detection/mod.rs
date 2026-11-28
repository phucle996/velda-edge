//! Deep Layer 7 Bot Detection Plugin (`bot_detection`).
//!
//! Designed to execute **after** [`crate::pre_upstream::ja4_filter::Ja4FilterPlugin`]:
//! - `ja4_filter` drops known botnet TLS signatures immediately with `403 Forbidden`.
//! - `bot_detection` evaluates suspicious traffic on a `0..=100` risk scale via boot-fused
//!   SoA bitmask rules and mitigates offenders primarily with progressive tarpit delays
//!   backed by a strict 64-byte L1 448-bit bitfilter (+ 8B epoch) + L2 exact `(client_ip, ja4)` arena.

pub mod arena;
pub mod compiler;
pub mod config;
pub mod plugin;
pub mod validation;

pub use config::{
    BotDetectionConfig, BotDetectionMode, BotDetectionPreset, BotDetectionThresholds,
    BotEscalationConfig, BotRuleConfig, BotRuleWhen,
};
pub use plugin::BotDetectionPlugin;
