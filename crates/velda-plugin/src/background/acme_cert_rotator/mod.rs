//! ACME Automated Certificate Rotator Plugin (`phase: "background"`).
//!
//! Provides zero-downtime, automated Let's Encrypt / ACME RFC 8555 certificate issuance and rotation.
//! Operates strictly on Shadow Logistics workers (`SCHED_IDLE`), maintaining zero overhead on frontline traffic workers.

pub mod challenge;
pub mod client;
pub mod cron;
pub mod crypto;
pub mod expiry;
pub mod hot_swap;
pub mod plugin;
pub mod route_guard;
pub mod storage;

pub use plugin::AcmeCertRotatorPlugin;
pub use velda_sync::AcmeCertRotatorConfig;
