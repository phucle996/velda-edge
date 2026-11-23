//! Velda External Third-Party (`velda-etp`) Connection & Lifecycle Engine.
//!
//! ```text
//!   Caller / Plugin matches EtpInstance.backend (EtpBackend) directly
//!         │
//!         ├─► registry::build_registry() / reconcile(&current, new_cfgs, prewarm)
//!         │     ├─ unchanged fingerprint ──► Arc::clone(existing) [0 reconnect]
//!         │     └─ changed / new         ──► build_instance() -> warm_instance()
//!         │
//!         └─► <Kind>Etp::acquire() / warm() ──► EtpPageArena ((required_pages + 1) * 4096B)
//!               ├─ Ready hit (TZCNT) ──► borrow Slot[idx] in-place ──► <Kind>Lease
//!               └─ Miss / Burst      ──► connect.rs (LKG IP / TLS pin) ──► <Kind>Lease
//!
//!   Crate Ownership & Memory Architecture:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ Root Lifecycle: error.rs | config.rs | connect.rs | arena.rs | registry.rs │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Isolated Per-ETP Subfolders (4096B Page-Aligned Bitmap Arena & Leases):    │
//!   │   redis/ | clickhouse/ | s3/ | loki/ | elasticsearch/ | otlp/ | kafka/     │
//!   │   statsd/ (pre-connected UDP socket arena)                                 │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod arena;
pub mod config;
pub mod connect;
pub mod error;
pub mod registry;

pub mod clickhouse;
pub mod elasticsearch;
pub mod kafka;
pub mod loki;
pub mod otlp;
pub mod redis;
pub mod s3;
pub mod statsd;

pub use config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
pub use connect::{EtpTlsConfig, EtpTlsVersion, EtpTransportSecurity};
pub use error::EtpError;
pub use registry::{EtpBackend, EtpInstance, EtpRegistry};
