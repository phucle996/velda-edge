//! DragonflyDB ETP (`velda-etp::dragonfly`) dedicated multi-core single-endpoint RESP2 pool.
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Dragonfly)
//!         └──► DragonflyEtp (ColdConnectBlueprint + EtpPageArena + AUTH/SELECT)
//!                └──► warm() / acquire() ──► DragonflyLease (Slot[idx])
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::DragonflyEtpConfig;
pub use lease::DragonflyLease;
pub use pool::DragonflyEtp;
