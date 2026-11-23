//! Loki External Third-Party (`velda-etp::loki`) connection pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► LokiEtpConfig::from_entry() ──► LokiEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   LokiLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (LokiEtpConfig) | pool.rs (Blueprint + PageArena)                │
//!   │ lease.rs  (LokiLease borrowing Slot[idx] in-place inside LokiEtp arena)    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::LokiEtpConfig;
pub use lease::LokiLease;
pub use pool::LokiEtp;
