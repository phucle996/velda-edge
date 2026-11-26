//! MySQL / MariaDB External Third-Party (`velda-etp::mysql`) connection pool and session lease.
//!
//! ```text
//!   EtpEntryConfig ──► MysqlEtpConfig::from_entry() ──► MysqlEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   MysqlLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (MysqlEtpConfig) | pool.rs (HandshakeV10 + native/caching_sha2)  │
//!   │ lease.rs  (MysqlLease borrowing Slot[idx] in-place inside PageArena)       │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::MysqlEtpConfig;
pub use lease::MysqlLease;
pub use pool::MysqlEtp;
