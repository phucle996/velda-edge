//! PostgreSQL External Third-Party (`velda-etp::postgres`) connection pool and session lease.
//!
//! ```text
//!   EtpEntryConfig ──► PostgresEtpConfig::from_entry() ──► PostgresEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   PostgresLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (PostgresEtpConfig) | pool.rs (Startup + Cleartext/MD5/SCRAM)    │
//!   │ lease.rs  (PostgresLease borrowing Slot[idx] in-place inside PageArena)    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::PostgresEtpConfig;
pub use lease::PostgresLease;
pub use pool::PostgresEtp;
