//! Elasticsearch External Third-Party (`velda-etp::elasticsearch`) connection pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► ElasticsearchEtpConfig::from_entry() ──► ElasticsearchEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   ElasticsearchLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (ElasticsearchEtpConfig) | pool.rs (Blueprint + PageArena)       │
//!   │ lease.rs  (ElasticsearchLease borrowing Slot[idx] in-place inside Arena)   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::ElasticsearchEtpConfig;
pub use lease::ElasticsearchLease;
pub use pool::ElasticsearchEtp;
