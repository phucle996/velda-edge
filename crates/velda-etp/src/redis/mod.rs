//! Redis External Third-Party (`velda-etp::redis`) connection pool and session lease.
//!
//! ```text
//!   EtpEntryConfig ──► RedisEtpConfig::from_entry() ──► RedisEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   RedisLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (RedisEtpConfig) | pool.rs (RedisEtp + RESP2 AUTH/SELECT)        │
//!   │ lease.rs  (RedisLease borrowing Slot[idx] in-place inside (N+1)*4096 Arena)│
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::RedisEtpConfig;
pub use lease::RedisLease;
pub use pool::RedisEtp;
