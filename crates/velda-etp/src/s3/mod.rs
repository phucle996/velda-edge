//! S3 External Third-Party (`velda-etp::s3`) connection pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► S3EtpConfig::from_entry() ──► S3Etp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   S3Lease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (S3EtpConfig) | pool.rs (S3Etp + (N+1)*4096B PageArena)          │
//!   │ lease.rs  (S3Lease borrowing Slot[idx] in-place inside S3Etp arena)        │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::S3EtpConfig;
pub use lease::S3Lease;
pub use pool::S3Etp;
