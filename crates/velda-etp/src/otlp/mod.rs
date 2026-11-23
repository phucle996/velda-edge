//! OTLP External Third-Party (`velda-etp::otlp`) connection pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► OtlpEtpConfig::from_entry() ──► OtlpEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   OtlpLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (OtlpEtpConfig, OtlpProtocol) | pool.rs (Blueprint + PageArena)  │
//!   │ lease.rs  (OtlpLease borrowing Slot[idx] in-place inside OtlpEtp arena)    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::{OtlpEtpConfig, OtlpProtocol};
pub use lease::OtlpLease;
pub use pool::OtlpEtp;
