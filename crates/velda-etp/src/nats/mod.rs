//! NATS External Third-Party (`velda-etp::nats`) connection pool and session lease.
//!
//! ```text
//!   EtpEntryConfig ──► NatsEtpConfig::from_entry() ──► NatsEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   NatsLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (NatsEtpConfig) | pool.rs (NatsEtp + INFO/CONNECT/PING-PONG)     │
//!   │ lease.rs  (NatsLease borrowing Slot[idx] in-place inside PageArena)        │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::NatsEtpConfig;
pub use lease::NatsLease;
pub use pool::NatsEtp;
