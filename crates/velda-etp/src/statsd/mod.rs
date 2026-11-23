//! StatsD External Third-Party (`velda-etp::statsd`) UDP socket pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► StatsdEtpConfig::from_entry() ──► StatsdEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<UdpSocket> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   StatsdLease ──► Deref/DerefMut(&mut UdpSocket) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (StatsdEtpConfig, StatsdFlavor) | pool.rs (UdpSocket PageArena)  │
//!   │ lease.rs  (StatsdLease borrowing Slot[idx] UdpSocket in-place inside Arena)│
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::{StatsdEtpConfig, StatsdFlavor};
pub use lease::StatsdLease;
pub use pool::StatsdEtp;
