//! ClickHouse External Third-Party (`velda-etp::clickhouse`) connection pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► ClickHouseEtpConfig::from_entry() ──► ClickHouseEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │
//!         ▼
//!   ClickHouseLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (ClickHouseEtpConfig) | pool.rs (Blueprint + PageArena)          │
//!   │ lease.rs  (ClickHouseLease borrowing Slot[idx] in-place inside PageArena)  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::ClickHouseEtpConfig;
pub use lease::ClickHouseLease;
pub use pool::ClickHouseEtp;
