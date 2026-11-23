//! Kafka External Third-Party (`velda-etp::kafka`) broker connection pool and lease.
//!
//! ```text
//!   EtpEntryConfig ──► KafkaEtpConfig::from_entry() ──► KafkaEtp::new()
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire() ──► EtpPageArena<EtpStream> (TZCNT Slot[idx])
//!         │                                  └─ optional SaslHandshake + SaslAuthenticate
//!         ▼
//!   KafkaLease ──► Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!
//!   Module & Memory Ownership:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs (KafkaEtpConfig + KafkaSaslMechanism)                            │
//!   │ pool.rs   (round-robin brokers + optional SASL + PageArena)                │
//!   │ lease.rs  (KafkaLease borrowing Slot[idx] in-place inside KafkaEtp arena)  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod lease;
pub mod pool;

pub use config::{KafkaEtpConfig, KafkaSaslMechanism};
pub use lease::KafkaLease;
pub use pool::KafkaEtp;
