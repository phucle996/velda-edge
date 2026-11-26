//! Standalone single-endpoint Redis pool branch (`velda-etp::redis::standalone`).
//!
//! ```text
//!   RedisEtpConfig (mode == Standalone) ──► RedisStandalonePool
//!     └─► 1 ColdConnectBlueprint + 1 EtpPageArena<EtpStream> (zero routing overhead)
//! ```

pub mod pool;

pub use pool::RedisStandalonePool;
