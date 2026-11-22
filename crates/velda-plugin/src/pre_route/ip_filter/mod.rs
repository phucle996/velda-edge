//! IP Access Control Plugin Module (`ip_filter`).
//!
//! ```text
//!  IP / CIDR Access Control Subsystem Architecture:
//!
//!              ┌──────────────────────────┐
//!              │     Incoming Request     │
//!              └────────────┬─────────────┘
//!                           │
//!                           ▼
//!              ┌──────────────────────────┐
//!              │  stream.rs (Token Stream)│
//!              │  - Zero stack buffer     │
//!              │  - Registers: ParsedIp   │
//!              └────────────┬─────────────┘
//!                           │
//!            ┌──────────────┴──────────────┐
//!            ▼                             ▼
//!  ┌───────────────────┐         ┌───────────────────┐
//!  │  engine.rs        │         │  engine.rs        │
//!  │  (Exact Partition)│         │  (Subnet SWAR)    │
//!  │  /32, /128 (==)   │         │  /1..=/31, /127   │
//!  └───────────────────┘         └───────────────────┘
//! ```

pub mod config;
pub mod engine;
pub mod plugin;
pub mod stream;
pub mod validation;

pub use config::{ClientIpSource, IpFilterConfig, IpFilterMode, IpNetwork};
pub use engine::{IpFilterEngine, match_cidr_v4, match_cidr_v4_64};
pub use plugin::IpFilterPlugin;
pub use stream::{MAX_XFF_HEADERS, MAX_XFF_HOPS, ParsedIp, parse_ip_bytes};
