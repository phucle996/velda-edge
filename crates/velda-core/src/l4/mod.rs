//! Layer 4 Transport Contracts (`velda_core::l4`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                       LAYER 4 TRANSPORT MODEL (PURE INTEGER POD)                        │
//! ├──────────────┬──────────────────────────────────────────────────────────────────────────┤
//! │ request      │ L4Request { connection_id: u64, peer/local/original_dst (u128 + u16 BE) }│
//! │ response     │ L4Response { action: Forward | Close | Reject, target_ip, target_port_be}│
//! └──────────────┴──────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod request;
pub mod response;
