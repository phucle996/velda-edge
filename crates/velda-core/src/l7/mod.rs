//! Layer 7 Application Request & Response Models (`velda_core::l7`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                         LAYER 7 APPLICATION DATA CONTRACTS                              │
//! ├──────────────┬──────────────────────────────────────────────────────────────────────────┤
//! │ request      │ L7Request (method, uri, version, HeaderWorkspace, Body)                  │
//! │ response     │ L7Response (status, version, HeaderWorkspace, Body)                      │
//! └──────────────┴──────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod request;
pub mod response;

pub use crate::buffer::header::HeaderWorkspace;
pub use request::Body;
