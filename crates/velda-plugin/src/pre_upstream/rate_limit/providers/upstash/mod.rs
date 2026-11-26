//! Upstash HTTPS REST (`/pipeline`) State-of-Truth (`velda_etp::upstash::UpstashEtp`)
//! adapter for `rate_limit`.
//!
//! ```text
//!   providers/upstash/
//!     ├─► mod.rs    (module root export)
//!     ├─► http.rs   (HTTP/1.1 header + chunked/Content-Length response framing)
//!     ├─► client.rs (POST /pipeline SET EX 15 NX + INCRBY + Bearer auth)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod http;
pub mod sync;
