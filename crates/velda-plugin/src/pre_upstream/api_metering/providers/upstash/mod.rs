//! Upstash HTTPS REST (`/pipeline`) State-of-Truth (`velda_etp::upstash::UpstashEtp`)
//! adapter for `api_metering`.
//!
//! ```text
//!   providers/upstash/
//!     ├─► mod.rs    (module root export)
//!     ├─► http.rs   (HTTP/1.1 header + chunked/Content-Length response framing)
//!     ├─► client.rs (POST /pipeline HINCRBY JSON + Bearer auth over UpstashEtp lease)
//!     └─► sync.rs   (Shadow Worker delta collect -> /pipeline -> reconcile_balances)
//! ```

pub mod client;
pub mod http;
pub mod sync;
