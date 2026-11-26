//! Upstash HTTPS REST (`/pipeline`) State-of-Truth (`velda_etp::upstash::UpstashEtp`)
//! adapter for `openai_token_quota`.
//!
//! ```text
//!   providers/upstash/
//!     ├─► mod.rs    (module root export)
//!     ├─► http.rs   (HTTP/1.1 header + chunked/Content-Length response framing)
//!     ├─► client.rs (POST /pipeline DECRBY JSON + compensating INCRBY refund + Bearer auth)
//!     └─► sync.rs   (Shadow Worker refill/ticker -> flush_batch)
//! ```

pub mod client;
pub mod http;
pub mod sync;
