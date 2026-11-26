//! Elasticsearch / OpenSearch State-of-Truth (`velda_etp::elasticsearch::ElasticsearchEtp`)
//! adapter for `rate_limit`.
//!
//! ```text
//!   providers/elasticsearch/
//!     ├─► mod.rs    (module root export)
//!     ├─► http.rs   (HTTP/1.1 response header + Content-Length reader)
//!     ├─► client.rs (_bulk painless scripted_upsert + _mget cluster total readback)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod http;
pub mod sync;
