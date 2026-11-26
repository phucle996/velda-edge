//! AWS S3 / MinIO / R2 State-of-Truth (`velda_etp::s3::S3Etp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/s3/
//!     ├─► mod.rs    (module root export)
//!     ├─► sigv4.rs  (HMAC-SHA256 AWS4 signer + S3 URI path builder)
//!     ├─► http.rs   (HTTP/1.1 response header + ETag + Content-Length reader)
//!     ├─► client.rs (SigV4 GET + PUT counter object merge per key epoch)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod http;
pub mod sigv4;
pub mod sync;
