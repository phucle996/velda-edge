//! GCP Memorystore State-of-Truth (`velda_etp::gcp_memorystore::GcpMemorystoreEtp`) adapter
//! for `api_metering`.
//!
//! ```text
//!   providers/gcp_memorystore/
//!     ├─► mod.rs    (module root export)
//!     ├─► codec.rs  (RESP2 HINCRBY encoder, reply parser & lease round-trip)
//!     ├─► client.rs (IAM-authenticated Standalone & Cluster batch orchestration)
//!     └─► sync.rs   (Shadow Worker delta collect -> HINCRBY -> reconcile_balances)
//! ```

pub mod client;
pub mod codec;
pub mod sync;
