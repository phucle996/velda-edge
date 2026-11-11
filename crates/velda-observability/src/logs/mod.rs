//! Lock-Free 64-Byte Cacheline Access Log Subsystem (`velda_observability::logs`).
//!
//! Organized by processing stage:
//! - `types`: Finite protocol/method/gRPC/completion discriminants and RFC 4291 `u128` IP codecs.
//! - `user_agent`: Finite `UaFamily`/`UaOs` discriminants and single-pass $\mathcal{O}(N)$ byte classifier.
//! - `record`: Canonical 64-byte (`#[repr(C, align(64))]`) pure-integer [`AccessLogRecord`].
//! - `ring`: Pre-allocated 64-byte aligned per-core [`SpscLogRing`] with Non-Temporal streaming stores
//!   on Frontline push and L2/L3 `_MM_HINT_T1` prefetching on Shadow drain.
//! - `store`: Reference-counted `Arc<[AccessLogRecord]>` multi-exporter fan-out [`LogStore`].

mod record;
mod ring;
mod store;
mod types;
mod user_agent;

pub use record::AccessLogRecord;
pub use ring::{DEFAULT_CORE_RING_CAPACITY, SpscLogRing};
pub use store::{LogStore, LogSubscriber, global_log_store, init_global_log_store};
pub use types::{
    LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, decode_ip_u128, encode_ip_u128,
    encode_upstream_ip_u32,
};
pub use user_agent::{UaFamily, UaOs, classify_user_agent};
