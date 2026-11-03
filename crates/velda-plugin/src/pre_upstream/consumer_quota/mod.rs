//! Consumer Quota & Fair-Use Enforcement Subsystem.

pub mod algorithm;
pub mod key;
pub mod plugin;

pub use algorithm::{
    QuotaAlgorithm, QuotaDecision, QuotaWindowConfig, ShardedQuotaState, WindowState,
};
pub use key::resolve_quota_key;
pub use plugin::{ConsumerQuotaConfig, ConsumerQuotaPlugin};
