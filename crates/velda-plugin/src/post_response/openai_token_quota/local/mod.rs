//! Local Tier (`local/`): In-memory contiguous `k * 4096`-byte `SharedUsageArena`.

pub mod arena;

pub use arena::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, Fnv1aHasher, MAX_CONSUMER_KEY_LEN, RecordOutcome,
    SharedUsageArena, UsageSlot, fnv1a_64,
};
