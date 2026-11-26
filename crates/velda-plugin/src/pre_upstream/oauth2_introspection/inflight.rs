//! Sharded Single-Flight Coordinator (`oauth2_introspection::inflight`).
//!
//! ```text
//!   Cold Miss (`fp: u128`, calling `current_lane_idx()`)
//!        │
//!        ▼
//!   InFlightCoordinator::introspect_coalesced()  [64 Shards x 64 Entries = 4,096 max]
//!        ├── [Leader]    ──► providers::execute(&call) -> provider fetches, parses & caches Leader lane
//!        │                     └─ Broadcast `(exp_opt, ColdOutcome)` to Followers
//!        ├── [Follower]  ──► Await broadcast -> Commit `(exp_opt, ColdOutcome)` on Follower's own lane
//!        └── [Saturated] ──► Direct `providers::execute(&call)` fallback
//!
//!   Stale-While-Revalidate (`velda-shadow-{id}`):
//!        └──► `providers::revalidate(&call)` (fetches, parses & updates `origin_lane` directly)
//! ```

use std::sync::Mutex;

use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use velda_core::HeaderKey;
use velda_etp::EtpBackend;

use super::cache::{ColdOutcome, IntrospectionCache};
use super::providers::{self, IntrospectionCall, IntrospectionCallParams};

pub const INFLIGHT_SHARDS: usize = 64;
pub const MAX_INFLIGHT_PER_SHARD: usize = 64; // 64 * 64 = 4,096 concurrent distinct tokens max

#[derive(Clone)]
struct BroadcastResult {
    exp_opt: Option<u64>,
    outcome: ColdOutcome,
}

enum InFlightSlot {
    Leader(broadcast::Sender<BroadcastResult>, usize),
    Follower(broadcast::Receiver<BroadcastResult>),
    Saturated,
}

#[repr(align(64))]
struct InFlightShard {
    entries: Mutex<FxHashMap<u128, broadcast::Sender<BroadcastResult>>>,
}

/// Bounded sharded single-flight coordinator deduplicating concurrent IdP/Vault calls per token.
pub struct InFlightCoordinator {
    shards: Box<[InFlightShard]>,
}

impl Default for InFlightCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl InFlightCoordinator {
    /// Allocates 64 cache-line aligned single-flight shards.
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(INFLIGHT_SHARDS);
        for _ in 0..INFLIGHT_SHARDS {
            shards.push(InFlightShard {
                entries: Mutex::new(FxHashMap::default()),
            });
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    fn enter(&self, fp: u128) -> InFlightSlot {
        let shard_idx = (fp as usize) & (INFLIGHT_SHARDS - 1);
        let mut guard = match self.shards[shard_idx].entries.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(tx) = guard.get(&fp) {
            return InFlightSlot::Follower(tx.subscribe());
        }
        if guard.len() >= MAX_INFLIGHT_PER_SHARD {
            return InFlightSlot::Saturated;
        }
        let (tx, _) = broadcast::channel(1);
        guard.insert(fp, tx.clone());
        InFlightSlot::Leader(tx, shard_idx)
    }

    fn remove(&self, fp: u128, shard_idx: usize) {
        if let Ok(mut guard) = self.shards[shard_idx].entries.lock() {
            guard.remove(&fp);
        }
    }

    /// Executes single-flight coalesced dispatch into [`providers::execute`], ensuring both
    /// Leader and any Follower threads warm their local cache lanes.
    #[allow(clippy::too_many_arguments)]
    pub async fn introspect_coalesced(
        &self,
        backend: &EtpBackend,
        params: &IntrospectionCallParams,
        cache: &IntrospectionCache,
        claims_to_headers: &[(Box<str>, HeaderKey)],
        required_scopes: &[String],
        token: &str,
        fp: u128,
    ) -> ColdOutcome {
        let lane = cache.current_lane_idx();
        let call = IntrospectionCall {
            backend,
            params,
            cache,
            claims_to_headers,
            required_scopes,
            token,
            fp,
            target_lane: lane,
        };

        match self.enter(fp) {
            InFlightSlot::Follower(mut rx) => match rx.recv().await {
                Ok(res) => {
                    if cache.is_enabled() {
                        let now = velda_core::clock::coarse_sec();
                        commit_follower_lane(cache, lane, fp, res.exp_opt, &res.outcome, now);
                    }
                    res.outcome
                }
                Err(_) => providers::execute(&call).await.1,
            },
            InFlightSlot::Saturated => providers::execute(&call).await.1,
            InFlightSlot::Leader(tx, shard_idx) => {
                let guard = InFlightLeaderGuard {
                    coord: self,
                    fp,
                    shard_idx,
                    tx: Some(tx),
                    finished: false,
                };
                let (exp_opt, outcome) = providers::execute(&call).await;
                guard.complete(BroadcastResult { exp_opt, outcome })
            }
        }
    }
}

struct InFlightLeaderGuard<'a> {
    coord: &'a InFlightCoordinator,
    fp: u128,
    shard_idx: usize,
    tx: Option<broadcast::Sender<BroadcastResult>>,
    finished: bool,
}

impl InFlightLeaderGuard<'_> {
    fn complete(mut self, res: BroadcastResult) -> ColdOutcome {
        self.coord.remove(self.fp, self.shard_idx);
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(res.clone());
        }
        self.finished = true;
        res.outcome
    }
}

impl Drop for InFlightLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.coord.remove(self.fp, self.shard_idx);
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(BroadcastResult {
                    exp_opt: None,
                    outcome: ColdOutcome::Unavailable,
                });
            }
        }
    }
}

#[inline]
fn commit_follower_lane(
    cache: &IntrospectionCache,
    lane_idx: usize,
    fp: u128,
    exp_opt: Option<u64>,
    outcome: &ColdOutcome,
    now: u64,
) {
    match outcome {
        ColdOutcome::Allow(hdrs) => {
            cache.store_positive_on_lane(lane_idx, fp, true, hdrs.clone(), exp_opt, now);
        }
        ColdOutcome::Forbidden => {
            cache.store_positive_on_lane(lane_idx, fp, false, None, exp_opt, now);
        }
        ColdOutcome::Unauthorized => {
            cache.store_negative_on_lane(lane_idx, fp, now);
        }
        ColdOutcome::Unavailable => {}
    }
}

/// Dispatches an asynchronous background revalidation call to the provider for `origin_lane`.
#[allow(clippy::too_many_arguments)]
pub async fn revalidate_on_shadow(
    backend: &EtpBackend,
    params: &IntrospectionCallParams,
    cache: &IntrospectionCache,
    claims_to_headers: &[(Box<str>, HeaderKey)],
    required_scopes: &[String],
    token: &str,
    fp: u128,
    origin_lane: usize,
) {
    let call = IntrospectionCall {
        backend,
        params,
        cache,
        claims_to_headers,
        required_scopes,
        token,
        fp,
        target_lane: origin_lane,
    };
    providers::revalidate(&call).await;
}
