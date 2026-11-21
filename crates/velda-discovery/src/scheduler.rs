//! Centralized background discovery scheduler with coarse-clock jitter and change detection.
//!
//! Designed to run on Shadow Worker 0 (`SCHED_IDLE`), replacing per-service `tokio::spawn` loops
//! with a single cooperative tick loop, pre-allocated scratch buffers, and zero-alloc slice `==`
//! change detection.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use crate::dns::{DnsResolverProvider, DnsServerProvider, DnsTransport};
use crate::endpoint::Endpoint;
use crate::service::Discovery;

/// Callback invoked only when a service's sorted endpoint slice actually changes.
pub type ChangeCallback = Arc<dyn Fn(&[Endpoint], u64) + Send + Sync>;

/// Abstract resolution source polled by [`DiscoveryScheduler`].
pub trait DiscoveryTarget: Send + Sync {
    /// Resolves the current list of endpoints for this target.
    fn resolve<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<Vec<Endpoint>>> + Send + 'a>>;

    /// Remaining cache TTL if known.
    fn ttl_remaining(&self) -> Option<Duration>;

    /// Human-readable label for diagnostics.
    fn label(&self) -> &str;
}

/// Standard A/AAAA DNS target polled by [`DiscoveryScheduler`].
pub struct DnsDiscoveryTarget<S: DnsServerProvider, T: DnsTransport> {
    pub host: String,
    pub port: u16,
    pub resolver: Arc<DnsResolverProvider<S, T>>,
}

impl<S: DnsServerProvider + 'static, T: DnsTransport + 'static> DiscoveryTarget
    for DnsDiscoveryTarget<S, T>
{
    fn resolve<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<Vec<Endpoint>>> + Send + 'a>> {
        Box::pin(async move { self.resolver.resolve(&self.host, self.port).await })
    }

    fn ttl_remaining(&self) -> Option<Duration> {
        self.resolver.cache().ttl_remaining(&self.host)
    }

    fn label(&self) -> &str {
        &self.host
    }
}

/// RFC 2782 DNS SRV target polled by [`DiscoveryScheduler`].
pub struct DnsSrvDiscoveryTarget<S: DnsServerProvider, T: DnsTransport> {
    pub service: String,
    pub resolver: Arc<DnsResolverProvider<S, T>>,
}

impl<S: DnsServerProvider + 'static, T: DnsTransport + 'static> DiscoveryTarget
    for DnsSrvDiscoveryTarget<S, T>
{
    fn resolve<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<Vec<Endpoint>>> + Send + 'a>> {
        Box::pin(async move { self.resolver.resolve_srv(&self.service).await })
    }

    fn ttl_remaining(&self) -> Option<Duration> {
        None
    }

    fn label(&self) -> &str {
        &self.service
    }
}

struct ScheduledEntry {
    discovery: Weak<Discovery>,
    target: Box<dyn DiscoveryTarget>,
    refresh_interval_secs: u64,
    next_tick_sec: u64,
    published: Vec<Endpoint>,
    generation: u64,
    on_change: Option<ChangeCallback>,
}

struct SchedulerState {
    entries: Vec<ScheduledEntry>,
    /// Pre-allocated scratch buffer reused across ticks for in-place sort & `==` comparison.
    scratch: Vec<Endpoint>,
}

/// Centralized discovery scheduler executed cooperatively on Shadow Worker 0 (`SCHED_IDLE`).
pub struct DiscoveryScheduler {
    pending: std::sync::Mutex<Vec<ScheduledEntry>>,
    state: tokio::sync::Mutex<SchedulerState>,
}

impl Default for DiscoveryScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscoveryScheduler {
    /// Creates a new empty [`DiscoveryScheduler`] with pre-allocated scratch capacity.
    pub fn new() -> Self {
        Self {
            pending: std::sync::Mutex::new(Vec::new()),
            state: tokio::sync::Mutex::new(SchedulerState {
                entries: Vec::new(),
                scratch: Vec::with_capacity(64),
            }),
        }
    }

    /// Registers a discovery target into the centralized scheduler.
    ///
    /// Non-blocking: pushes to the pending queue without awaiting active resolution I/O.
    pub fn register(
        &self,
        discovery: &Arc<Discovery>,
        target: Box<dyn DiscoveryTarget>,
        refresh_interval: Duration,
        initial_endpoints: &[Endpoint],
        initial_gen: u64,
        on_change: Option<ChangeCallback>,
    ) {
        let mut published = initial_endpoints.to_vec();
        published.sort_unstable();

        let refresh_interval_secs = refresh_interval.as_secs().max(1);
        let next_tick_sec = if initial_endpoints.is_empty() {
            0
        } else {
            let now = velda_core::clock::coarse_sec();
            let base = target.ttl_remaining().map_or(refresh_interval_secs, |d| {
                d.as_secs().clamp(1, refresh_interval_secs)
            });
            now + base
        };

        let entry = ScheduledEntry {
            discovery: Arc::downgrade(discovery),
            target,
            refresh_interval_secs,
            next_tick_sec,
            published,
            generation: initial_gen,
            on_change,
        };

        if let Ok(mut pending) = self.pending.lock() {
            pending.push(entry);
        }
    }

    /// Returns the number of active + pending entries (primarily for diagnostics/tests).
    pub async fn len(&self) -> usize {
        let pending_len = self.pending.lock().map(|p| p.len()).unwrap_or(0);
        let state = self.state.lock().await;
        state.entries.len() + pending_len
    }

    /// Returns `true` if there are no active or pending entries.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Runs one cooperative tick using `velda_core::clock::coarse_sec()`.
    pub async fn tick(&self) {
        self.tick_at(velda_core::clock::coarse_sec()).await;
    }

    /// Runs one cooperative tick at the specified `now_sec` coarse timestamp.
    pub async fn tick_at(&self, now_sec: u64) {
        let mut state = self.state.lock().await;

        // Drain newly registered entries
        if let Ok(mut pending) = self.pending.lock()
            && !pending.is_empty()
        {
            state.entries.append(&mut *pending);
        }

        // Prune dropped or explicitly shut-down discoveries
        state
            .entries
            .retain(|entry| entry.discovery.upgrade().is_some_and(|d| !d.is_shutdown()));

        let SchedulerState { entries, scratch } = &mut *state;
        for (idx, entry) in entries.iter_mut().enumerate() {
            if now_sec < entry.next_tick_sec {
                continue;
            }

            let Some(disc) = entry.discovery.upgrade() else {
                continue;
            };
            if disc.is_shutdown() {
                continue;
            }

            match entry.target.resolve().await {
                Ok(new_endpoints) => {
                    scratch.clear();
                    scratch.extend_from_slice(&new_endpoints);
                    scratch.sort_unstable();

                    if scratch.as_slice() != entry.published.as_slice() {
                        entry.published.clear();
                        entry.published.extend_from_slice(scratch);
                        entry.generation += 1;
                        disc.update_endpoints(scratch.clone(), entry.generation);
                        if let Some(ref cb) = entry.on_change {
                            cb(&entry.published, entry.generation);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target = %entry.target.label(),
                        error = %e,
                        "Background discovery refresh failed; retaining Last-Known-Good endpoints"
                    );
                }
            }

            let ttl_secs = entry
                .target
                .ttl_remaining()
                .map_or(entry.refresh_interval_secs, |t| {
                    t.as_secs().clamp(1, entry.refresh_interval_secs)
                });
            let max_jitter = (ttl_secs / 4).max(1);
            let jitter = now_sec
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(idx as u64)
                % max_jitter;
            entry.next_tick_sec = now_sec.saturating_add(ttl_secs).saturating_add(jitter);
        }
    }
}

static GLOBAL_DISCOVERY_SCHEDULER: OnceLock<Arc<DiscoveryScheduler>> = OnceLock::new();

/// Returns the process-wide centralized [`DiscoveryScheduler`] singleton.
#[inline]
pub fn global_discovery_scheduler() -> &'static Arc<DiscoveryScheduler> {
    GLOBAL_DISCOVERY_SCHEDULER.get_or_init(|| Arc::new(DiscoveryScheduler::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    struct MockTarget {
        endpoints: Arc<std::sync::Mutex<Vec<Endpoint>>>,
    }

    impl DiscoveryTarget for MockTarget {
        fn resolve<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = crate::error::Result<Vec<Endpoint>>> + Send + 'a>>
        {
            let eps = self.endpoints.lock().unwrap().clone();
            Box::pin(async move { Ok(eps) })
        }

        fn ttl_remaining(&self) -> Option<Duration> {
            None
        }

        fn label(&self) -> &str {
            "mock-service"
        }
    }

    #[tokio::test]
    async fn test_scheduler_skip_if_unchanged() {
        let ep1 = Endpoint::from_v4([10, 0, 0, 1], 8080, 1);
        let ep2 = Endpoint::from_v4([10, 0, 0, 2], 8080, 1);
        let ep3 = Endpoint::from_v4([10, 0, 0, 3], 8080, 1);

        let shared = Arc::new(std::sync::Mutex::new(vec![ep1, ep2]));
        let discovery = Discovery::new_explicit(vec![ep1, ep2]);

        let cb_calls = Arc::new(AtomicUsize::new(0));
        let last_gen = Arc::new(AtomicU64::new(0));
        let cb_calls_c = Arc::clone(&cb_calls);
        let last_gen_c = Arc::clone(&last_gen);

        let scheduler = DiscoveryScheduler::new();
        scheduler.register(
            &discovery,
            Box::new(MockTarget {
                endpoints: Arc::clone(&shared),
            }),
            Duration::from_secs(5),
            &[ep1, ep2],
            1,
            Some(Arc::new(move |_eps, generation| {
                cb_calls_c.fetch_add(1, Ordering::Relaxed);
                last_gen_c.store(generation, Ordering::Relaxed);
            })),
        );

        let base = velda_core::clock::coarse_sec();

        // Same endpoints in reversed order -> sorted slice equals published -> skip-if-unchanged
        *shared.lock().unwrap() = vec![ep2, ep1];
        scheduler.tick_at(base + 100).await;
        assert_eq!(cb_calls.load(Ordering::Relaxed), 0);

        // Same endpoints again on subsequent tick -> still skipped
        scheduler.tick_at(base + 1_000).await;
        assert_eq!(cb_calls.load(Ordering::Relaxed), 0);

        // Endpoints actually change -> callback invoked and generation increments
        *shared.lock().unwrap() = vec![ep1, ep3];
        scheduler.tick_at(base + 10_000).await;
        assert_eq!(cb_calls.load(Ordering::Relaxed), 1);
        assert_eq!(last_gen.load(Ordering::Relaxed), 2);
    }
}
