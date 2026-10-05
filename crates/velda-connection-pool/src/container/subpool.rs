//! Isolated LIFO container holding idle resources for a single target key identity.

use std::time::{Duration, Instant};

use crate::container::probed_max_idle_per_key;
use crate::resource::PoolableResource;

/// Isolated LIFO container holding idle resources for a single key identity.
///
/// Resources are maintained in Last-In-First-Out order so the hottest connection
/// (with warmed TCP congestion windows and live keep-alive timers) is reused first.
#[derive(Debug)]
pub struct SubPool<R> {
    idle_resources: Vec<R>,
    max_idle: usize,
    created_at: Instant,
    empty_since: Option<Instant>,
}

impl<R: PoolableResource> Default for SubPool<R> {
    fn default() -> Self {
        Self::new(probed_max_idle_per_key())
    }
}

impl<R: PoolableResource> SubPool<R> {
    /// Creates an empty container with the specified capacity limit.
    pub fn new(max_idle: usize) -> Self {
        Self {
            idle_resources: Vec::new(),
            max_idle: max_idle.max(1),
            created_at: Instant::now(),
            empty_since: Some(Instant::now()),
        }
    }

    /// Returns the timestamp when this container was created.
    #[inline]
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Returns the configured maximum idle capacity.
    #[inline]
    pub fn max_idle(&self) -> usize {
        self.max_idle
    }

    /// Returns the duration this container has continuously remained empty, or Duration::ZERO if not empty.
    #[inline]
    pub fn empty_duration(&self) -> Duration {
        self.empty_since.map_or(Duration::ZERO, |t| t.elapsed())
    }

    /// Sets the maximum idle capacity for this container and closes any excess idle resources.
    pub fn set_max_idle(&mut self, max_idle: usize) {
        self.max_idle = max_idle.max(1);
        if self.idle_resources.len() > self.max_idle {
            let excess = self.idle_resources.len() - self.max_idle;
            for mut excess_res in self.idle_resources.drain(0..excess) {
                excess_res.close();
            }
        }
    }

    /// Attempts to acquire an idle resource respecting health, idle expiration, and max lifetime.
    ///
    /// Uses LIFO (Last-In-First-Out) retrieval: the most recently used connection is
    /// popped first, maximizing hot TCP window reuse and avoiding remote server keep-alive timeouts.
    /// Samples system clock once to minimize critical section latency under lock.
    pub fn acquire(&mut self, idle_timeout: Duration, max_lifetime: Option<Duration>) -> Option<R> {
        let now = Instant::now();
        while let Some(mut resource) = self.idle_resources.pop() {
            let is_idle_valid =
                now.saturating_duration_since(resource.last_used_at()) <= idle_timeout;
            let is_lifetime_valid = match max_lifetime {
                Some(ttl) => now.saturating_duration_since(resource.created_at()) <= ttl,
                None => true,
            };

            if resource.is_healthy() && is_idle_valid && is_lifetime_valid {
                resource.touch_at(now);
                if self.idle_resources.is_empty() {
                    self.empty_since = Some(now);
                }
                return Some(resource);
            }
            resource.close();
        }

        if self.idle_resources.is_empty() && self.empty_since.is_none() {
            self.empty_since = Some(now);
        }
        None
    }

    /// Releases an active resource back into the idle container.
    ///
    /// If the container is full (`len >= max_idle`), the resource is closed immediately
    /// to prevent File Descriptor exhaustion (`EMFILE`).
    pub fn release(&mut self, mut resource: R) {
        if !resource.is_healthy() {
            resource.close();
            return;
        }

        if self.idle_resources.len() < self.max_idle {
            self.empty_since = None;
            self.idle_resources.push(resource);
        } else {
            resource.close();
        }
    }

    /// Purges all expired or unhealthy resources from this container.
    ///
    /// Returns the number of evicted resources.
    pub fn evict_expired(
        &mut self,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> usize {
        let now = Instant::now();
        let initial_len = self.idle_resources.len();
        self.idle_resources.retain_mut(|res| {
            let is_idle_valid = now.saturating_duration_since(res.last_used_at()) <= idle_timeout;
            let is_lifetime_valid = match max_lifetime {
                Some(ttl) => now.saturating_duration_since(res.created_at()) <= ttl,
                None => true,
            };

            if res.is_healthy() && is_idle_valid && is_lifetime_valid {
                true
            } else {
                res.close();
                false
            }
        });

        if self.idle_resources.is_empty() && self.empty_since.is_none() {
            self.empty_since = Some(now);
        }

        initial_len - self.idle_resources.len()
    }

    /// Closes and drains all connections in this container.
    ///
    /// Returns the number of drained resources.
    pub fn drain_all(&mut self) -> usize {
        let count = self.idle_resources.len();
        for mut res in self.idle_resources.drain(..) {
            res.close();
        }
        self.empty_since = Some(Instant::now());
        count
    }

    /// Closes all connections and clears the container.
    #[inline]
    pub fn clear(&mut self) {
        self.drain_all();
    }

    /// Returns the current number of idle resources in this container.
    #[inline]
    pub fn len(&self) -> usize {
        self.idle_resources.len()
    }

    /// Returns `true` if there are no idle resources.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.idle_resources.is_empty()
    }
}
