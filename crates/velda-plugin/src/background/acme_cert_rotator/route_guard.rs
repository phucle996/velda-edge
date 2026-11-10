//! Ephemeral Route Injection and RAII Guard for HTTP-01 Challenges.
//!
//! Bridges the ACME challenge lifecycle into the in-memory router table:
//! - Injects an exact route: `/.well-known/acme-challenge/<token>` -> `RouteId(u32::MAX)`.
//! - Pre-bakes the complete static wire response into [`ChallengeStore`] (residing cleanly in L3/RAM).
//! - Implements [`Drop`] to guarantee unconditional teardown upon success, failure, timeout, or cancellation.

use std::sync::Arc;
use std::sync::RwLock;

use velda_core::{ListenerId, RouteId, UpstreamId};
use velda_router::{Http1Route, Router};

use super::challenge::{CHALLENGE_PATH_PREFIX, ChallengeGuard, ChallengeStore};

/// Ephemeral route identifier reserved strictly for transient ACME HTTP-01 challenge matching.
pub const ACME_CHALLENGE_ROUTE_ID: RouteId = RouteId::new(u32::MAX);

/// Dummy upstream identifier used because ACME challenges short-circuit directly in memory.
pub const ACME_DUMMY_UPSTREAM_ID: UpstreamId = UpstreamId::new(0);

/// Thread-safe registry providing dynamic exact-route registration into the active [`Router`].
pub struct RouterRegistry {
    router: Arc<RwLock<Router>>,
    listener_id: ListenerId,
}

impl RouterRegistry {
    /// Creates a new router registry targeting a specific ingress listener.
    pub fn new(router: Arc<RwLock<Router>>, listener_id: impl Into<ListenerId>) -> Self {
        Self {
            router,
            listener_id: listener_id.into(),
        }
    }

    /// Registers an ephemeral exact route and publishes the challenge response, returning an RAII guard.
    pub fn register_challenge(
        &self,
        token: &str,
        key_authorization: &str,
        challenge_store: &Arc<ChallengeStore>,
    ) -> EphemeralRouteGuard {
        let exact_path = format!("{CHALLENGE_PATH_PREFIX}{token}");

        // 1. Publish pre-baked wire response in ChallengeStore (L3/RAM resident)
        let challenge_guard = challenge_store.publish(token, key_authorization);

        // 2. Inject ephemeral exact-path route into the Router table
        let route = Http1Route {
            id: ACME_CHALLENGE_ROUTE_ID,
            listener_id: self.listener_id,
            upstream_id: ACME_DUMMY_UPSTREAM_ID,
        };

        if let Ok(mut r) = self.router.write() {
            r.insert_http1_exact_route(self.listener_id, exact_path.clone(), route);
        }

        EphemeralRouteGuard {
            router: Arc::clone(&self.router),
            listener_id: self.listener_id,
            exact_path,
            _challenge_guard: challenge_guard,
        }
    }
}

/// RAII Scope Guard guaranteeing zero memory or route table leakage.
///
/// Automatically removes the ephemeral exact route from [`Router`] and unregisters
/// the pre-baked challenge response from [`ChallengeStore`] when dropped.
pub struct EphemeralRouteGuard {
    router: Arc<RwLock<Router>>,
    listener_id: ListenerId,
    exact_path: String,
    _challenge_guard: ChallengeGuard,
}

impl Drop for EphemeralRouteGuard {
    fn drop(&mut self) {
        if let Ok(mut r) = self.router.write() {
            r.remove_http1_exact_route(self.listener_id, &self.exact_path, ACME_CHALLENGE_ROUTE_ID);
        }
        tracing::debug!(
            path = %self.exact_path,
            "Ephemeral ACME HTTP-01 challenge route purged from router table"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_router::{Http1Route, Http1RouteRequest, Http1Router};

    #[test]
    fn test_ephemeral_route_guard_lifecycle() {
        let http1 = Http1Router::new(vec![Http1Route::new(
            RouteId::new(1),
            "listener-http",
            "/api",
            UpstreamId::new(10),
        )])
        .unwrap();

        let router = Arc::new(RwLock::new(Router::new(
            Default::default(),
            Default::default(),
            http1,
            Default::default(),
            Default::default(),
            Default::default(),
        )));

        let store = ChallengeStore::new();
        let registry = RouterRegistry::new(Arc::clone(&router), "listener-http");

        let token = "challenge_token_xyz";
        let path = format!("{CHALLENGE_PATH_PREFIX}{token}");

        // 1. Before registration: route does not match
        let req = Http1RouteRequest::new(&path);
        {
            let r = router.read().unwrap();
            assert!(r.route_http1("listener-http", &req).is_none());
        }

        // 2. Active registration: route matches and store has response
        {
            let _guard = registry.register_challenge(token, "token.thumbprint", &store);

            let r = router.read().unwrap();
            let matched = r.route_http1("listener-http", &req).unwrap();
            assert_eq!(matched.id, ACME_CHALLENGE_ROUTE_ID);

            let resp = store.lookup(&path).unwrap();
            assert!(resp.starts_with(b"HTTP/1.1 200 OK"));
        }

        // 3. After guard drop: route is unconditionally removed and store is empty
        {
            let r = router.read().unwrap();
            assert!(r.route_http1("listener-http", &req).is_none());
            assert!(store.is_empty());
        }
    }
}
