//! Static Bootstrap ACME Route Constants & Challenge Lifecycle Guard.
//!
//! The `/.well-known/acme-challenge/` prefix route is pre-compiled into the immutable
//! 4KB page-aligned [`RouteArena`](velda_router::arena::RouteArena) at bootstrap with
//! [`ACME_CHALLENGE_ROUTE_ID`]. At runtime, ACME HTTP-01 challenges only publish and
//! evict pre-baked wire responses in [`ChallengeStore`](super::challenge::ChallengeStore)
//! via RAII [`ChallengeGuard`](super::challenge::ChallengeGuard), requiring zero writes
//! or locks on `Router`.

/// Route identifier reserved strictly for ACME HTTP-01 challenge matching.
pub const ACME_CHALLENGE_ROUTE_ID: u16 = u16::MAX;

/// Dummy upstream identifier used because ACME challenges short-circuit directly in memory.
pub const ACME_DUMMY_UPSTREAM_ID: u16 = 0;

#[cfg(test)]
mod tests {
    use super::super::challenge::{CHALLENGE_PATH_PREFIX, ChallengeStore};
    use super::*;
    use velda_core::fnv1a_hash_32;
    use velda_router::{Http1Route, Http1RouteRequest, Http1Router, Router};

    #[test]
    fn test_static_bootstrap_acme_route_and_challenge_store_lifecycle() {
        let lid = fnv1a_hash_32("listener-http");
        let http1 = Http1Router::new(vec![
            Http1Route::new(1, lid, "/api"),
            Http1Route::new(ACME_CHALLENGE_ROUTE_ID, lid, CHALLENGE_PATH_PREFIX).with_method("GET"),
        ])
        .unwrap();

        let router = Router::new(
            Default::default(),
            Default::default(),
            http1,
            Default::default(),
            Default::default(),
            Default::default(),
        );

        let store = ChallengeStore::new();
        let token = "challenge_token_xyz";
        let path = format!("{CHALLENGE_PATH_PREFIX}{token}");
        let req = Http1RouteRequest::new(&path).with_method("GET");

        // Static bootstrap route is always present in immutable 4KB RouteArena
        let matched = router.route_http1(lid, &req).unwrap();
        assert_eq!(matched.id, ACME_CHALLENGE_ROUTE_ID);

        // Before publishing: ChallengeStore has no entry
        assert!(store.lookup(&path).is_none());

        // Active challenge: ChallengeGuard publishes response into ChallengeStore
        {
            let _guard = store.publish(token, "token.thumbprint");
            let resp = store.lookup(&path).unwrap();
            assert!(resp.starts_with(b"HTTP/1.1 200 OK"));
        }

        // After guard drop: ChallengeStore is unconditionally purged; Router remains immutable
        assert!(store.is_empty());
        assert_eq!(
            router.route_http1(lid, &req).unwrap().id,
            ACME_CHALLENGE_ROUTE_ID
        );
    }
}
