//! Global Route Catalog for Access Log Formatting (`velda_observability::logs::catalog`).
//!
//! # Mechanism & Architecture
//! Frontline traffic workers only store compact `u16` route IDs in the 256-byte
//! [`super::AccessLogRecord`] to preserve cache line alignment and zero-alloc hot path.
//!
//! During batch log emission on the shadow logistics worker (`velda-shadow-{id}`),
//! [`with_route_name`] maps `route_id: u16` to its human-readable identifier (e.g. `"http-plain"`,
//! `"grpc-all"`) directly from CPU L1D cache without heap allocation.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

static ROUTE_CATALOG: LazyLock<RwLock<HashMap<u16, Box<str>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Registers a single route ID to its human-readable name in the global route catalog.
pub fn register_route_name(id: u16, name: &str) {
    if let Ok(mut lock) = ROUTE_CATALOG.write() {
        lock.insert(id, name.into());
    }
}

/// Atomically replaces all route catalog entries during config load or reload.
pub fn set_route_catalog<I, S>(entries: I)
where
    I: IntoIterator<Item = (u16, S)>,
    S: AsRef<str>,
{
    if let Ok(mut lock) = ROUTE_CATALOG.write() {
        lock.clear();
        for (id, name) in entries {
            lock.insert(id, name.as_ref().into());
        }
    }
}

/// Executes a closure with the route name string corresponding to `route_id`.
///
/// If `route_id == 0` or if the route ID is not found, passes `"unmatched"`.
#[inline]
pub fn with_route_name<R>(id: u16, f: impl FnOnce(&str) -> R) -> R {
    if id == 0 {
        return f("unmatched");
    }
    if let Ok(lock) = ROUTE_CATALOG.read()
        && let Some(name) = lock.get(&id)
    {
        return f(name);
    }
    f("unmatched")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_catalog_lookup() {
        set_route_catalog([(10, "route-http"), (20, "route-grpc")]);

        with_route_name(10, |s| assert_eq!(s, "route-http"));
        with_route_name(20, |s| assert_eq!(s, "route-grpc"));
        with_route_name(0, |s| assert_eq!(s, "unmatched"));
        with_route_name(999, |s| assert_eq!(s, "unmatched"));
    }
}
