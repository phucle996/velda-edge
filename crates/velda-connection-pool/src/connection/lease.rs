//! Unified connection lease enum dispatching to the specific profile lease.

use std::fmt;
use std::hash::Hash;
use std::ops::Deref;

use crate::connection::exclusive::ExclusiveLease;
use crate::connection::multiplexed::StreamLease;
use crate::connection::sequential::SequentialLease;
use crate::resource::PoolableResource;

/// An active RAII lease returned by [`crate::PoolManager`] matching the profile's reuse mode.
///
/// Implements [`Deref`] to `R`, allowing callers to transparently access the underlying connection.
/// When dropped, executes the exact release semantics of the profile:
/// - [`ConnectionLease::Sequential`]: Returned to LIFO queue for the next sequential request.
/// - [`ConnectionLease::Exclusive`]: Closed or returned via hook to protect socket state.
/// - [`ConnectionLease::Multiplexed`]: Decrements the active stream counter on the shared connection.
pub enum ConnectionLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Sequential transactional lease (HTTP/1.1).
    Sequential(SequentialLease<K, R>),
    /// Exclusive single-borrower lease (Raw TCP / L4 Tunnel).
    Exclusive(ExclusiveLease<R>),
    /// Multiplexed stream slot lease (HTTP/2, HTTP/3).
    Multiplexed(StreamLease<R>),
}

impl<K, R> fmt::Debug for ConnectionLease<K, R>
where
    K: Eq + Hash + Clone + fmt::Debug,
    R: PoolableResource,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequential(l) => f
                .debug_tuple("ConnectionLease::Sequential")
                .field(l)
                .finish(),
            Self::Exclusive(l) => f
                .debug_tuple("ConnectionLease::Exclusive")
                .field(l)
                .finish(),
            Self::Multiplexed(l) => f
                .debug_tuple("ConnectionLease::Multiplexed")
                .field(l)
                .finish(),
        }
    }
}

impl<K, R> ConnectionLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Explicitly releases the connection lease before it naturally falls out of scope.
    ///
    /// - For [`Self::Sequential`]: If `reusable` is true and backend is healthy, returns to LIFO pool.
    /// - For [`Self::Exclusive`]: If `reusable` is true and hook is attached, invokes return callback; otherwise closes.
    /// - For [`Self::Multiplexed`]: Releases stream slot; if `reusable` is false, marks connection with GOAWAY.
    pub fn release(self, reusable: bool) {
        match self {
            Self::Sequential(l) => l.release(reusable),
            Self::Exclusive(l) => l.release(reusable),
            Self::Multiplexed(l) => {
                if !reusable {
                    l.multiplexed_connection().mark_goaway();
                }
                // drop(l) will naturally decrement active streams
            }
        }
    }
}

impl<K, R> Deref for ConnectionLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    type Target = R;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Sequential(l) => l.deref(),
            Self::Exclusive(l) => l.deref(),
            Self::Multiplexed(l) => l.deref(),
        }
    }
}
