//! Request lifecycle and extension contracts for Velda Edge.
//!
//! The lifecycle describes how a request moves through the data plane.
//! `velda-core` defines the contract only; the actual execution is
//! driven by the traffic and protocol engines (`velda-transport` and `velda-http`).
//!
//! High-level lifecycle:
//!
//! ```text
//!     ACCEPT
//!        |
//!        v
//!     DECODE
//!        |
//!        v
//!     PRE_ROUTE      <- L7 hooks
//!        |
//!        v
//!     ROUTE
//!        |
//!        v
//!     PRE_UPSTREAM   <- L7 hooks
//!        |
//!        v
//!     UPSTREAM
//!        |
//!        v
//!     POST_RESPONSE  <- L7 hooks
//! ```
//!
//! Transport-level processing such as TCP accept, TLS negotiation,
//! and HTTP decoding is not implemented here.

use std::future::Future;

use crate::{context::RequestContext, error::Error, l7::response::L7Response};

/// A phase in the Velda request lifecycle.
///
/// Not every lifecycle phase is hookable. `PreRoute`, `PreUpstream`,
/// and `PostResponse` are the primary extension points for plugins.
///
/// The remaining phases are controlled by the data-plane engine itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// A new L4 connection has been accepted.
    Accept,

    /// Transport/protocol data is being decoded into a request model.
    Decode,

    /// Request-level policy is evaluated before route matching.
    ///
    /// Typical hooks:
    /// - authentication
    /// - client IP access control
    /// - rate limiting
    /// - request normalization
    PreRoute,

    /// The router determines which configured route matches the request.
    Route,

    /// Request-level policy is evaluated after routing and before
    /// sending the request to the selected upstream.
    ///
    /// Typical hooks:
    /// - WAF
    /// - route-specific authorization
    /// - header mutation
    /// - upstream policy
    PreUpstream,

    /// The request is executed against the selected upstream.
    Upstream,

    /// The upstream response has been received and can be processed
    /// before encoding it back to the client.
    ///
    /// Typical hooks:
    /// - security headers
    /// - response transformation
    /// - access logging
    /// - response metrics
    PostResponse,
}

impl Phase {
    /// Returns `true` when the phase accepts L7 hooks.
    #[inline]
    pub const fn is_l7_hook_phase(self) -> bool {
        matches!(
            self,
            Self::PreRoute | Self::PreUpstream | Self::PostResponse
        )
    }

    /// Returns `true` when the phase is controlled by the core
    /// data-plane processing rather than a plugin.
    #[inline]
    pub const fn is_engine_phase(self) -> bool {
        matches!(
            self,
            Self::Accept | Self::Decode | Self::Route | Self::Upstream
        )
    }
}

/// Result returned by an L7 hook.
///
/// A hook does not directly control the whole proxy pipeline.
/// It can only request one of the allowed actions below.
#[derive(Debug)]
pub enum Action {
    /// Continue processing the request with the next hook or phase.
    Continue,

    /// Stop the normal lifecycle and return a response directly
    /// to the client.
    Respond(L7Response),

    /// Stop processing because the request cannot continue.
    ///
    /// The proxy is responsible for translating the error into
    /// an appropriate client-facing response.
    Reject(Error),
}

impl Action {
    /// Constructs a `Continue` action.
    #[inline]
    pub const fn r#continue() -> Self {
        Self::Continue
    }

    /// Constructs a `Respond` action with the given L7 HTTP response.
    #[inline]
    pub fn respond(response: L7Response) -> Self {
        Self::Respond(response)
    }

    /// Constructs a `Reject` action with the given core error.
    #[inline]
    pub fn reject(error: Error) -> Self {
        Self::Reject(error)
    }

    /// Returns `true` when processing should continue.
    #[inline]
    pub const fn is_continue(&self) -> bool {
        matches!(self, Self::Continue)
    }

    /// Returns `true` when the hook produced a direct response.
    #[inline]
    pub const fn is_response(&self) -> bool {
        matches!(self, Self::Respond(_))
    }

    /// Returns `true` when the hook rejected the request.
    #[inline]
    pub const fn is_rejected(&self) -> bool {
        matches!(self, Self::Reject(_))
    }
}

/// L7 plugin extension point.
///
/// Implementations are provided by crates such as `velda-plugin`.
///
/// The hook receives a mutable [`RequestContext`] so it can inspect
/// the request and update request state, but it does not own or
/// orchestrate the complete request lifecycle.
pub trait Hook: Send + Sync {
    /// Returns a stable name for the hook.
    ///
    /// This is useful for configuration, debugging, metrics,
    /// tracing, and administration APIs.
    fn name(&self) -> &'static str;

    /// Executes the hook against the current request context.
    fn handle(&self, ctx: &mut RequestContext) -> impl Future<Output = Action> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookPhase {
    /// Runs before route matching.
    PreRoute,

    /// Runs after route selection and before upstream execution.
    PreUpstream,

    /// Runs after an upstream response is received.
    PostResponse,
}

impl HookPhase {
    /// Maps the hook phase to the corresponding lifecycle phase.
    #[inline]
    pub const fn lifecycle_phase(self) -> Phase {
        match self {
            Self::PreRoute => Phase::PreRoute,
            Self::PreUpstream => Phase::PreUpstream,
            Self::PostResponse => Phase::PostResponse,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use http::StatusCode;

    #[test]
    fn test_phase_properties() {
        assert!(Phase::PreRoute.is_l7_hook_phase());
        assert!(Phase::PreUpstream.is_l7_hook_phase());
        assert!(Phase::PostResponse.is_l7_hook_phase());
        assert!(!Phase::Route.is_l7_hook_phase());
        assert!(Phase::Route.is_engine_phase());

        assert_eq!(HookPhase::PreRoute.lifecycle_phase(), Phase::PreRoute);
        assert_eq!(HookPhase::PreUpstream.lifecycle_phase(), Phase::PreUpstream);
        assert_eq!(
            HookPhase::PostResponse.lifecycle_phase(),
            Phase::PostResponse
        );
    }

    #[test]
    fn test_action_constructors_and_inspectors() {
        let c = Action::r#continue();
        assert!(c.is_continue());
        assert!(!c.is_response());
        assert!(!c.is_rejected());

        let r = Action::respond(L7Response::empty(StatusCode::OK));
        assert!(!r.is_continue());
        assert!(r.is_response());
        assert!(!r.is_rejected());

        let rej = Action::reject(Error::new(ErrorKind::Rejected, "blocked by test"));
        assert!(!rej.is_continue());
        assert!(!rej.is_response());
        assert!(rej.is_rejected());
    }
}
