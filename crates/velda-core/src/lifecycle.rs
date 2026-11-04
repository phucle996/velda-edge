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
use std::time::Duration;

use http::{HeaderValue, StatusCode};

use crate::{
    buffer::HeaderWorkspace,
    context::RequestContext,
    error::RejectReason,
    l7::{request::Body, response::L7Response},
};

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

/// Pre-computed cache metadata and in-memory payload directive returned on Cache HIT.
///
/// Designed to eliminate intermediate [`http::HeaderMap`] and [`L7Response`] heap allocations
/// on the pre-upstream cache lookup path. The downstream pipeline can either directly serialize
/// these pre-computed HTTP headers onto the wire or instantiate an [`L7Response`] via
/// [`into_l7_response`](Self::into_l7_response) when post-response hooks (e.g. logging) are registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedResponseDirective {
    /// Cached HTTP response status code (e.g. 200 OK, 304 Not Modified).
    pub status: StatusCode,
    /// In-memory zero-copy cached payload slice.
    pub payload: bytes::Bytes,
    /// Cached `Content-Type` header if present.
    pub content_type: Option<HeaderValue>,
    /// Content encoding variant (e.g. `gzip`, `br`, `zstd`) if negotiated.
    pub encoding: Option<HeaderValue>,
    /// Age of the cache entry in seconds.
    pub age_secs: u64,
    /// Whether the cached entry possesses compressed variants (requiring `Vary: Accept-Encoding`).
    pub has_variants: bool,
}

impl CachedResponseDirective {
    /// Constructs a new [`CachedResponseDirective`].
    #[inline]
    pub fn new(
        status: StatusCode,
        payload: bytes::Bytes,
        content_type: Option<HeaderValue>,
        encoding: Option<HeaderValue>,
        age_secs: u64,
        has_variants: bool,
    ) -> Self {
        Self {
            status,
            payload,
            content_type,
            encoding,
            age_secs,
            has_variants,
        }
    }

    /// Converts this cached directive into an [`L7Response`] with standard HTTP cache headers
    /// populated in a pre-warmed [`HeaderWorkspace`].
    pub fn into_l7_response(self) -> L7Response {
        let mut resp_headers = HeaderWorkspace::with_capacity(6);
        if let Some(ct) = self.content_type {
            resp_headers.insert(http::header::CONTENT_TYPE, ct);
        }
        resp_headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from(self.payload.len()),
        );
        resp_headers.insert(
            http::header::HeaderName::from_static("age"),
            HeaderValue::from(self.age_secs),
        );
        resp_headers.insert(
            http::header::HeaderName::from_static("x-cache"),
            HeaderValue::from_static("HIT"),
        );
        if let Some(enc) = self.encoding {
            resp_headers.insert(http::header::CONTENT_ENCODING, enc);
            resp_headers.insert(
                http::header::VARY,
                HeaderValue::from_static("Accept-Encoding"),
            );
        } else if self.has_variants {
            resp_headers.insert(
                http::header::VARY,
                HeaderValue::from_static("Accept-Encoding"),
            );
        }

        L7Response::new(
            self.status,
            http::Version::HTTP_11,
            resp_headers,
            Body::Bytes(self.payload),
        )
    }
}

/// Bounded execution authority returned by L7 plugin hooks.
///
/// # Architectural Invariant: Finite Extension Authority
/// Hooks in Velda Edge do not have raw access to network sockets, background loops,
/// or connection pools. They are strictly bounded to this finite state machine:
///
/// 1. `Continue`: Advances the request to the subsequent hook or pipeline phase.
/// 2. `Respond(L7Response)`: Short-circuits immediately with an explicit, caller-provided
///    HTTP response. Used strictly when the plugin supplies explicit body content, custom
///    non-error status codes (e.g. CORS `204 No Content`).
/// 3. `Reject(RejectReason)`: Canonical edge rejection for policy/security failures.
///    Synthesizes zero-alloc protocol-native error framing downstream and strips Bit 0 headers.
/// 4. `Delay { duration, and_then }`: Non-blocking asynchronous sleep (used for chaos fault
///    injection / artificial latency) before evaluating the boxed `and_then` action.
/// 5. `ServeCached(CachedResponseDirective)`: Fast-path cache directive returning pre-computed
///    response metadata and in-memory payload without intermediate `HeaderMap` heap allocations.
///
/// # Memory Layout
/// `and_then` is boxed inside `Delay` to ensure that `Action` remains small and cacheline-friendly
/// across the 99.9% non-delay request path.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Action {
    /// Continue processing the request with the next hook or phase.
    Continue,

    /// Stop the normal lifecycle and return a response directly
    /// to the client.
    Respond(L7Response),

    /// Stop processing because the request cannot continue.
    ///
    /// The proxy is responsible for translating the canonical rejection
    /// reason into an appropriate client-facing response.
    Reject(RejectReason),

    /// Delay execution for the specified duration before continuing with `and_then`.
    Delay {
        duration: Duration,
        and_then: Box<Action>,
    },

    /// Serve pre-computed cached payload directly from in-memory storage,
    /// bypassing upstream roundtrips and intermediate `HeaderMap` allocations.
    ServeCached(CachedResponseDirective),
}

impl Action {
    /// Constructs a `Continue` action.
    #[inline]
    pub const fn r#continue() -> Self {
        Self::Continue
    }

    /// Constructs a `Delay` action that pauses execution non-blocking for `duration`
    /// before evaluating `and_then`.
    #[inline]
    pub fn delay(duration: Duration, and_then: Action) -> Self {
        Self::Delay {
            duration,
            and_then: Box::new(and_then),
        }
    }

    /// Constructs a `Respond` action with the given L7 HTTP response.
    #[inline]
    pub fn respond(response: L7Response) -> Self {
        Self::Respond(response)
    }

    /// Constructs a `Reject` action with the given canonical rejection reason.
    #[inline]
    pub const fn reject(reason: RejectReason) -> Self {
        Self::Reject(reason)
    }

    /// Constructs a `ServeCached` action with the given cached response directive.
    #[inline]
    pub fn serve_cached(directive: CachedResponseDirective) -> Self {
        Self::ServeCached(directive)
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

    /// Returns `true` when the action requires a delay before proceeding.
    #[inline]
    pub const fn is_delay(&self) -> bool {
        matches!(self, Self::Delay { .. })
    }

    /// Returns `true` when the action serves a cached response directive.
    #[inline]
    pub const fn is_serve_cached(&self) -> bool {
        matches!(self, Self::ServeCached(_))
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

        let rej = Action::reject(RejectReason::Forbidden);
        assert!(!rej.is_continue());
        assert!(!rej.is_response());
        assert!(rej.is_rejected());
        assert!(!rej.is_delay());

        let d = Action::delay(Duration::from_millis(100), Action::r#continue());
        assert!(!d.is_continue());
        assert!(!d.is_response());
        assert!(!d.is_rejected());
        assert!(d.is_delay());
        assert!(!d.is_serve_cached());

        let cached = CachedResponseDirective::new(
            StatusCode::OK,
            bytes::Bytes::from_static(b"hello cached"),
            Some(HeaderValue::from_static("text/plain")),
            Some(HeaderValue::from_static("gzip")),
            120,
            true,
        );
        let serve = Action::serve_cached(cached.clone());
        assert!(!serve.is_continue());
        assert!(!serve.is_response());
        assert!(!serve.is_rejected());
        assert!(!serve.is_delay());
        assert!(serve.is_serve_cached());

        let l7 = cached.into_l7_response();
        assert_eq!(l7.status, StatusCode::OK);
        assert_eq!(l7.headers.get("x-cache").unwrap(), "HIT");
        assert_eq!(l7.headers.get("age").unwrap(), "120");
        assert_eq!(
            l7.headers.get(http::header::CONTENT_ENCODING).unwrap(),
            "gzip"
        );
        assert_eq!(
            l7.headers.get(http::header::VARY).unwrap(),
            "Accept-Encoding"
        );
    }
}
