//! Common error model for the Velda data plane.
//!
//! `velda-core` defines the shared error contract used across the
//! request lifecycle. Concrete subsystems such as routing, upstream,
//! TLS, transport, and plugins may define their own internal errors
//! and convert them into this type when crossing crate boundaries.
//!
//! The core error model intentionally does not contain HTTP-specific
//! response generation logic. Translating an error into an HTTP
//! response is the responsibility of the appropriate data-plane layer.

use std::{error::Error as StdError, fmt};

/// Broad category of a Velda error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The client request or connection data is malformed.
    InvalidRequest,

    /// A protocol violation occurred.
    Protocol,

    /// The requested route does not exist.
    RouteNotFound,

    /// Route configuration is invalid or inconsistent.
    RouteConfig,

    /// An upstream could not be selected or reached.
    UpstreamUnavailable,

    /// An upstream operation failed.
    UpstreamFailure,

    /// An operation exceeded its configured deadline.
    Timeout,

    /// A connection-level operation failed.
    Connection,

    /// A plugin rejected the request.
    Rejected,

    /// A plugin or subsystem was canceled before completion.
    Canceled,

    /// An internal implementation error.
    Internal,
}

impl ErrorKind {
    /// Returns whether the error is primarily caused by the client.
    pub const fn is_client_error(self) -> bool {
        matches!(self, Self::InvalidRequest | Self::Protocol | Self::Rejected)
    }

    /// Returns whether the error is related to an upstream.
    pub const fn is_upstream_error(self) -> bool {
        matches!(
            self,
            Self::UpstreamUnavailable | Self::UpstreamFailure | Self::Timeout
        )
    }

    /// Returns whether the operation may have been canceled.
    pub const fn is_canceled(self) -> bool {
        matches!(self, Self::Canceled)
    }
}

/// Shared Velda error type.
///
/// This is the error contract that can safely cross crate boundaries.
///
/// Subsystems may keep richer internal error types and convert them
/// into `Error` only when they need to communicate with higher layers.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl Error {
    /// Creates a new error without an underlying source.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Creates a new error with an underlying source error.
    pub fn with_source<E>(kind: ErrorKind, message: impl Into<String>, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Returns the error category.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the underlying source error, if any.
    pub fn source_error(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidRequest => "invalid request",
            Self::Protocol => "protocol error",
            Self::RouteNotFound => "route not found",
            Self::RouteConfig => "route configuration error",
            Self::UpstreamUnavailable => "upstream unavailable",
            Self::UpstreamFailure => "upstream failure",
            Self::Timeout => "timeout",
            Self::Connection => "connection error",
            Self::Rejected => "request rejected",
            Self::Canceled => "canceled",
            Self::Internal => "internal error",
        };

        f.write_str(name)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::with_source(ErrorKind::Connection, "I/O operation failed", value)
    }
}

/// Specialized `Result` type for operations that can produce a `velda_core::Error`.
pub type Result<T> = std::result::Result<T, Error>;
