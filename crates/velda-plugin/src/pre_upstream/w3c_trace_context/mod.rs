//! W3C TraceContext Propagation Pre-Upstream Plugin Module (`w3c_trace_context`).

pub mod plugin;
pub mod traceparent;

pub use plugin::{W3cTraceContextConfig, W3cTraceContextPlugin};
pub use traceparent::{ParsedTraceParent, TraceParentCodec};
