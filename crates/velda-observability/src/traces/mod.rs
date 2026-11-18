//! Lock-Free 64-Byte Cacheline Trace Span Subsystem (`velda_observability::traces`).
//!
//! ```text
//!  Frontline Worker           SpscSpanRing (256KB/core)        Shadow Worker           TraceStore
//!  ┌───────────────┐          ┌─────────────────────┐          ┌─────────────┐        ┌──────────────┐
//!  │ SpanRecord    │──push───►│ 64B Aligned Slots   │──drain──►│ Arc<[Span]> │──fan──►│ K Exporters  │
//!  │ (Root Server) │ (NT-st)  │ (Head / Tail index) │ (NT-ld)  │ (Ref-count) │ -out   │ (Subscribers)│
//!  └───────────────┘          └─────────────────────┘          └─────────────┘        └──────────────┘
//! ```

mod record;
mod ring;
mod store;

pub use record::SpanRecord;
pub use ring::{DEFAULT_TRACE_CORE_RING_CAPACITY, SpscSpanRing};
pub use store::{TraceStore, TraceSubscriber, global_trace_store};
