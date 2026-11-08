//! # velda-router
//!
//! High-performance, in-memory routing engine for Velda Edge Data Plane.
//!
//! Provides lock-free Layer 4 (TCP, UDP), Layer 7 HTTP/1.1, HTTP/2, HTTP/3,
//! and Layer 7 gRPC (service/method) routing.
//! All routing structures are pre-compiled in RAM for zero-IO, zero-allocation request serving
//! with L1D cache line alignment and hardware register-adaptive dynamic dispatch.

pub mod error;
pub mod host;
pub mod l4;
pub mod l7;
pub mod method;
pub mod path;
pub mod router;

pub use error::RouterError;
pub use host::{
    COMPARE_LABELS_DUAL_FN, CompareLabelsDualFn, CompiledHost, HostLabelBlock, ReverseHostLabels,
    clean_host, compare_labels_dual_64, compare_labels_quad_32, eq_ignore_ascii_case_dual64_swar,
    host_specificity, swar_find_colon,
};
pub use l4::{TcpRoute, TcpRouter, UdpRoute, UdpRouter, UdpSessionAffinity};
pub use l7::{
    GrpcHotCandidate, GrpcRoute, GrpcRouteRequest, GrpcRouteRule, GrpcRouter, Http1HotCandidate,
    Http1Route, Http1RouteRequest, Http1RouteRule, Http1Router, Http2HotCandidate, Http2Route,
    Http2RouteRequest, Http2RouteRule, Http2Router, Http3HotCandidate, Http3Route,
    Http3RouteRequest, Http3RouteRule, Http3Router, ListenerGrpcRouter, ListenerHttp1Router,
    ListenerHttp2Router, ListenerHttp3Router,
};
pub use method::{
    MATCH_PACKED_METHODS_FN, MatchPackedMethodsFn, MethodMask, match_packed_methods,
    match_packed_methods_32, match_packed_methods_64,
};
pub use path::{
    COMMON_PREFIX_LEN_FN, CommonPrefixLenFn, PathPrefixMatcher, SlashBlockNode,
    SlashBlockPrefixTable, common_prefix_len, common_prefix_len_32, common_prefix_len_64,
    common_prefix_len_128, common_prefix_len_256, is_clean_path, normalize_path, prefetch_l1_keep,
    starts_with_prefix_swar,
};
pub use router::{Router, RouterBuilder};
