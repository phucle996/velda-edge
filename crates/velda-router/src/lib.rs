//! # velda-router
//!
//! High-performance, protocol-isolated in-memory routing engine for the Velda Edge Data Plane.
//!
//! Provides lock-free Layer 4 (TCP, UDP), Layer 7 HTTP/1.1, HTTP/2, HTTP/3,
//! and Layer 7 gRPC (service/method) routing.
//! All routing structures are pre-compiled in RAM for zero-IO, zero-allocation request serving
//! with L1D cache line alignment and hardware register-adaptive dynamic dispatch.
//!
//! ---
//!
//! ## Architectural Invariant: Elimination of Traditional Radix Tries
//!
//! ### The Fundamental Problem with Industry-Standard Radix Trees (Envoy, Kong, Pingora, Traefik)
//! Conventional edge gateways and reverse proxies (e.g. Envoy, Kong/OpenResty, Pingora, Traefik)
//! universally deploy pointer-chasing Radix Trees (Patricia Tries) or dynamic tree structures
//! for URI prefix and path resolution. While Radix Trees offer theoretical $O(K)$ lookup time,
//! their physical hardware execution characteristics are fundamentally misaligned with modern
//! out-of-order superscalar CPU microarchitectures:
//!
//! 1. **The Pointer-Chasing Memory Wall**:
//!    - In a classic Radix Tree, every branch and edge is represented by an independently heap-allocated
//!      node struct connected via pointers (`*const Node`, `Box<Node>`, or `Arc<Node>`).
//!    - When resolving an incoming path like `/api/v1/checkout/orders/items/42`, the CPU must dereference
//!      5 to 7 scattered memory addresses in sequential dependency.
//!    - If any intermediate node is not resident in CPU L1D/L2 cache, the core encounters a **Last-Level
//!      Cache (LLC) miss to main DRAM**, incurring a latency penalty of **$60\text{--}80\text{ ns}$ (200+ CPU clock cycles)**
//!      per dereference. The CPU's deep execution pipelines completely stall waiting for memory bus transactions.
//!
//! 2. **Cache Line Fragmentation & False Eviction**:
//!    - Standard trie node structures vary dynamically in size and are rarely aligned to hardware cache line
//!      boundaries (64 bytes on x86-64 and AArch64).
//!    - A single trie node can span two separate 64-byte cache lines ("split load"), doubling memory bus transactions.
//!    - High concurrency across multiple worker threads leads to cache line ping-ponging and eviction of hot routing
//!      state by logging or payload buffers.
//!
//! 3. **Branch Predictor Thrashing in String Scans**:
//!    - Traditional trie traversal scans path strings byte-by-byte in tight conditional loops (`while *p == *s`).
//!    - When traversing deep or diverse paths, CPU Branch Target Buffers (BTB) suffer severe mispredictions,
//!      incurring **$15\text{--}20\text{ cycle}$ pipeline flushes** per mispredicted branch.
//!
//! ---
//!
//! ## Velda Edge's Architectural Divergence: Flat Slash-Block Table & SWAR Integer Blocks
//!
//! Velda Edge deliberately **eliminates pointer-chasing Radix Trees** from the request serving hot path.
//! In their place, `velda-router` introduces a hardware-conscious, cache-resident routing architecture:
//!
//! ```text
//! Incoming Path: "/api/v1/orders/12345/details"
//!                      │
//!                      ▼
//!        ┌───────────────────────────┐
//!        │   Tier 1: Exact Match     │ ───[HIT]───► 1-Cycle Method & Host Check ──► Route Found
//!        │  (O(1) FxHashMap Lookup)  │
//!        └─────────────┬─────────────┘
//!                      │ [MISS]
//!                      ▼
//!        ┌───────────────────────────┐
//!        │   Tier 2: Prefix Match    │
//!        │  (Flat Slash-Block Table) │ ───[HIT]───► Longest Prefix Match ──► Route Found
//!        │   O(P) SWAR Integer Word  │
//!        └─────────────┬─────────────┘
//!                      │ [MISS]
//!                      ▼
//!          Deterministic None (404)
//! ```
//!
//! ### Core Microarchitectural Pillars:
//!
//! 1. **Flat Contiguous Node Arena (`Vec<SlashBlockNode>`)**:
//!    - All routing prefix rules are compiled offline/at bootstrap via Breadth-First Search (BFS) into
//!      a single, contiguous memory arena (`SlashBlockPrefixTable`).
//!    - Sibling nodes belonging to any parent are arranged sequentially in memory (`children_start .. children_start + children_count`).
//!    - Node transitions operate purely on `u32` array indices. **Zero pointers (`*const`, `Box`), zero heap allocations,
//!      and zero pointer chasing** occur during route matching.
//!
//! 2. **Strict 64-Byte L1D Cache Line Alignment (`SlashBlockNode`)**:
//!    - Every [`SlashBlockNode`] is declared `#[repr(C, align(64))]` and sized to exactly 64 bytes.
//!    - When a worker thread accesses a node, the CPU memory subsystem fetches the entire node in a **single
//!      atomic 64-byte L1D cache transaction**, guaranteeing zero split-line loads and zero bus serialization stalls.
//!
//! 3. **64-bit Integer Head Word Matching (`head8: u64`)**:
//!    - The first 8 bytes of every prefix segment are pre-packed into a 64-bit little-endian integer (`head8`).
//!    - Non-matching branches are rejected in **1 single CPU clock cycle** using a bitwise XOR integer instruction:
//!      $$\text{diff} = \text{path\_u64} \oplus \text{node.head8}$$
//!      If $\text{diff} \ne 0$, the candidate branch is discarded immediately without executing string loops.
//!
//! 4. **Branchless SWAR Prefix Equality (`starts_with_prefix_swar`)**:
//!    - Prefix validation executes via Length-Guarded SIMD-Within-A-Register (SWAR):
//!      - $\le 4\text{ bytes}$: Evaluated directly in `u16` or `u32` General-Purpose Registers (GPR).
//!      - $5\text{--}8\text{ bytes}$: Overlapping 32-bit SWAR (evaluating bytes `0..4` and `n-4..n` concurrently in 2 ALU cycles).
//!      - $9\text{--}16\text{ bytes}$: Overlapping 64-bit SWAR (evaluating bytes `0..8` and `n-8..n` in 2 ALU cycles).
//!      - $> 16\text{ bytes}$: Dual 64-bit SWAR loop comparing 16 bytes per cycle across ALU Port 0 & ALU Port 1.
//!
//! 5. **Dynamic Hardware Multi-Versioning Registers**:
//!    - Probed and bound once at boot across 4 hardware tiers:
//!      - `Bits256`: 256-bit AVX2 vector registers scanning 32 bytes per cycle.
//!      - `Bits128`: 128-bit SSE2 / NEON vector registers scanning 16 bytes per cycle.
//!      - `Bits64`: Dual 64-bit SWAR integer registers.
//!      - `Bits32`: Quad 32-bit SWAR registers executing across 4 ALU ports concurrently.
//!    - **Strict Invariant**: Zero AVX-512 instructions (zero `%zmm` registers) to prevent Intel core frequency downclocking.
//!
//! 6. **Workload-Adaptive Listener Specialization**:
//!    - Small route tables ($\le 3$ routes): Bypasses all tables and hashing, resolving routes via unrolled
//!      scalar register comparisons directly in CPU GPRs (`%rax`, `%rcx`, `%rdx`).
//!    - Large route tables ($> 3$ routes): Evaluates Tier 1 $O(1)$ exact match before falling back to Tier 2
//!      flat Slash-Block prefix table with hardware temporal prefetching (`prefetch_l1_keep`).

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
    clean_host, compare_labels_dual, compare_labels_dual_64, eq_ignore_ascii_case_dual64_swar,
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
    match_packed_methods_64,
};
pub use path::{
    COMMON_PREFIX_LEN_FN, CommonPrefixLenFn, PathPrefixMatcher, SlashBlockNode,
    SlashBlockPrefixTable, common_prefix_len, common_prefix_len_64, common_prefix_len_128,
    common_prefix_len_256, is_clean_path, normalize_path, prefetch_l1_keep,
    starts_with_prefix_swar,
};
pub use router::{Router, RouterBuilder};
