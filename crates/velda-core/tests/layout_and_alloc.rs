//! Layout and zero-allocation assertions for velda-core.
//!
//! Hot-path entities in velda-core must fit within strict memory budgets
//! and operations like querying or mutating state must be strictly zero-allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

use http::{Method, Uri, Version};
use velda_core::l4::request::TransportProtocol;
use velda_core::l7::request::Body;
use velda_core::{
    Action, ErrorKind, HeaderWorkspace, HookPhase, L4Action, L4Request, L7Request, Phase,
    RequestContext, RequestState, UNASSIGNED_UPSTREAM_ID, UNMATCHED_ROUTE_ID,
};

/// Custom counting allocator to verify zero heap allocation on the hot path.
struct CountingAllocator {
    alloc_count: AtomicUsize,
    dealloc_count: AtomicUsize,
}

thread_local! {
    static TRACKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl CountingAllocator {
    const fn new() -> Self {
        Self {
            alloc_count: AtomicUsize::new(0),
            dealloc_count: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.alloc_count.store(0, Ordering::SeqCst);
        self.dealloc_count.store(0, Ordering::SeqCst);
    }

    fn allocations(&self) -> usize {
        self.alloc_count.load(Ordering::SeqCst)
    }

    fn enable_tracking() {
        TRACKING.with(|t| t.set(true));
    }

    fn disable_tracking() {
        TRACKING.with(|t| t.set(false));
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.with(|t| t.get()) {
            self.alloc_count.fetch_add(1, Ordering::SeqCst);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.with(|t| t.get()) {
            self.dealloc_count.fetch_add(1, Ordering::SeqCst);
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: CountingAllocator = CountingAllocator::new();

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn type_sizes() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // 1. Primitive IDs sizes
    assert_eq!(size_of::<u128>(), 16);
    assert_eq!(size_of::<u64>(), 8);
    assert_eq!(size_of::<u16>(), 2);

    // 2. RequestState must be compact and easily fit in a single 64-byte L1 Cacheline!
    // request_id (8) + route Option<u32> (8) + upstream Option<u32> (8) + 3 bools (3) + padding = 32 bytes
    assert!(
        size_of::<RequestState>() <= 64,
        "RequestState size ({} bytes) must be <= 64 bytes (single cacheline)",
        size_of::<RequestState>()
    );

    // 3. Fieldless enums are 1 byte
    assert_eq!(size_of::<Phase>(), 1);
    assert_eq!(size_of::<HookPhase>(), 1);
    assert_eq!(size_of::<L4Action>(), 1);
    assert_eq!(size_of::<ErrorKind>(), 1);
}

#[test]
fn zero_alloc_hot_path() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    A.reset();
    CountingAllocator::enable_tracking();

    // 1. Creating and querying flat integer IDs
    let req_id: u128 = 42;
    let route_id: u16 = 100;
    let upstream_id: u16 = 200;

    assert_eq!(req_id, 42);
    assert_eq!(route_id, 100);
    assert_eq!(upstream_id, 200);
    assert_eq!(A.allocations(), 0, "ID operations must not allocate");

    // 2. RequestState operations (Creation, state mutations, getters)
    let mut state = RequestState::new(req_id);
    assert_eq!(state.request_id, req_id);
    assert!(!state.routed);
    assert_eq!(state.route_id, UNMATCHED_ROUTE_ID);
    assert_eq!(state.upstream_id, UNASSIGNED_UPSTREAM_ID);
    assert!(!state.upstream_started);
    assert!(!state.upstream_completed);

    state.set_route(route_id);
    assert!(state.routed);
    assert_eq!(state.route_id, route_id);

    state.set_upstream(upstream_id);
    assert_eq!(state.upstream_id, upstream_id);

    state.mark_upstream_started();
    assert!(state.upstream_started);

    state.mark_upstream_completed();
    assert!(state.upstream_completed);

    assert_eq!(
        A.allocations(),
        0,
        "RequestState lifecycle transitions must be strictly zero-allocation"
    );

    // 3. Action::Continue, Phase comparisons
    let action = Action::Continue;
    match action {
        Action::Continue => {}
        _ => panic!("Expected Continue"),
    }

    assert_eq!(
        A.allocations(),
        0,
        "Lifecycle action evaluation must not allocate"
    );

    CountingAllocator::disable_tracking();
}

#[test]
fn test_zero_allocation_context_lookups() {
    // Setup stack data
    let client_ip = velda_core::lut::ip::v4_to_u128(0x7F00_0001);
    let client_port_be = 45678u16.to_be();
    let local_ip = velda_core::lut::ip::v4_to_u128(0x7F00_0001);
    let local_port_be = 80u16.to_be();

    let l4_req = L4Request::new(
        1,
        TransportProtocol::Tcp,
        client_ip,
        client_port_be,
        local_ip,
        local_port_be,
    );

    let l7_req = L7Request::new(
        Method::GET,
        Uri::from_static("/healthz"),
        Version::HTTP_11,
        HeaderWorkspace::empty(0),
        Body::Empty,
    );

    let mut req_ctx = RequestContext::new(l4_req, l7_req, 99);

    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Reset allocation counter before testing hot-path context read/write
    A.reset();
    CountingAllocator::enable_tracking();

    // Hot-path reading
    assert_eq!(req_ctx.state.request_id, 99);
    assert_eq!(req_ctx.l4.peer_ip, client_ip);
    assert_eq!(req_ctx.state.route_id, UNMATCHED_ROUTE_ID);
    assert_eq!(req_ctx.state.upstream_id, UNASSIGNED_UPSTREAM_ID);

    // Hot-path mutating
    req_ctx.state.set_route(1);
    req_ctx.state.set_upstream(5);

    assert_eq!(req_ctx.state.route_id, 1);
    assert_eq!(req_ctx.state.upstream_id, 5);

    assert_eq!(
        A.allocations(),
        0,
        "RequestContext lookups and mutations on pre-created contexts must be 0 allocations"
    );

    CountingAllocator::disable_tracking();
}
