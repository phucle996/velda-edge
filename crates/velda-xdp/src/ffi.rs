//! # Linux UAPI Bindings & Structs for XDP, AF_XDP & eBPF
//!
//! Provides direct bindings to `<linux/if_xdp.h>`, `<linux/bpf.h>`, and `<linux/if_link.h>`.

#![allow(non_camel_case_types)]
#![allow(dead_code)]

/// Protocol family for AF_XDP (Linux 4.18+).
pub const AF_XDP: i32 = 44;
/// Protocol level for AF_XDP socket options.
pub const SOL_XDP: i32 = 283;

// Socket options for SOL_XDP
pub const XDP_MMAP_OFFSETS: i32 = 1;
pub const XDP_RX_RING: i32 = 2;
pub const XDP_TX_RING: i32 = 3;
pub const XDP_UMEM_REG: i32 = 4;
pub const XDP_UMEM_FILL_RING: i32 = 5;
pub const XDP_UMEM_COMPLETION_RING: i32 = 6;
pub const XDP_STATISTICS: i32 = 7;
pub const XDP_OPTIONS: i32 = 8;

// XDP attachment flags for netlink / BPF attachment
pub const XDP_FLAGS_UPDATE_IF_NOEXIST: u32 = 1 << 0;
pub const XDP_FLAGS_SKB_MODE: u32 = 1 << 1;
pub const XDP_FLAGS_DRV_MODE: u32 = 1 << 2;
pub const XDP_FLAGS_HW_MODE: u32 = 1 << 3;
pub const XDP_FLAGS_MODES: u32 = XDP_FLAGS_SKB_MODE | XDP_FLAGS_DRV_MODE | XDP_FLAGS_HW_MODE;

// AF_XDP socket bind flags (`sxdp_flags`)
pub const XDP_SHARED_UMEM: u16 = 1 << 0;
pub const XDP_COPY: u16 = 1 << 1;
pub const XDP_ZEROCOPY: u16 = 1 << 2;
pub const XDP_USE_NEED_WAKEUP: u16 = 1 << 3;

// Ring mmap offset constants (Page offsets in bytes)
pub const XDP_PGOFF_RX_RING: u64 = 0;
pub const XDP_PGOFF_TX_RING: u64 = 0x8000_0000;
pub const XDP_UMEM_PGOFF_FILL_RING: u64 = 0x1_0000_0000;
pub const XDP_UMEM_PGOFF_COMPLETION_RING: u64 = 0x1_8000_0000;

// Ring descriptor flags
pub const XDP_RING_NEED_WAKEUP: u32 = 1 << 0;

// Canonical XDP action codes
pub const XDP_ABORTED: u32 = 0;
pub const XDP_DROP: u32 = 1;
pub const XDP_PASS: u32 = 2;
pub const XDP_TX: u32 = 3;
pub const XDP_REDIRECT: u32 = 4;

/// Socket address structure for `AF_XDP` sockets (`struct sockaddr_xdp`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct sockaddr_xdp {
    pub sxdp_family: u16,
    pub sxdp_flags: u16,
    pub sxdp_ifindex: u32,
    pub sxdp_queue_id: u32,
    pub sxdp_shared_umem_fd: u32,
}

/// Registration configuration for user memory arena (`struct xdp_umem_reg`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct xdp_umem_reg {
    pub addr: u64,
    pub len: u64,
    pub chunk_size: u32,
    pub headroom: u32,
    pub flags: u32,
}

/// Ring layout offset descriptors (`struct xdp_ring_offset`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct xdp_ring_offset {
    pub producer: u64,
    pub consumer: u64,
    pub desc: u64,
    pub flags: u64,
}

/// Offsets returned by kernel for memory-mapping ring buffers (`struct xdp_mmap_offsets`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct xdp_mmap_offsets {
    pub rx: xdp_ring_offset,
    pub tx: xdp_ring_offset,
    pub fr: xdp_ring_offset,
    pub cr: xdp_ring_offset,
}

/// Single frame descriptor in Rx and Tx rings (`struct xdp_desc`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct xdp_desc {
    /// Address in UMEM: (frame_idx * frame_size) + offset
    pub addr: u64,
    /// Length of packet payload in bytes
    pub len: u32,
    /// Reserved descriptor options
    pub options: u32,
}

/// Diagnostic statistics from socket (`struct xdp_statistics`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct xdp_statistics {
    pub rx_dropped: u64,
    pub rx_invalid_descs: u64,
    pub tx_invalid_descs: u64,
    pub rx_ring_full: u64,
    pub rx_fill_ring_empty_descs: u64,
    pub tx_ring_empty_descs: u64,
}
