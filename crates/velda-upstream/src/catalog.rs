//! Contiguous, Page-Aligned Upstream Byte Catalog (`velda_upstream::catalog`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │ UpstreamCatalog (align = 4096, size = k * 4096 bytes)                  │
//! ├────────────────────────────────────────────────────────────────────────┤
//! │ [Dense Index Table: M * 4B]                                            │
//! │   Slot i: { offset: u16, len: u16 } ──► Points to Name Bytes in Page   │
//! │                                                                        │
//! │ [Contiguous Raw UTF-8 Byte Arena: remainder of page]                   │
//! │   "upstream_0_name""upstream_1_name"..."upstream_M-1_name"             │
//! └────────────────────────────────────────────────────────────────────────┘
//! ```

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ptr::NonNull;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use velda_core::UpstreamId;

static GLOBAL_UPSTREAM_CATALOG: ArcSwapOption<UpstreamCatalog> = ArcSwapOption::const_empty();

pub const PAGE_SIZE: usize = 4096;

/// Replaces the global upstream catalog atomically.
pub fn set_global_upstream_catalog(catalog: UpstreamCatalog) {
    GLOBAL_UPSTREAM_CATALOG.store(Some(Arc::new(catalog)));
}

/// Executes a closure with the raw byte slice of the upstream corresponding to `id`.
///
/// Falls back to `b"unmatched"` when `id == UNASSIGNED_UPSTREAM_ID`, no catalog is loaded, or `id` is out of range.
pub fn with_upstream_bytes<R>(id: u16, f: impl FnOnce(&[u8]) -> R) -> R {
    if id == velda_core::UNASSIGNED_UPSTREAM_ID {
        return f(b"unmatched");
    }
    let guard = GLOBAL_UPSTREAM_CATALOG.load();
    if let Some(cat) = guard.as_ref()
        && let Some(bytes) = cat.get_bytes(UpstreamId(id))
    {
        return f(bytes);
    }
    f(b"unmatched")
}

/// Contiguous, 4KB page-aligned upstream name byte catalog.
#[derive(Debug)]
pub struct UpstreamCatalog {
    ptr: NonNull<u8>,
    capacity: usize,
    entry_count: usize,
}

unsafe impl Send for UpstreamCatalog {}
unsafe impl Sync for UpstreamCatalog {}

impl Drop for UpstreamCatalog {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, PAGE_SIZE)
            .expect("UpstreamCatalog layout must be valid");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Default for UpstreamCatalog {
    fn default() -> Self {
        Self::build(std::iter::empty::<&str>())
    }
}

impl UpstreamCatalog {
    /// Builds a 4KB page-aligned [`UpstreamCatalog`] from upstream name slices.
    pub fn build(names: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let names_vec: Vec<String> = names.into_iter().map(|n| n.as_ref().to_string()).collect();
        let count = names_vec.len();

        let data_start = count * 4;
        let total_name_bytes: usize = names_vec.iter().map(|n| n.len()).sum();
        let total_required = data_start + total_name_bytes;

        let pages = total_required.div_ceil(PAGE_SIZE).max(1);
        let capacity = pages * PAGE_SIZE;

        let layout = Layout::from_size_align(capacity, PAGE_SIZE)
            .expect("Page layout creation must succeed");

        let raw_ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw_ptr).expect("Page allocation failed");

        unsafe {
            let base = ptr.as_ptr();
            let mut current_data_offset = data_start;
            for (i, name) in names_vec.iter().enumerate() {
                let name_bytes = name.as_bytes();
                let name_len = name_bytes.len();

                let index_slot = base.add(i * 4);
                std::ptr::write_unaligned(index_slot as *mut u16, current_data_offset as u16);
                std::ptr::write_unaligned(index_slot.add(2) as *mut u16, name_len as u16);

                std::ptr::copy_nonoverlapping(
                    name_bytes.as_ptr(),
                    base.add(current_data_offset),
                    name_len,
                );

                current_data_offset += name_len;
            }
        }

        Self {
            ptr,
            capacity,
            entry_count: count,
        }
    }

    /// Resolves the raw byte slice of an upstream name in O(1) by [`UpstreamId`].
    #[inline(always)]
    pub fn get_bytes(&self, id: UpstreamId) -> Option<&[u8]> {
        let idx = id.0 as usize;
        if idx >= self.entry_count {
            return None;
        }

        let slice = unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.capacity) };
        let slot_offset = idx * 4;
        let offset = u16::from_ne_bytes([slice[slot_offset], slice[slot_offset + 1]]) as usize;
        let len = u16::from_ne_bytes([slice[slot_offset + 2], slice[slot_offset + 3]]) as usize;

        Some(&slice[offset..offset + len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_alignment() {
        let catalog = UpstreamCatalog::build(["grpc-payment", "http-catalog", "ws-chat", "ai"]);

        assert_eq!(catalog.entry_count, 4);
        assert_eq!(catalog.capacity, PAGE_SIZE);
        assert_eq!((catalog.ptr.as_ptr() as usize) % PAGE_SIZE, 0);

        assert_eq!(
            catalog.get_bytes(UpstreamId(0)),
            Some(b"grpc-payment".as_slice())
        );
        assert_eq!(catalog.get_bytes(UpstreamId(3)), Some(b"ai".as_slice()));
        assert_eq!(catalog.get_bytes(UpstreamId(4)), None);
    }

    #[test]
    fn global_access() {
        set_global_upstream_catalog(UpstreamCatalog::build(["root", "auth-svc", "payment-svc"]));

        with_upstream_bytes(0, |bytes| assert_eq!(bytes, b"root"));
        with_upstream_bytes(1, |bytes| assert_eq!(bytes, b"auth-svc"));
        with_upstream_bytes(2, |bytes| assert_eq!(bytes, b"payment-svc"));
        with_upstream_bytes(velda_core::UNASSIGNED_UPSTREAM_ID, |bytes| {
            assert_eq!(bytes, b"unmatched")
        });
        with_upstream_bytes(999, |bytes| assert_eq!(bytes, b"unmatched"));
    }
}
