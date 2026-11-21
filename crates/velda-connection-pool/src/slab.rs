//! Page-aligned (`4096` bytes) contiguous memory slabs for connection pool arenas.
//!
//! ```text
//! PageAlignedSlab<T> (HotLane, single-writer contiguous growth)
//! ┌──────────────────────────────────┬──────────────────────────────────┐
//! │ Page 0 (4096B, align=4096)       │ Page 1 (4096B, appended on spill)│
//! │ [ slot 0 | slot 1 | ... | slot N │ slot N+1 | ... ]                 │
//! └──────────────────────────────────┴──────────────────────────────────┘
//!   realloc(align=4096, pages * 2) -> 1 -> 2 -> 4 -> ... (0 fragmentation)
//!
//! FixedPageSlab<T> (ColdPool, lock-free shared across all cores)
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │ Pre-zeroed contiguous k * 4096B (align=4096), demand-zero paged     │
//! │ Address never moves after init -> safe for concurrent atomic CAS    │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error, realloc};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

/// Standard OS virtual memory page size (4 KiB).
pub const PAGE_SIZE: usize = 4096;

/// Contiguous `4096`-byte page-aligned growable slab (`size = k * 4096`).
///
/// Designed for single-writer core-local `HotLane` tables. Starts with at least
/// 1 full 4KB page and doubles page count (`1 -> 2 -> 4 -> ...`) on overflow so
/// virtual memory remains a single contiguous region without fragmentation.
pub struct PageAlignedSlab<T> {
    ptr: NonNull<u8>,
    pages: usize,
    cap: usize,
    len: usize,
    _marker: PhantomData<T>,
}

// Safety: `PageAlignedSlab<T>` owns its heap allocation and elements `T`.
unsafe impl<T: Send> Send for PageAlignedSlab<T> {}
unsafe impl<T: Sync> Sync for PageAlignedSlab<T> {}

impl<T> PageAlignedSlab<T> {
    /// Creates a new slab backed by at least `min_pages` contiguous 4KB pages.
    pub fn with_pages(min_pages: usize) -> Self {
        assert!(
            size_of::<T>() > 0,
            "Zero-sized types are not supported in PageAlignedSlab"
        );
        assert!(
            align_of::<T>() <= PAGE_SIZE,
            "Element alignment cannot exceed PAGE_SIZE"
        );

        let pages = min_pages.max(1);
        let byte_size = pages
            .checked_mul(PAGE_SIZE)
            .expect("PageAlignedSlab byte size overflow");
        let layout =
            Layout::from_size_align(byte_size, PAGE_SIZE).expect("Invalid PageAlignedSlab layout");

        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
        let cap = byte_size / size_of::<T>();

        Self {
            ptr,
            pages,
            cap,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// Number of initialized elements currently stored.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no elements are stored.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Element capacity fitting within currently allocated 4KB pages.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Number of contiguous 4KB pages backing this slab.
    #[inline]
    pub fn page_count(&self) -> usize {
        self.pages
    }

    /// Returns the total allocated page-aligned byte size (`pages * 4096`).
    #[inline]
    pub fn byte_len(&self) -> usize {
        self.pages * PAGE_SIZE
    }

    /// Returns a contiguous slice of all initialized elements.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const T, self.len) }
    }

    /// Returns a contiguous mutable slice of all initialized elements.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr() as *mut T, self.len) }
    }

    /// Appends an element, growing contiguously to `2x` pages if the current page boundary is reached.
    #[inline]
    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            self.grow_pages();
        }
        unsafe {
            let dst = (self.ptr.as_ptr() as *mut T).add(self.len);
            dst.write(value);
        }
        self.len += 1;
    }

    /// Removes and returns the element at `index` in O(1) by swapping with the last element.
    ///
    /// Returns `None` if `index >= self.len`.
    #[inline]
    pub fn swap_remove(&mut self, index: usize) -> Option<T> {
        if index >= self.len {
            return None;
        }
        self.len -= 1;
        unsafe {
            let base = self.ptr.as_ptr() as *mut T;
            let removed = base.add(index).read();
            if index != self.len {
                let last = base.add(self.len).read();
                base.add(index).write(last);
            }
            Some(removed)
        }
    }

    /// Removes all elements, dropping each in place while retaining the 4KB-aligned pages.
    pub fn clear(&mut self) {
        let len = self.len;
        self.len = 0;
        let base = self.ptr.as_ptr() as *mut T;
        for i in 0..len {
            unsafe {
                std::ptr::drop_in_place(base.add(i));
            }
        }
    }

    /// Grows the backing allocation contiguously by doubling page count (`1 -> 2 -> 4 -> ...`).
    #[cold]
    #[inline(never)]
    fn grow_pages(&mut self) {
        let old_bytes = self.pages * PAGE_SIZE;
        let old_layout = Layout::from_size_align(old_bytes, PAGE_SIZE)
            .expect("Invalid old PageAlignedSlab layout");

        let new_pages = self
            .pages
            .checked_mul(2)
            .expect("PageAlignedSlab page count overflow");
        let new_bytes = new_pages
            .checked_mul(PAGE_SIZE)
            .expect("PageAlignedSlab byte size overflow");
        let new_layout = Layout::from_size_align(new_bytes, PAGE_SIZE)
            .expect("Invalid new PageAlignedSlab layout");

        let new_raw = unsafe { realloc(self.ptr.as_ptr(), old_layout, new_bytes) };
        self.ptr = NonNull::new(new_raw).unwrap_or_else(|| handle_alloc_error(new_layout));
        self.pages = new_pages;
        self.cap = new_bytes / size_of::<T>();
    }
}

impl<T> Drop for PageAlignedSlab<T> {
    fn drop(&mut self) {
        self.clear();
        let bytes = self.pages * PAGE_SIZE;
        let layout =
            Layout::from_size_align(bytes, PAGE_SIZE).expect("Invalid PageAlignedSlab drop layout");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

/// Fixed-capacity `4096`-byte page-aligned contiguous memory region (`size = k * 4096`).
///
/// Never reallocates after construction, making it safe for lock-free concurrent access
/// across all CPU cores (used by the shared `ColdPool`).
pub struct FixedPageSlab<T> {
    ptr: NonNull<u8>,
    pages: usize,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for FixedPageSlab<T> {}
unsafe impl<T: Sync> Sync for FixedPageSlab<T> {}

impl<T> FixedPageSlab<T> {
    /// Allocates a zero-initialized contiguous page-aligned buffer holding at least `min_slots`.
    pub fn with_capacity(min_slots: usize) -> Self {
        assert!(
            size_of::<T>() > 0,
            "Zero-sized types are not supported in FixedPageSlab"
        );
        assert!(
            align_of::<T>() <= PAGE_SIZE,
            "Element alignment cannot exceed PAGE_SIZE"
        );

        let min_bytes = min_slots
            .max(1)
            .checked_mul(size_of::<T>())
            .expect("FixedPageSlab byte size overflow");
        let pages = min_bytes.div_ceil(PAGE_SIZE).max(1);
        let byte_size = pages * PAGE_SIZE;
        let layout =
            Layout::from_size_align(byte_size, PAGE_SIZE).expect("Invalid FixedPageSlab layout");

        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
        let cap = byte_size / size_of::<T>();

        Self {
            ptr,
            pages,
            cap,
            _marker: PhantomData,
        }
    }

    /// Total slot capacity within the contiguous page region.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Number of contiguous 4KB pages backing this region.
    #[inline]
    pub fn page_count(&self) -> usize {
        self.pages
    }

    /// Returns a shared slice over all `capacity()` pre-zeroed slots.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const T, self.cap) }
    }
}

impl<T> Drop for FixedPageSlab<T> {
    fn drop(&mut self) {
        let base = self.ptr.as_ptr() as *mut T;
        for i in 0..self.cap {
            unsafe {
                std::ptr::drop_in_place(base.add(i));
            }
        }
        let bytes = self.pages * PAGE_SIZE;
        let layout =
            Layout::from_size_align(bytes, PAGE_SIZE).expect("Invalid FixedPageSlab drop layout");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_aligned_slab_growth_and_alignment() {
        // 64-byte elements -> 64 elements per 4096-byte page
        #[repr(align(64))]
        #[derive(Debug, PartialEq)]
        struct Elem([u64; 8]);

        let mut slab = PageAlignedSlab::<Elem>::with_pages(1);
        assert_eq!(slab.page_count(), 1);
        assert_eq!(slab.byte_len(), 4096);
        assert_eq!(slab.capacity(), 64);
        assert_eq!((slab.as_slice().as_ptr() as usize) % PAGE_SIZE, 0);

        for i in 0..64u64 {
            slab.push(Elem([i; 8]));
        }
        assert_eq!(slab.page_count(), 1);

        // Overflowing page 1 must allocate page 2 contiguously
        slab.push(Elem([64; 8]));
        assert_eq!(slab.page_count(), 2);
        assert_eq!(slab.byte_len(), 8192);
        assert_eq!(slab.capacity(), 128);
        assert_eq!((slab.as_slice().as_ptr() as usize) % PAGE_SIZE, 0);
        assert_eq!(slab.as_slice()[64], Elem([64; 8]));

        let removed = slab.swap_remove(0).unwrap();
        assert_eq!(removed, Elem([0; 8]));
        assert_eq!(slab.len(), 64);
        assert_eq!(slab.as_slice()[0], Elem([64; 8]));
    }
}
