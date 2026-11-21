//! 4KB Page-Aligned Contiguous Per-Listener [`RouteArena`] (`velda_router::arena`).
//!
//! Separates path radix routing from host pattern matching across two independent 4KB pages:
//! - **Page 1 ([`HostTable`])**: 4KB page-aligned contiguous array of 64-byte [`HostPattern`] entries.
//! - **Page 2 ([`RouteArena`])**: 4KB page-aligned contiguous block per `listener_id`:
//!   - `[0..64)`       [`ArenaMeta`] (64B, 1 L1D cache line)
//!   - `[nodes_off]`  Region 1: [`ArenaNode`] (16B/node, 4 nodes per 64B cache line)
//!   - `[entries_off]` Region 2: [`RouteEntry`] (8B/entry, 8 entries per 64B cache line)

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::collections::VecDeque;
use std::ptr::NonNull;

use crate::error::RouterError;
use crate::host::{HostPattern, HostTable};
use crate::path::{LEN_MASKS, common_prefix_len, read_chunk_u64};

/// Physical OS memory page size (4 KiB).
pub const ARENA_PAGE_SIZE: usize = 4096;

/// Sentinel index for missing candidates or wildcard host (`host_id = 0xFFFF`).
pub const ARENA_NONE: u16 = 0xFFFF;

/// Cache-line aligned metadata header at offset 0 of every [`RouteArena`] (64 bytes).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaMeta {
    pub listener_id: u32,
    pub node_count: u16,
    pub entry_count: u16,
    pub catch_all_start: u16,
    pub catch_all_count: u16,
    pub nodes_offset: u32,
    pub entries_offset: u32,
    pub _pad1: [u8; 44],
}

const _: () = assert!(std::mem::size_of::<ArenaMeta>() == 64);
const _: () = assert!(std::mem::align_of::<ArenaMeta>() == 64);

/// Contiguous 16-byte integer-block radix node (4 nodes per 64B L1D cache line).
#[repr(C, align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaNode {
    /// 8-byte chunk of the path/service segment (little-endian `u64`).
    pub chunk: u64,
    /// Contiguous start index of child nodes in Region 1.
    pub children_start: u16,
    /// Contiguous start index of entries in Region 2 (`ARENA_NONE` = no entries).
    pub cand_start: u16,
    /// Byte length of `chunk` (`1..=8`, or `0` for root).
    pub len: u8,
    /// Number of child nodes (`0..=255`).
    pub children_count: u8,
    /// Number of entries at this node (`0..=255`).
    pub cand_count: u8,
    /// Bit `0x01`: `1` if any entry at this node allows prefix matching.
    pub flags: u8,
}

const _: () = assert!(std::mem::size_of::<ArenaNode>() == 16);
const _: () = assert!(std::mem::align_of::<ArenaNode>() == 16);

/// Contiguous 8-byte hot-path route descriptor (8 entries per 64B cache line).
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteEntry {
    /// Route ID for `RequestState`.
    pub route_id: u16,
    /// 1-cycle HTTP method bitmask (`MethodMask`), or gRPC method tag (`0xFFFF` = any).
    pub method_mask: u16,
    /// `0xFFFF` (`ARENA_NONE`) = ANY (skip Host check in 0 cycles); otherwise index into [`HostTable`].
    pub host_id: u16,
    /// 2B alignment padding (bit 0: `1` for prefix route, `0` for exact route).
    pub _pad: u16,
}

const _: () = assert!(std::mem::size_of::<RouteEntry>() == 8);
const _: () = assert!(std::mem::align_of::<RouteEntry>() == 8);

/// Build-time input entry compiled into [`RouteArena`] + [`HostTable`].
#[derive(Debug, Clone, Copy)]
pub struct ArenaRouteEntry<'a> {
    pub pattern: &'a str,
    pub route_id: u16,
    pub method_mask: u16,
    pub host: Option<HostPattern>,
    pub specificity: u8,
    pub is_prefix: bool,
    pub is_catch_all: bool,
}

#[derive(Debug, Clone, Default)]
struct BuilderNode {
    prefix: String,
    cand_start: u16,
    cand_count: u8,
    has_prefix: bool,
    children: Vec<BuilderNode>,
}

impl BuilderNode {
    fn insert(&mut self, path: &str, cand_start: u16, cand_count: u8, has_prefix: bool) {
        if path.is_empty() {
            self.cand_start = cand_start;
            self.cand_count = cand_count;
            self.has_prefix = self.has_prefix || has_prefix;
            return;
        }

        let target_byte = path.as_bytes()[0];
        if let Some(child_idx) = self
            .children
            .iter()
            .position(|c| c.prefix.as_bytes().first().copied() == Some(target_byte))
        {
            let child = &mut self.children[child_idx];
            let common_len = common_prefix_len(&child.prefix, path);

            if common_len == child.prefix.len() {
                child.insert(&path[common_len..], cand_start, cand_count, has_prefix);
            } else {
                let common_prefix = child.prefix[..common_len].to_string();
                let child_suffix = child.prefix[common_len..].to_string();
                let path_suffix = path[common_len..].to_string();

                let split_node = BuilderNode {
                    prefix: common_prefix,
                    cand_start: if path_suffix.is_empty() {
                        cand_start
                    } else {
                        ARENA_NONE
                    },
                    cand_count: if path_suffix.is_empty() {
                        cand_count
                    } else {
                        0
                    },
                    has_prefix: if path_suffix.is_empty() {
                        has_prefix
                    } else {
                        false
                    },
                    children: Vec::new(),
                };

                child.prefix = child_suffix;
                let old_child = std::mem::replace(child, split_node);
                self.children[child_idx].children.push(old_child);

                if !path_suffix.is_empty() {
                    self.children[child_idx].children.push(BuilderNode {
                        prefix: path_suffix,
                        cand_start,
                        cand_count,
                        has_prefix,
                        children: Vec::new(),
                    });
                }
            }
        } else {
            self.children.push(BuilderNode {
                prefix: path.to_string(),
                cand_start,
                cand_count,
                has_prefix,
                children: Vec::new(),
            });
        }
    }

    fn normalize_chunk_lengths(&mut self) {
        for child in &mut self.children {
            child.normalize_chunk_lengths();
        }

        while self.prefix.len() > 8 {
            let tail_prefix = self.prefix[8..].to_string();
            self.prefix.truncate(8);

            let old_children = std::mem::take(&mut self.children);
            let old_cand_start = std::mem::replace(&mut self.cand_start, ARENA_NONE);
            let old_cand_count = std::mem::replace(&mut self.cand_count, 0);
            let old_has_prefix = std::mem::replace(&mut self.has_prefix, false);

            let mut intermediate_child = BuilderNode {
                prefix: tail_prefix,
                cand_start: old_cand_start,
                cand_count: old_cand_count,
                has_prefix: old_has_prefix,
                children: old_children,
            };

            intermediate_child.normalize_chunk_lengths();
            self.children.push(intermediate_child);
        }
    }
}

/// Contiguous, 4KB page-aligned route arena for a single `listener_id`, paired with
/// an independent 4KB page-aligned [`HostTable`].
#[derive(Debug)]
pub struct RouteArena {
    ptr: NonNull<u8>,
    capacity: usize,
    host_table: HostTable,
}

unsafe impl Send for RouteArena {}
unsafe impl Sync for RouteArena {}

impl Drop for RouteArena {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, ARENA_PAGE_SIZE)
            .expect("RouteArena layout must be valid");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Clone for RouteArena {
    fn clone(&self) -> Self {
        let layout = Layout::from_size_align(self.capacity, ARENA_PAGE_SIZE)
            .expect("RouteArena layout must be valid");
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).expect("RouteArena clone allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), self.capacity);
        }
        Self {
            ptr,
            capacity: self.capacity,
            host_table: self.host_table.clone(),
        }
    }
}

impl Default for RouteArena {
    fn default() -> Self {
        Self::build(0, Vec::new()).expect("empty RouteArena build must succeed")
    }
}

impl RouteArena {
    /// Compiles route entries for `listener_id` into a 4KB page-aligned [`RouteArena`]
    /// and a separate 4KB page-aligned [`HostTable`].
    ///
    /// Uses `Vec::sort_by` + linear host deduplication — **0 `HashMap` allocations**.
    /// Returns [`RouterError::InvalidRoute`] if per-node candidates/children exceed `u8::MAX`
    /// or total nodes/entries/hosts exceed `u16` index bounds.
    pub fn build(listener_id: u32, entries: Vec<ArenaRouteEntry<'_>>) -> Result<Self, RouterError> {
        let mut pattern_entries = Vec::with_capacity(entries.len());
        let mut catch_all_entries = Vec::new();
        for e in entries {
            if e.is_catch_all {
                catch_all_entries.push(e);
            } else {
                pattern_entries.push(e);
            }
        }

        pattern_entries.sort_by(|a, b| {
            a.pattern
                .cmp(b.pattern)
                .then_with(|| a.is_prefix.cmp(&b.is_prefix))
                .then_with(|| b.specificity.cmp(&a.specificity))
        });

        catch_all_entries.sort_by_key(|a| std::cmp::Reverse(a.specificity));

        let mut route_entries: Vec<RouteEntry> =
            Vec::with_capacity(pattern_entries.len() + catch_all_entries.len());
        let mut hosts: Vec<HostPattern> = Vec::new();

        let intern_host =
            |host: Option<HostPattern>, hosts: &mut Vec<HostPattern>| -> Result<u16, RouterError> {
                let Some(pat) = host else {
                    return Ok(ARENA_NONE);
                };
                if let Some(pos) = hosts.iter().position(|h| *h == pat) {
                    Ok(pos as u16)
                } else {
                    if hosts.len() >= ARENA_NONE as usize {
                        return Err(RouterError::InvalidRoute {
                            detail: format!(
                                "listener {listener_id} exceeds maximum {} unique host patterns",
                                ARENA_NONE
                            ),
                        });
                    }
                    let idx = hosts.len() as u16;
                    hosts.push(pat);
                    Ok(idx)
                }
            };

        let mut root_builder = BuilderNode {
            prefix: String::new(),
            cand_start: ARENA_NONE,
            cand_count: 0,
            has_prefix: false,
            children: Vec::new(),
        };

        let mut i = 0;
        while i < pattern_entries.len() {
            let pat = pattern_entries[i].pattern;
            let mut j = i + 1;
            while j < pattern_entries.len() && pattern_entries[j].pattern == pat {
                j += 1;
            }

            let count = j - i;
            if count > u8::MAX as usize {
                return Err(RouterError::InvalidRoute {
                    detail: format!(
                        "pattern '{pat}' on listener {listener_id} has {count} candidates (max {})",
                        u8::MAX
                    ),
                });
            }
            if route_entries.len() + count > ARENA_NONE as usize {
                return Err(RouterError::InvalidRoute {
                    detail: format!(
                        "listener {listener_id} exceeds maximum {} route entries",
                        ARENA_NONE
                    ),
                });
            }

            let cand_start = route_entries.len() as u16;
            let mut has_prefix = false;

            for e in &pattern_entries[i..j] {
                if e.is_prefix {
                    has_prefix = true;
                }
                let host_id = intern_host(e.host, &mut hosts)?;
                route_entries.push(RouteEntry {
                    route_id: e.route_id,
                    method_mask: e.method_mask,
                    host_id,
                    _pad: u16::from(e.is_prefix),
                });
            }

            root_builder.insert(pat, cand_start, count as u8, has_prefix);
            i = j;
        }

        let catch_all_start = if catch_all_entries.is_empty() {
            ARENA_NONE
        } else {
            if route_entries.len() + catch_all_entries.len() > ARENA_NONE as usize {
                return Err(RouterError::InvalidRoute {
                    detail: format!(
                        "listener {listener_id} exceeds maximum {} route entries",
                        ARENA_NONE
                    ),
                });
            }
            route_entries.len() as u16
        };
        let catch_all_count = catch_all_entries.len() as u16;
        for e in &catch_all_entries {
            let host_id = intern_host(e.host, &mut hosts)?;
            route_entries.push(RouteEntry {
                route_id: e.route_id,
                method_mask: e.method_mask,
                host_id,
                _pad: u16::from(e.is_prefix),
            });
        }

        // Compile radix tree into BFS flat ArenaNode array
        root_builder.normalize_chunk_lengths();
        let mut nodes = Vec::new();
        let mut queue = VecDeque::new();

        nodes.push(ArenaNode {
            chunk: 0,
            children_start: 0,
            cand_start: root_builder.cand_start,
            len: 0,
            children_count: 0,
            cand_count: root_builder.cand_count,
            flags: u8::from(root_builder.has_prefix),
        });
        queue.push_back((root_builder, 0usize));

        while let Some((parent_b, parent_idx)) = queue.pop_front() {
            if parent_b.children.is_empty() {
                continue;
            }

            let count = parent_b.children.len();
            if count > u8::MAX as usize {
                return Err(RouterError::InvalidRoute {
                    detail: format!(
                        "radix node on listener {listener_id} has {count} children (max {})",
                        u8::MAX
                    ),
                });
            }
            let start = nodes.len();
            if start + count > u16::MAX as usize {
                return Err(RouterError::InvalidRoute {
                    detail: format!(
                        "listener {listener_id} exceeds maximum {} radix nodes",
                        u16::MAX
                    ),
                });
            }
            nodes[parent_idx].children_start = start as u16;
            nodes[parent_idx].children_count = count as u8;

            for child_b in &parent_b.children {
                let mut buf = [0u8; 8];
                let b = child_b.prefix.as_bytes();
                let copy_len = b.len().min(8);
                buf[..copy_len].copy_from_slice(&b[..copy_len]);

                nodes.push(ArenaNode {
                    chunk: u64::from_le_bytes(buf),
                    children_start: 0,
                    cand_start: child_b.cand_start,
                    len: copy_len as u8,
                    children_count: 0,
                    cand_count: child_b.cand_count,
                    flags: u8::from(child_b.has_prefix),
                });
            }

            for (idx_offset, child_b) in parent_b.children.into_iter().enumerate() {
                queue.push_back((child_b, start + idx_offset));
            }
        }

        let nodes_offset = std::mem::size_of::<ArenaMeta>(); // 64 (16B aligned)
        let entries_offset = nodes_offset + nodes.len() * std::mem::size_of::<ArenaNode>();
        let total_required =
            entries_offset + route_entries.len() * std::mem::size_of::<RouteEntry>();

        let pages = total_required.div_ceil(ARENA_PAGE_SIZE).max(1);
        let capacity = pages * ARENA_PAGE_SIZE;

        let layout = Layout::from_size_align(capacity, ARENA_PAGE_SIZE)
            .expect("RouteArena page layout must succeed");
        let raw_ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw_ptr).expect("RouteArena allocation failed");

        let header = ArenaMeta {
            listener_id,
            node_count: nodes.len() as u16,
            entry_count: route_entries.len() as u16,
            catch_all_start,
            catch_all_count,
            nodes_offset: nodes_offset as u32,
            entries_offset: entries_offset as u32,
            _pad1: [0; 44],
        };

        unsafe {
            let base = ptr.as_ptr();
            std::ptr::write(base as *mut ArenaMeta, header);
            if !nodes.is_empty() {
                std::ptr::copy_nonoverlapping(
                    nodes.as_ptr(),
                    base.add(nodes_offset) as *mut ArenaNode,
                    nodes.len(),
                );
            }
            if !route_entries.is_empty() {
                std::ptr::copy_nonoverlapping(
                    route_entries.as_ptr(),
                    base.add(entries_offset) as *mut RouteEntry,
                    route_entries.len(),
                );
            }
        }

        let host_table = HostTable::from_patterns(&hosts);

        Ok(Self {
            ptr,
            capacity,
            host_table,
        })
    }

    /// Returns the 64-byte cache-aligned arena metadata.
    #[inline(always)]
    pub fn header(&self) -> &ArenaMeta {
        unsafe { &*(self.ptr.as_ptr() as *const ArenaMeta) }
    }

    /// Returns the independent 4KB page-aligned [`HostTable`].
    #[inline(always)]
    pub fn host_table(&self) -> &HostTable {
        &self.host_table
    }

    /// Returns the contiguous slice of 16-byte radix nodes (Region 1).
    #[inline(always)]
    pub fn nodes(&self) -> &[ArenaNode] {
        let h = self.header();
        if h.node_count == 0 {
            return &[];
        }
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.as_ptr().add(h.nodes_offset as usize) as *const ArenaNode,
                h.node_count as usize,
            )
        }
    }

    /// Returns the contiguous slice of 8-byte [`RouteEntry`] descriptors (Region 2).
    #[inline(always)]
    pub fn entries(&self) -> &[RouteEntry] {
        let h = self.header();
        if h.entry_count == 0 {
            return &[];
        }
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.as_ptr().add(h.entries_offset as usize) as *const RouteEntry,
                h.entry_count as usize,
            )
        }
    }

    /// Returns the contiguous slice of catch-all entries (`*` / empty service).
    #[inline(always)]
    pub fn catch_all_entries(&self) -> &[RouteEntry] {
        let h = self.header();
        if h.catch_all_count == 0 || h.catch_all_start == ARENA_NONE {
            return &[];
        }
        let all = self.entries();
        let start = h.catch_all_start as usize;
        let end = (start + h.catch_all_count as usize).min(all.len());
        &all[start..end]
    }

    /// Returns the [`RouteEntry`] slice for a given `(cand_start, cand_count)` pair.
    #[inline(always)]
    pub fn node_entries(&self, cand_start: u16, cand_count: u8) -> &[RouteEntry] {
        if cand_start == ARENA_NONE || cand_count == 0 {
            return &[];
        }
        let all = self.entries();
        let start = cand_start as usize;
        let end = (start + cand_count as usize).min(all.len());
        &all[start..end]
    }

    /// Returns the allocated page-aligned size of the route arena in bytes (`k * 4096`).
    #[inline(always)]
    pub fn allocated_bytes(&self) -> usize {
        self.capacity
    }

    /// Traverses the 16-byte radix node arena for `path`, recording `(cand_start, cand_count, is_exact)`
    /// from root down to the deepest matching node into `out`.
    #[inline]
    pub fn match_path(&self, path: &str, out: &mut [(u16, u8, bool); 32]) -> usize {
        let nodes = self.nodes();
        if nodes.is_empty() {
            return 0;
        }
        crate::path::prefetch_l1_keep(nodes.as_ptr() as *const u8);

        let mut count = 0;
        let mut current_idx = 0;
        let mut remaining = path.as_bytes();

        if nodes[0].cand_start != ARENA_NONE && nodes[0].cand_count > 0 {
            let is_exact = remaining.is_empty();
            if is_exact || (nodes[0].flags & 0x01) != 0 {
                out[count] = (nodes[0].cand_start, nodes[0].cand_count, is_exact);
                count += 1;
            }
        }

        while !remaining.is_empty() {
            let node = &nodes[current_idx];
            let start = node.children_start as usize;
            let c_count = node.children_count as usize;
            if c_count == 0 {
                break;
            }

            let chunk = read_chunk_u64(remaining);
            let rem_len = remaining.len();
            let mut matched_child = None;

            for (offset, child) in nodes[start..start + c_count].iter().enumerate() {
                let clen = child.len as usize;
                if rem_len >= clen {
                    let mask = LEN_MASKS[clen];
                    if ((chunk ^ child.chunk) & mask) == 0 {
                        matched_child = Some(start + offset);
                        break;
                    }
                }
            }

            let Some(child_idx) = matched_child else {
                break;
            };

            let child = &nodes[child_idx];
            remaining = &remaining[child.len as usize..];
            current_idx = child_idx;

            if child.cand_start != ARENA_NONE && child.cand_count > 0 && count < out.len() {
                let is_exact = remaining.is_empty();
                if is_exact || (child.flags & 0x01) != 0 {
                    out[count] = (child.cand_start, child.cand_count, is_exact);
                    count += 1;
                }
            }
        }

        count
    }

    /// Two-stage exact traversal for gRPC `(service, Option<method>)` with zero allocations.
    ///
    /// Returns `(method_cands, service_cands)` where each is `Option<(cand_start, cand_count)>`.
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn match_grpc(
        &self,
        service: &str,
        method: Option<&str>,
    ) -> (Option<(u16, u8)>, Option<(u16, u8)>) {
        let nodes = self.nodes();
        if nodes.is_empty() {
            return (None, None);
        }
        crate::path::prefetch_l1_keep(nodes.as_ptr() as *const u8);

        let svc_cands = walk_exact_segments(nodes, &[service.as_bytes()]).and_then(|idx| {
            let n = &nodes[idx];
            (n.cand_start != ARENA_NONE && n.cand_count > 0).then_some((n.cand_start, n.cand_count))
        });

        let method_cands = method.and_then(|m| {
            walk_exact_segments(nodes, &[service.as_bytes(), b"/", m.as_bytes()]).and_then(|idx| {
                let n = &nodes[idx];
                (n.cand_start != ARENA_NONE && n.cand_count > 0)
                    .then_some((n.cand_start, n.cand_count))
            })
        });

        (method_cands, svc_cands)
    }
}

#[inline(always)]
fn read_chunk_from_segments(
    segs: &[&[u8]],
    mut seg_idx: usize,
    mut seg_off: usize,
) -> (u64, usize) {
    let mut buf = [0u8; 8];
    let mut filled = 0;
    while filled < 8 && seg_idx < segs.len() {
        let seg = segs[seg_idx];
        let avail = seg.len().saturating_sub(seg_off);
        if avail == 0 {
            seg_idx += 1;
            seg_off = 0;
            continue;
        }
        let take = avail.min(8 - filled);
        buf[filled..filled + take].copy_from_slice(&seg[seg_off..seg_off + take]);
        filled += take;
        seg_off += take;
    }
    (u64::from_le_bytes(buf), filled)
}

#[inline(always)]
fn walk_exact_segments(nodes: &[ArenaNode], segs: &[&[u8]]) -> Option<usize> {
    let total_len: usize = segs.iter().map(|s| s.len()).sum();
    let mut current_idx = 0;
    let mut consumed = 0usize;
    let mut seg_idx = 0usize;
    let mut seg_off = 0usize;

    while consumed < total_len {
        let node = &nodes[current_idx];
        let start = node.children_start as usize;
        let c_count = node.children_count as usize;
        if c_count == 0 {
            return None;
        }

        let (chunk, rem_avail) = read_chunk_from_segments(segs, seg_idx, seg_off);
        let rem_len = total_len - consumed;
        let avail = rem_avail.min(rem_len);
        let mut matched_child = None;

        for (offset, child) in nodes[start..start + c_count].iter().enumerate() {
            let clen = child.len as usize;
            if avail >= clen {
                let mask = LEN_MASKS[clen];
                if ((chunk ^ child.chunk) & mask) == 0 {
                    matched_child = Some(start + offset);
                    break;
                }
            }
        }

        let child_idx = matched_child?;
        let step = nodes[child_idx].len as usize;
        consumed += step;
        let mut left = step;
        while left > 0 && seg_idx < segs.len() {
            let avail_in_seg = segs[seg_idx].len() - seg_off;
            if left < avail_in_seg {
                seg_off += left;
                left = 0;
            } else {
                left -= avail_in_seg;
                seg_idx += 1;
                seg_off = 0;
            }
        }
        current_idx = child_idx;
    }
    Some(current_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::HostBlocks;

    #[test]
    fn test_route_arena_and_host_table_separation() {
        let entries = vec![
            ArenaRouteEntry {
                pattern: "/healthz",
                route_id: 1,
                method_mask: 0xFFFF,
                host: HostPattern::compile(None),
                specificity: 0,
                is_prefix: false,
                is_catch_all: false,
            },
            ArenaRouteEntry {
                pattern: "/api/v1",
                route_id: 2,
                method_mask: 0xFFFF,
                host: HostPattern::compile(Some("api.velda.io")),
                specificity: 2,
                is_prefix: true,
                is_catch_all: false,
            },
        ];

        let arena = RouteArena::build(42, entries).unwrap();
        assert_eq!((arena.ptr.as_ptr() as usize) % ARENA_PAGE_SIZE, 0);
        assert_eq!(arena.allocated_bytes() % ARENA_PAGE_SIZE, 0);
        assert_eq!(arena.header().listener_id, 42);
        assert_eq!(arena.host_table().patterns().len(), 1);

        let mut buf = [(0u16, 0u8, false); 32];
        let n = arena.match_path("/api/v1/users", &mut buf);
        assert_eq!(n, 1);
        let cands = arena.node_entries(buf[0].0, buf[0].1);
        assert_eq!(cands[0].route_id, 2);
        assert!(
            arena
                .host_table()
                .matches(cands[0].host_id, &HostBlocks::parse("api.velda.io"))
        );

        let cloned = arena.clone();
        assert_eq!(cloned.header().listener_id, 42);
        assert_eq!(cloned.entries(), arena.entries());
    }

    #[test]
    fn test_route_arena_rejects_candidate_overflow() {
        let entries: Vec<ArenaRouteEntry<'_>> = (0..256)
            .map(|i| ArenaRouteEntry {
                pattern: "/api",
                route_id: i as u16,
                method_mask: 0xFFFF,
                host: None,
                specificity: 0,
                is_prefix: true,
                is_catch_all: false,
            })
            .collect();

        let err = RouteArena::build(7, entries).unwrap_err();
        assert!(matches!(err, RouterError::InvalidRoute { .. }));
    }
}
