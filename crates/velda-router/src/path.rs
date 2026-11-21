//! High-performance SIMD URI path normalizer & contiguous 16-byte integer-block matcher.
//!
//! ```text
//!                              INCOMING RAW URI
//!                                     │
//!                     ┌───────────────▼───────────────┐
//!                     │ is_clean_path (Branchless)    │
//!                     │ SIMD vector scan (32B / cycle)│
//!                     └───────┬───────────────┬───────┘
//!                             │               │
//!            CLEAN (>99.9%)   │               │ ANOMALOUS (<0.1%)
//!       ┌─────────────────────┘               └─────────────────────┐
//!       ▼                                                           ▼
//!  Cow::Borrowed(path)                                     normalize_path_cold
//!  Zero Heap Allocation                                    Strip /./, /../, %2e
//!       │                                                           │
//!       └─────────────────────────────┬─────────────────────────────┘
//!                                     │
//!                                     ▼
//!                        INTEGER-BLOCK ROUTE MATCHING
//!                        Vec<IntegerBlockNode> (16B)
//!                    4 Nodes fit in 1 Cache Line (64B)
//!                                     │
//!                     ┌───────────────▼───────────────┐
//!                     │ 1-Cycle ALU (chunk ^ path)    │
//!                     └───────┬───────────────┬───────┘
//!                             │               │
//!                    [PATH ENDS]             [PATH CONTINUES]
//!                    Exact Match             Longest Prefix Candidate
//! ```

use std::borrow::Cow;
use std::collections::VecDeque;

use crate::error::RouterError;

// ============================================================================
// 1. SIMD-Accelerated Path Normalization (RFC 3986)
// ============================================================================

/// Fast check whether a path is canonical and free of anomalies (`\0`, `\`, `%`, `//`, `/.`).
#[inline(always)]
pub fn is_clean_path(bytes: &[u8]) -> bool {
    // 1. Must start with '/'
    if bytes.is_empty() || bytes[0] != b'/' {
        return false;
    }

    // 2. SIMD vector scan: rejects null bytes, backslashes, percent signs in 1 vector cycle
    if memchr::memchr3(b'\0', b'\\', b'%', bytes).is_some() {
        return false;
    }

    // 3. SIMD substring scan for consecutive slashes "//" or dot segments "/."
    if memchr::memmem::find(bytes, b"//").is_some() || memchr::memmem::find(bytes, b"/.").is_some()
    {
        return false;
    }

    true
}

/// Normalizes a request URI path with zero allocations on clean paths (`Cow::Borrowed`).
#[inline(always)]
pub fn normalize_path(path: &str) -> Result<Cow<'_, str>, RouterError> {
    let bytes = path.as_bytes();
    if is_clean_path(bytes) {
        return Ok(Cow::Borrowed(path));
    }

    normalize_path_cold(path)
}

/// Cold slow-path normalization for anomalous or unsafe paths (RFC 3986).
#[cold]
#[inline(never)]
fn normalize_path_cold(path: &str) -> Result<Cow<'static, str>, RouterError> {
    let bytes = path.as_bytes();

    // 1. Reject null bytes immediately (security attack vector)
    if memchr::memchr(b'\0', bytes).is_some() {
        return Err(RouterError::InvalidPath {
            detail: "Prohibited null byte in URI path".into(),
        });
    }

    // 2. Decode percent-encoded traversal bytes (%2e -> '.', %2f -> '/', %5c -> '/')
    // and convert backslashes '\' to '/'
    let mut decoded = String::with_capacity(bytes.len() + 1);
    if !path.starts_with('/') {
        decoded.push('/');
    }

    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            decoded.push('/');
            i += 1;
        } else if b == b'%' && i + 2 < bytes.len() {
            let h1 = bytes[i + 1];
            let h2 = bytes[i + 2];
            match (h1, h2) {
                (b'2', b'e') | (b'2', b'E') => {
                    decoded.push('.');
                    i += 3;
                }
                (b'2', b'f') | (b'2', b'F') | (b'5', b'c') | (b'5', b'C') => {
                    decoded.push('/');
                    i += 3;
                }
                _ => {
                    decoded.push(b as char);
                    i += 1;
                }
            }
        } else {
            decoded.push(b as char);
            i += 1;
        }
    }

    // 3. Resolve dot segments (RFC 3986 Section 5.2.4)
    let mut segments: Vec<&str> = Vec::with_capacity(16);
    for part in decoded.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            if segments.pop().is_none() {
                return Err(RouterError::InvalidPath {
                    detail: "Path traversal escaping URI root".into(),
                });
            }
        } else {
            segments.push(part);
        }
    }

    // 4. Rebuild normalized path
    let mut out = String::with_capacity(path.len() + 1);
    out.push('/');
    for (idx, seg) in segments.iter().enumerate() {
        if idx > 0 {
            out.push('/');
        }
        out.push_str(seg);
    }
    if path.ends_with('/') && !out.ends_with('/') {
        out.push('/');
    }

    Ok(Cow::Owned(out))
}

// ============================================================================
// 2. Common Prefix Calculation (Radix Tree Compilation Helper)
// ============================================================================

/// Computes common prefix length between two strings during Radix tree compilation.
///
/// Uses natural 64-bit SWAR chunking (8 bytes per iteration) with zero heap allocation
/// and portable ALU operations across all architectures.
#[inline]
pub fn common_prefix_len(a: &str, b: &str) -> usize {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    let min_len = ab.len().min(bb.len());
    let mut i = 0;

    // Fast 64-bit SWAR chunking for 8 bytes per step
    while i + 8 <= min_len {
        let wa = u64::from_le_bytes(ab[i..i + 8].try_into().unwrap());
        let wb = u64::from_le_bytes(bb[i..i + 8].try_into().unwrap());
        let diff = wa ^ wb;
        if diff != 0 {
            return i + (diff.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }

    // Remainder scalar comparison
    while i < min_len && ab[i] == bb[i] {
        i += 1;
    }
    i
}

// ============================================================================
// 3. Flat Cache-Resident Integer-Block Matcher (16-Byte Nodes)
// ============================================================================

/// Precomputed bitmasks for zero-cost length-guarded integer matching (1..=8 bytes).
pub static LEN_MASKS: [u64; 9] = [
    0x0000_0000_0000_0000,
    0x0000_0000_0000_00FF,
    0x0000_0000_0000_FFFF,
    0x0000_0000_00FF_FFFF,
    0x0000_0000_FFFF_FFFF,
    0x0000_00FF_FFFF_FFFF,
    0x0000_FFFF_FFFF_FFFF,
    0x00FF_FFFF_FFFF_FFFF,
    0xFFFF_FFFF_FFFF_FFFF,
];

/// Reads up to 8 bytes from `bytes` as a little-endian `u64`.
#[inline(always)]
pub fn read_chunk_u64(bytes: &[u8]) -> u64 {
    if bytes.len() >= 8 {
        let raw = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const u64) };
        u64::from_le(raw)
    } else {
        let mut buf = [0u8; 8];
        buf[..bytes.len()].copy_from_slice(bytes);
        u64::from_le_bytes(buf)
    }
}

/// A 16-byte integer-block node for contiguous URI routing (4 nodes per 64B cache line).
#[repr(C, align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegerBlockNode {
    /// 8-byte chunk of the path segment (little-endian u64).
    pub chunk: u64,
    /// Contiguous start offset of child nodes in the flat node arena.
    pub children_start: u16,
    /// Pattern index in `pattern_routes` (0xFFFF = NONE).
    pub pattern_idx: u16,
    /// Byte length of `chunk` (1..=8, or 0 for root).
    pub len: u8,
    /// Number of child nodes in the flat arena (0..=255).
    pub children_count: u8,
    /// 1 if prefix matching is allowed (can be used as prefix when path continues); 0 if exact-only.
    pub is_prefix: u8,
    /// Padding byte to guarantee exact 16-byte alignment.
    pub _pad: u8,
}

const _: () = assert!(std::mem::size_of::<IntegerBlockNode>() == 16);
const _: () = assert!(std::mem::align_of::<IntegerBlockNode>() == 16);

/// Result record of a matched pattern during URI traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MatchedPattern {
    /// Matched pattern index.
    pub pattern_idx: u16,
    /// `true` if the entire URI matched exactly up to this pattern.
    pub is_exact: bool,
}

#[derive(Debug, Clone, Default)]
struct PrefixBuilderNode {
    prefix: String,
    pattern_idx: Option<usize>,
    is_prefix: bool,
    children: Vec<PrefixBuilderNode>,
}

impl PrefixBuilderNode {
    fn insert(&mut self, path: &str, pattern_idx: usize, is_prefix: bool) {
        if path.is_empty() {
            self.pattern_idx = Some(pattern_idx);
            self.is_prefix = self.is_prefix || is_prefix;
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
                child.insert(&path[common_len..], pattern_idx, is_prefix);
            } else {
                let common_prefix = child.prefix[..common_len].to_string();
                let child_suffix = child.prefix[common_len..].to_string();
                let path_suffix = path[common_len..].to_string();

                let split_node = PrefixBuilderNode {
                    prefix: common_prefix,
                    pattern_idx: if path_suffix.is_empty() {
                        Some(pattern_idx)
                    } else {
                        None
                    },
                    is_prefix: if path_suffix.is_empty() {
                        is_prefix
                    } else {
                        false
                    },
                    children: Vec::new(),
                };

                child.prefix = child_suffix;
                let old_child = std::mem::replace(child, split_node);
                self.children[child_idx].children.push(old_child);

                if !path_suffix.is_empty() {
                    self.children[child_idx].children.push(PrefixBuilderNode {
                        prefix: path_suffix,
                        pattern_idx: Some(pattern_idx),
                        is_prefix,
                        children: Vec::new(),
                    });
                }
            }
        } else {
            self.children.push(PrefixBuilderNode {
                prefix: path.to_string(),
                pattern_idx: Some(pattern_idx),
                is_prefix,
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
            let old_pattern_idx = self.pattern_idx.take();
            let old_is_prefix = std::mem::replace(&mut self.is_prefix, false);

            let mut intermediate_child = PrefixBuilderNode {
                prefix: tail_prefix,
                pattern_idx: old_pattern_idx,
                is_prefix: old_is_prefix,
                children: old_children,
            };

            intermediate_child.normalize_chunk_lengths();
            self.children.push(intermediate_child);
        }
    }
}

/// Flat, cache-resident Integer-Block Matcher for Layer 7 routing.
#[derive(Debug, Clone, Default)]
pub struct PathPrefixMatcher {
    builder: Option<PrefixBuilderNode>,
    nodes: Vec<IntegerBlockNode>,
}

impl PathPrefixMatcher {
    /// Creates a new empty integer-block matcher.
    pub fn new() -> Self {
        Self {
            builder: Some(PrefixBuilderNode::default()),
            nodes: Vec::new(),
        }
    }

    /// Inserts a pattern into the matcher with explicit prefix matching flag.
    pub fn insert_pattern(&mut self, path: &str, pattern_idx: usize, is_prefix: bool) {
        if let Some(ref mut builder) = self.builder {
            builder.insert(path, pattern_idx, is_prefix);
        } else {
            let mut b = PrefixBuilderNode::default();
            b.insert(path, pattern_idx, is_prefix);
            self.builder = Some(b);
        }
    }

    /// Inserts a prefix pattern (defaulting to `is_prefix = true` for backwards compatibility).
    pub fn insert(&mut self, path: &str, pattern_idx: usize) {
        self.insert_pattern(path, pattern_idx, true);
    }

    /// Compiles the internal hierarchical tree into a flat, cache-resident node arena.
    ///
    /// BFS ordering ensures that all child nodes for any given parent are placed
    /// contiguously in memory (`nodes[children_start .. children_start + children_count]`).
    pub fn compile(&mut self) {
        let Some(mut builder) = self.builder.take() else {
            return;
        };

        builder.normalize_chunk_lengths();

        let mut nodes = Vec::new();
        let mut queue = VecDeque::new();

        let root_node = IntegerBlockNode {
            chunk: 0,
            children_start: 0,
            pattern_idx: builder.pattern_idx.map_or(0xFFFF, |idx| idx as u16),
            len: 0,
            children_count: 0,
            is_prefix: if builder.is_prefix { 1 } else { 0 },
            _pad: 0,
        };
        nodes.push(root_node);
        queue.push_back((builder, 0usize));

        while let Some((parent_b, parent_idx)) = queue.pop_front() {
            if parent_b.children.is_empty() {
                continue;
            }

            let start = nodes.len();
            let count = parent_b.children.len();

            nodes[parent_idx].children_start = start as u16;
            nodes[parent_idx].children_count = count as u8;

            for child_b in &parent_b.children {
                let mut buf = [0u8; 8];
                let b = child_b.prefix.as_bytes();
                let copy_len = b.len().min(8);
                buf[..copy_len].copy_from_slice(&b[..copy_len]);
                let chunk = u64::from_le_bytes(buf);

                let node = IntegerBlockNode {
                    chunk,
                    children_start: 0,
                    pattern_idx: child_b.pattern_idx.map_or(0xFFFF, |idx| idx as u16),
                    len: copy_len as u8,
                    children_count: 0,
                    is_prefix: if child_b.is_prefix { 1 } else { 0 },
                    _pad: 0,
                };
                nodes.push(node);
            }

            for (i, child_b) in parent_b.children.into_iter().enumerate() {
                let child_idx = start + i;
                queue.push_back((child_b, child_idx));
            }
        }

        self.nodes = nodes;
    }

    /// Matches `path` against the flat integer table, collecting all matched pattern records
    /// from root down to the deepest node into `buf`.
    ///
    /// Returns the number of matches found.
    /// Traversal is strictly $\mathcal{O}(P)$ (where $P$ is matched prefix length) and early-terminates
    /// as soon as no child branch matches.
    #[inline]
    pub fn match_path(&self, path: &str, buf: &mut [MatchedPattern; 32]) -> usize {
        if self.nodes.is_empty() {
            return 0;
        }
        let mut count = 0;
        let mut current_idx = 0;
        let mut remaining = path.as_bytes();

        // Check root node
        if self.nodes[0].pattern_idx != 0xFFFF {
            buf[count] = MatchedPattern {
                pattern_idx: self.nodes[0].pattern_idx,
                is_exact: remaining.is_empty(),
            };
            count += 1;
        }

        while !remaining.is_empty() {
            let node = &self.nodes[current_idx];
            let start = node.children_start as usize;
            let c_count = node.children_count as usize;
            if c_count == 0 {
                break;
            }

            let chunk = read_chunk_u64(remaining);
            let rem_len = remaining.len();
            let mut matched_child = None;

            for child_idx in start..start + c_count {
                let child = &self.nodes[child_idx];
                let clen = child.len as usize;
                if rem_len >= clen {
                    let mask = LEN_MASKS[clen];
                    if ((chunk ^ child.chunk) & mask) == 0 {
                        matched_child = Some(child_idx);
                        break;
                    }
                }
            }

            let Some(child_idx) = matched_child else {
                break;
            };

            let child = &self.nodes[child_idx];
            remaining = &remaining[child.len as usize..];
            current_idx = child_idx;

            if child.pattern_idx != 0xFFFF && count < buf.len() {
                let is_exact = remaining.is_empty();
                if is_exact || child.is_prefix != 0 {
                    buf[count] = MatchedPattern {
                        pattern_idx: child.pattern_idx,
                        is_exact,
                    };
                    count += 1;
                }
            }
        }

        count
    }

    /// Matches `path` against the flat prefix table, collecting all matched route indices
    /// from root down to the deepest node into `buf`.
    #[inline]
    pub fn match_prefixes(&self, path: &str, buf: &mut [usize; 32]) -> usize {
        let mut p_buf = [MatchedPattern::default(); 32];
        let count = self.match_path(path, &mut p_buf);
        for i in 0..count {
            buf[i] = p_buf[i].pattern_idx as usize;
        }
        count
    }

    /// Returns a slice of the compiled nodes in the flat arena.
    #[inline(always)]
    pub fn nodes(&self) -> &[IntegerBlockNode] {
        &self.nodes
    }
}

/// Prefetches a memory block into the CPU L1 Data Cache (T0 / PLDL1KEEP)
/// to prioritize cache residency and prevent Pseudo-LRU (PLRU) eviction.
#[inline(always)]
pub fn prefetch_l1_keep(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{}]", in(reg) ptr, options(nostack, readonly));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_clean_path_happy_path() {
        assert!(is_clean_path(b"/api/v1/users"));
        assert!(is_clean_path(b"/healthz"));
        assert!(is_clean_path(b"/"));
        assert!(is_clean_path(b"/assets/style.css"));
        assert!(is_clean_path(b"/images/logo.png"));
    }

    #[test]
    fn test_is_clean_path_detects_anomalies() {
        assert!(!is_clean_path(b"/api//v1/users"));
        assert!(!is_clean_path(b"/api/v1/../users"));
        assert!(!is_clean_path(b"/api/v1/./users"));
        assert!(!is_clean_path(b"/api/%2e%2e/users"));
        assert!(!is_clean_path(b"/api\\users"));
        assert!(!is_clean_path(b"/api/users\0admin"));
        assert!(!is_clean_path(b"relative/path"));
    }

    #[test]
    fn test_normalize_path_happy_path_borrowed() {
        let path = "/api/v1/users";
        let res = normalize_path(path).unwrap();
        assert!(matches!(res, Cow::Borrowed(_)));
        assert_eq!(res.as_ref(), "/api/v1/users");
    }

    #[test]
    fn test_normalize_path_merges_slashes() {
        let res = normalize_path("/api//v1///users").unwrap();
        assert_eq!(res.as_ref(), "/api/v1/users");

        let res_root = normalize_path("//").unwrap();
        assert_eq!(res_root.as_ref(), "/");
    }

    #[test]
    fn test_normalize_path_collapses_dots() {
        let res = normalize_path("/api/v1/./users").unwrap();
        assert_eq!(res.as_ref(), "/api/v1/users");

        let res2 = normalize_path("/api/v1/../v2/users").unwrap();
        assert_eq!(res2.as_ref(), "/api/v2/users");

        let res3 = normalize_path("/api/v1/../").unwrap();
        assert_eq!(res3.as_ref(), "/api/");
    }

    #[test]
    fn test_normalize_path_decodes_percent_traversal() {
        let res = normalize_path("/api/%2e%2e/v2").unwrap();
        assert_eq!(res.as_ref(), "/v2");

        let res_slash = normalize_path("/api%2fv1/users").unwrap();
        assert_eq!(res_slash.as_ref(), "/api/v1/users");
    }

    #[test]
    fn test_normalize_path_rejects_root_breakout() {
        assert!(normalize_path("/..").is_err());
        assert!(normalize_path("/api/../../secret").is_err());
        assert!(normalize_path("/%2e%2e/etc/passwd").is_err());
    }

    #[test]
    fn test_normalize_path_rejects_null_byte() {
        assert!(normalize_path("/api/users\0admin").is_err());
    }

    #[test]
    fn test_swar_common_prefix_len_equivalence() {
        let cases = [
            ("", "", 0),
            ("/api/v1/users", "/api/v1/orders", 8),
            (
                "/api/v1/clusters/us-east/tenants",
                "/api/v1/clusters/us-east/tenants",
                32,
            ),
            (
                "/api/v1/clusters/us-east/tenants/a",
                "/api/v1/clusters/us-east/tenants/b",
                33,
            ),
            ("/short", "/shorter", 6),
            ("/a", "/b", 1),
            ("abcdefg", "abcdefh", 6),
            ("0123456789abcdef", "0123456789abcdeg", 15),
            (
                "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                62,
            ),
            (
                "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_diff",
                "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ!diff",
                62,
            ),
        ];

        for (a, b, expected) in cases {
            assert_eq!(common_prefix_len(a, b), expected, "Mismatch for ({a}, {b})");
        }
    }

    #[test]
    fn test_integer_block_node_size() {
        assert_eq!(std::mem::size_of::<IntegerBlockNode>(), 16);
        assert_eq!(std::mem::align_of::<IntegerBlockNode>(), 16);
    }

    #[test]
    fn test_slash_block_prefix_matcher_insert_and_match() {
        let mut matcher = PathPrefixMatcher::new();
        matcher.insert("/api/v1", 1);
        matcher.insert("/api/v2", 2);
        matcher.insert("/api", 3);
        matcher.insert("/api/v1/users", 4);
        matcher.compile();

        let mut buf = [0usize; 32];

        // Should match /api (3), /api/v1 (1), /api/v1/users (4)
        let count = matcher.match_prefixes("/api/v1/users/42", &mut buf);
        assert_eq!(&buf[..count], &[3, 1, 4]);

        // Longest-first via reverse iteration:
        let mut reversed = buf[..count].to_vec();
        reversed.reverse();
        assert_eq!(reversed, vec![4, 1, 3]);

        // Match /api/v2
        let count = matcher.match_prefixes("/api/v2/items", &mut buf);
        assert_eq!(&buf[..count], &[3, 2]);

        // Match only /api
        let count = matcher.match_prefixes("/api/v3/orders", &mut buf);
        assert_eq!(&buf[..count], &[3]);

        // Unknown path -> no matches
        let count = matcher.match_prefixes("/unknown/path", &mut buf);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_slash_block_prefix_matcher_root_prefix() {
        let mut matcher = PathPrefixMatcher::new();
        matcher.insert("/", 0);
        matcher.insert("/healthz", 1);
        matcher.compile();

        let mut buf = [0usize; 32];
        let count = matcher.match_prefixes("/healthz", &mut buf);
        assert_eq!(&buf[..count], &[0, 1]);

        let count = matcher.match_prefixes("/other", &mut buf);
        assert_eq!(&buf[..count], &[0]);
    }

    #[test]
    fn test_slash_block_prefix_matcher_deep_prefix_near_miss() {
        let mut matcher = PathPrefixMatcher::new();
        matcher.insert(
            "/api/v1/clusters/us-east/tenants/corp-alpha/analytics/realtime/events/v2",
            1,
        );
        matcher.insert("/api/v1/clusters/us-east/tenants/corp-alpha/analytics", 2);
        matcher.insert("/api/v1/clusters", 3);
        matcher.compile();

        let mut buf = [0usize; 32];

        // Near-miss at realtime events v3 -> should match clusters (3) and analytics (2)
        let count = matcher.match_prefixes(
            "/api/v1/clusters/us-east/tenants/corp-alpha/analytics/realtime/events/v3",
            &mut buf,
        );
        assert_eq!(&buf[..count], &[3, 2]);

        // Divergence at corp-beta -> should match clusters (3)
        let count =
            matcher.match_prefixes("/api/v1/clusters/us-east/tenants/corp-beta/other", &mut buf);
        assert_eq!(&buf[..count], &[3]);
    }

    #[test]
    fn test_integer_block_matcher_exact_and_prefix_unification() {
        let mut matcher = PathPrefixMatcher::new();
        // Exact /api/v1/users -> pattern 10 (exact)
        matcher.insert_pattern("/api/v1/users", 10, false);
        // Prefix /api/v1 -> pattern 20 (prefix)
        matcher.insert_pattern("/api/v1", 20, true);
        // Exact /healthz -> pattern 30 (exact)
        matcher.insert_pattern("/healthz", 30, false);
        matcher.compile();

        let mut buf = [MatchedPattern::default(); 32];

        // 1. Exact match /healthz
        let count = matcher.match_path("/healthz", &mut buf);
        assert_eq!(count, 1);
        assert_eq!(
            buf[0],
            MatchedPattern {
                pattern_idx: 30,
                is_exact: true
            }
        );

        // 2. Traversal past /healthz (e.g. /healthz/deep) -> should NOT match exact-only pattern 30
        let count = matcher.match_path("/healthz/deep", &mut buf);
        assert_eq!(count, 0);

        // 3. Exact match /api/v1/users -> hits prefix /api/v1 (not exact) and exact /api/v1/users (exact)
        let count = matcher.match_path("/api/v1/users", &mut buf);
        assert_eq!(count, 2);
        assert_eq!(
            buf[0],
            MatchedPattern {
                pattern_idx: 20,
                is_exact: false
            }
        );
        assert_eq!(
            buf[1],
            MatchedPattern {
                pattern_idx: 10,
                is_exact: true
            }
        );

        // 4. Ongoing path /api/v1/users/42 -> exact 10 does not match, prefix 20 matches
        let count = matcher.match_path("/api/v1/users/42", &mut buf);
        assert_eq!(count, 1);
        assert_eq!(
            buf[0],
            MatchedPattern {
                pattern_idx: 20,
                is_exact: false
            }
        );
    }
}
