//! Zero-allocation, high-performance Compressed Radix Trie (Patricia Trie)
//! for Layer 7 path prefix routing.
//!
//! Provides $O(P)$ prefix lookup time (where $P$ is the matched prefix length)
//! with early termination and longest-prefix-first fallback. Traversal operates
//! with zero heap allocations on the request serving hot path.

use velda_core::hardware::{RegisterWidth, global_hardware_topology};

/// Signature for register-width-specific common prefix length calculation.
pub type CommonPrefixLenFn = fn(a: &[u8], b: &[u8]) -> usize;

/// Pre-resolved static function pointer for 64-bit / 32-bit SWAR prefix matching.
///
/// Workload explicitly targets `RegisterWidth::Bits64` (avoiding wider vector overhead on short URI segments).
/// On 64-bit hosts, selects `common_prefix_len_64`; on 32-bit hosts, selects `common_prefix_len_32`.
pub static COMMON_PREFIX_LEN_FN: std::sync::LazyLock<CommonPrefixLenFn> =
    std::sync::LazyLock::new(|| {
        let caps = global_hardware_topology().cpu.register_capabilities;
        match caps.select_for_workload(RegisterWidth::Bits64) {
            RegisterWidth::Bits64 | RegisterWidth::Bits128 | RegisterWidth::Bits256 => {
                common_prefix_len_64
            }
            RegisterWidth::Bits32 => common_prefix_len_32,
        }
    });

#[inline]
pub fn common_prefix_len(a: &str, b: &str) -> usize {
    (*COMMON_PREFIX_LEN_FN)(a.as_bytes(), b.as_bytes())
}

/// Computes common prefix length using 64-bit SWAR word XOR + hardware TZCNT (`trailing_zeros() / 8`).
#[inline]
pub fn common_prefix_len_64(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    let word_len = len & !7;
    let mut i = 0;

    while i < word_len {
        let wa = u64::from_le_bytes(a[i..i + 8].try_into().unwrap());
        let wb = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        let diff = wa ^ wb;
        if diff != 0 {
            return i + (diff.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }

    while i < len && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Computes common prefix length using dual 32-bit SWAR registers (`2 x 32-bit`) + hardware TZCNT.
#[inline]
pub fn common_prefix_len_32(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    let pair_len = len & !7;
    let mut i = 0;

    // Process 8 bytes per iteration using two 32-bit registers (lo, hi)
    while i < pair_len {
        let a_lo = u32::from_le_bytes(a[i..i + 4].try_into().unwrap());
        let b_lo = u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let diff_lo = a_lo ^ b_lo;
        if diff_lo != 0 {
            return i + (diff_lo.trailing_zeros() / 8) as usize;
        }

        let a_hi = u32::from_le_bytes(a[i + 4..i + 8].try_into().unwrap());
        let b_hi = u32::from_le_bytes(b[i + 4..i + 8].try_into().unwrap());
        let diff_hi = a_hi ^ b_hi;
        if diff_hi != 0 {
            return i + 4 + (diff_hi.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }

    while i + 4 <= len {
        let a_w = u32::from_le_bytes(a[i..i + 4].try_into().unwrap());
        let b_w = u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let diff = a_w ^ b_w;
        if diff != 0 {
            return i + (diff.trailing_zeros() / 8) as usize;
        }
        i += 4;
    }

    while i < len && a[i] == b[i] {
        i += 1;
    }
    i
}

/// A node in the compressed radix trie.
#[derive(Debug, Clone)]
pub struct PrefixTrieNode {
    /// Cached first byte of `prefix` for branch selection without pointer chasing.
    pub first_byte: u8,
    /// Edge prefix segment.
    pub prefix: String,
    /// Registered route index in `prefix_routes` if a route prefix terminates at this node.
    pub route_index: Option<usize>,
    /// Sorted or compact child nodes branching from this node.
    pub children: Vec<PrefixTrieNode>,
}

impl PrefixTrieNode {
    /// Creates a new node with the specified prefix and optional route index.
    pub fn new(prefix: String, route_index: Option<usize>) -> Self {
        let first_byte = prefix.as_bytes().first().copied().unwrap_or(0);
        Self {
            first_byte,
            prefix,
            route_index,
            children: Vec::new(),
        }
    }

    /// Inserts a path into the trie, splitting nodes as necessary according to Radix Trie invariants.
    pub fn insert(&mut self, path: &str, route_index: usize) {
        if path.is_empty() {
            self.route_index = Some(route_index);
            return;
        }

        let target_byte = path.as_bytes()[0];

        if let Some(child_idx) = self
            .children
            .iter()
            .position(|c| c.first_byte == target_byte)
        {
            let child = &mut self.children[child_idx];
            let common_len = common_prefix_len(&child.prefix, path);

            if common_len == child.prefix.len() {
                // Existing child prefix is fully matched; recurse on remainder of path.
                child.insert(&path[common_len..], route_index);
            } else {
                // Split child into common prefix parent and divergent child branches.
                let common_prefix = child.prefix[..common_len].to_string();
                let child_suffix = child.prefix[common_len..].to_string();
                let path_suffix = path[common_len..].to_string();

                let split_node = PrefixTrieNode::new(
                    common_prefix,
                    if path_suffix.is_empty() {
                        Some(route_index)
                    } else {
                        None
                    },
                );

                // Reconfigure the existing child with its remaining suffix.
                child.prefix = child_suffix;
                child.first_byte = child.prefix.as_bytes()[0];

                let old_child = std::mem::replace(child, split_node);
                self.children[child_idx].children.push(old_child);

                if !path_suffix.is_empty() {
                    self.children[child_idx]
                        .children
                        .push(PrefixTrieNode::new(path_suffix, Some(route_index)));
                }
            }
        } else {
            self.children
                .push(PrefixTrieNode::new(path.to_string(), Some(route_index)));
        }
    }
}

impl Default for PrefixTrieNode {
    fn default() -> Self {
        Self::new(String::new(), None)
    }
}

/// Compressed Radix Trie holding compiled prefix routing trees for a listener.
#[derive(Debug, Clone, Default)]
pub struct PrefixTrie {
    root: PrefixTrieNode,
}

impl PrefixTrie {
    /// Creates a new empty prefix trie.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a pattern into the prefix trie.
    pub fn insert(&mut self, path: &str, route_index: usize) {
        self.root.insert(path, route_index);
    }

    /// Matches `path` against the prefix trie, collecting all matched route indices
    /// from root down to the deepest node into `buf`.
    ///
    /// Returns the number of matches found.
    /// Traversal is strictly $O(P)$ (where $P$ is matched prefix length) and early-terminates
    /// as soon as no child branch matches, without scanning the remainder of the URI path.
    #[inline]
    pub fn match_prefixes(&self, path: &str, buf: &mut [usize; 32]) -> usize {
        let mut count = 0;
        let mut current = &self.root;
        let mut remaining = path;

        if let Some(idx) = current.route_index {
            buf[count] = idx;
            count += 1;
        }

        while !remaining.is_empty() {
            let target_byte = remaining.as_bytes()[0];
            if let Some(child) = current
                .children
                .iter()
                .find(|c| c.first_byte == target_byte)
            {
                if remaining.starts_with(&child.prefix) {
                    if let Some(idx) = child.route_index
                        && count < buf.len()
                    {
                        buf[count] = idx;
                        count += 1;
                    }
                    remaining = &remaining[child.prefix.len()..];
                    current = child;
                } else {
                    // Branch diverged: early stop
                    break;
                }
            } else {
                // No child branch matched first byte: early stop
                break;
            }
        }

        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trie_insert_and_match() {
        let mut trie = PrefixTrie::new();
        trie.insert("/api/v1", 1);
        trie.insert("/api/v2", 2);
        trie.insert("/api", 3);
        trie.insert("/api/v1/users", 4);

        let mut buf = [0usize; 32];

        // Should match /api (3), /api/v1 (1), /api/v1/users (4)
        let count = trie.match_prefixes("/api/v1/users/42", &mut buf);
        assert_eq!(&buf[..count], &[3, 1, 4]);

        // Longest-first via reverse iteration:
        let mut reversed = buf[..count].to_vec();
        reversed.reverse();
        assert_eq!(reversed, vec![4, 1, 3]);

        // Match /api/v2
        let count = trie.match_prefixes("/api/v2/items", &mut buf);
        assert_eq!(&buf[..count], &[3, 2]);

        // Match only /api
        let count = trie.match_prefixes("/api/v3/orders", &mut buf);
        assert_eq!(&buf[..count], &[3]);

        // Unknown path -> no matches
        let count = trie.match_prefixes("/unknown/path", &mut buf);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_trie_root_prefix() {
        let mut trie = PrefixTrie::new();
        trie.insert("/", 0);
        trie.insert("/healthz", 1);

        let mut buf = [0usize; 32];
        let count = trie.match_prefixes("/healthz", &mut buf);
        assert_eq!(&buf[..count], &[0, 1]);

        let count = trie.match_prefixes("/other", &mut buf);
        assert_eq!(&buf[..count], &[0]);
    }

    #[test]
    fn test_trie_deep_prefix_near_miss() {
        let mut trie = PrefixTrie::new();
        trie.insert(
            "/api/v1/clusters/us-east/tenants/corp-alpha/analytics/realtime/events/v2",
            1,
        );
        trie.insert("/api/v1/clusters/us-east/tenants/corp-alpha/analytics", 2);
        trie.insert("/api/v1/clusters", 3);

        let mut buf = [0usize; 32];

        // Near-miss at real-time events v3 -> should match clusters (3) and analytics (2)
        let count = trie.match_prefixes(
            "/api/v1/clusters/us-east/tenants/corp-alpha/analytics/realtime/events/v3",
            &mut buf,
        );
        assert_eq!(&buf[..count], &[3, 2]);

        // Divergence at corp-beta -> should match clusters (3)
        let count =
            trie.match_prefixes("/api/v1/clusters/us-east/tenants/corp-beta/other", &mut buf);
        assert_eq!(&buf[..count], &[3]);
    }

    #[test]
    fn test_swar_common_prefix_len_64_and_32_equivalence() {
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
        ];

        for (a, b, expected) in cases {
            assert_eq!(common_prefix_len(a, b), expected, "Mismatch for ({a}, {b})");
            assert_eq!(
                common_prefix_len_64(a.as_bytes(), b.as_bytes()),
                expected,
                "64-bit mismatch for ({a}, {b})"
            );
            assert_eq!(
                common_prefix_len_32(a.as_bytes(), b.as_bytes()),
                expected,
                "32-bit mismatch for ({a}, {b})"
            );
        }
    }
}
