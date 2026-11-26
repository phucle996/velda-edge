//! URL Rewrite Pre-Upstream Plugin (`url_rewrite::plugin`).
//!
//! ```text
//!   Sync / Config Compile
//!     ├── path_rules[]  ──► AhoCorasick (MatchKind::LeftmostLongest) + literal replacements[]
//!     └── remove_params ──► AhoCorasick (is_match fast-bypass + anchored exact key lookup)
//!
//!   Frontline Worker Hot Path (SupportedProtocols::ALL_HTTP, AccessLevel::Write Header / None Payload)
//!     on_upstream_request(req, _targets)
//!       ├─► 1. Path pass  : AhoCorasick::find_iter(path_bytes) [stack span buf -> exact len]
//!       ├─► 2. Query pass : Keep (AC fast-check -> SIMD '&' scan -> key filter + append) | Drop | Replace
//!       ├─► 3. Zero-alloc : (!path_changed && !query_changed) -> Action::Continue
//!       └─► 4. Single alloc: BytesMut(exact_len) + simd_copy_nonoverlapping -> PathAndQuery -> Uri
//!                           (invalid PathAndQuery or missing leading '/' -> RejectReason::BadRequest)
//! ```

use aho_corasick::{AhoCorasick, AhoCorasickKind, Anchored, Input, MatchKind, StartKind};
use bytes::{BufMut, BytesMut};
use http::Uri;
use http::uri::PathAndQuery;
use velda_core::{Action, L7Request, RejectReason};

use super::config::{QueryRewriteMode, UrlRewriteConfig};
use super::validation::{validate_config_fields, validate_url_rewrite_config};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::wire_baked::simd_copy_nonoverlapping;

/// Stack-allocated span cap for zero-heap rewrite plans on common requests.
const STACK_SPAN_CAP: usize = 8;

#[derive(Debug, Clone, Copy)]
struct PathMatchSpan {
    start: usize,
    end: usize,
    rule_idx: usize,
}

#[derive(Debug, Clone)]
struct CompiledPathEngine {
    ac: AhoCorasick,
    replacements: Box<[Box<str>]>,
}

#[derive(Debug, Clone)]
enum CompiledQueryEngine {
    Drop,
    Replace(Box<str>),
    Keep {
        remove_ac: Option<AhoCorasick>,
        remove_lens: Box<[usize]>,
        append: Option<Box<str>>,
    },
}

/// High-performance literal URL Rewrite plugin powered exclusively by Aho-Corasick and SIMD.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct UrlRewritePlugin {
    path_engine: Option<CompiledPathEngine>,
    query_engine: CompiledQueryEngine,
    pub id: String,
    pub config: UrlRewriteConfig,
}

impl UrlRewritePlugin {
    /// Compiles and validates a [`UrlRewritePlugin`] from declarative configuration.
    pub fn new(id: impl Into<String>, config: UrlRewriteConfig) -> Result<Self, PluginError> {
        let id = id.into();
        validate_config_fields(&id, &config)?;

        let path_engine = if config.path_rules.is_empty() {
            None
        } else {
            let finds: Vec<&str> = config.path_rules.iter().map(|r| r.find.as_str()).collect();
            let ac = AhoCorasick::builder()
                .match_kind(MatchKind::LeftmostLongest)
                .build(&finds)
                .map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{id}': failed to compile path Aho-Corasick automaton: {e}"
                    ))
                })?;
            let replacements: Box<[Box<str>]> = config
                .path_rules
                .iter()
                .map(|r| r.replace.clone().into_boxed_str())
                .collect();
            Some(CompiledPathEngine { ac, replacements })
        };

        let query_engine = match config.query_mode {
            QueryRewriteMode::Drop => CompiledQueryEngine::Drop,
            QueryRewriteMode::Replace => CompiledQueryEngine::Replace(
                config
                    .query_replace
                    .clone()
                    .unwrap_or_default()
                    .into_boxed_str(),
            ),
            QueryRewriteMode::Keep => {
                let (remove_ac, remove_lens) = if config.remove_params.is_empty() {
                    (None, Box::default())
                } else {
                    // DFA with StartKind::Both supports fast unanchored `is_match` and `Anchored::Yes` exact key checks.
                    let ac = AhoCorasick::builder()
                        .kind(Some(AhoCorasickKind::DFA))
                        .start_kind(StartKind::Both)
                        .match_kind(MatchKind::LeftmostLongest)
                        .build(&config.remove_params)
                        .map_err(|e| {
                            PluginError::Config(format!(
                                "Plugin '{id}': failed to compile remove_params automaton: {e}"
                            ))
                        })?;
                    let lens: Box<[usize]> = config.remove_params.iter().map(|k| k.len()).collect();
                    (Some(ac), lens)
                };
                let append = config.query_append.clone().map(String::into_boxed_str);
                CompiledQueryEngine::Keep {
                    remove_ac,
                    remove_lens,
                    append,
                }
            }
        };

        Ok(Self {
            path_engine,
            query_engine,
            id,
            config,
        })
    }

    /// Parses, validates, and compiles a [`UrlRewritePlugin`] from raw [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_url_rewrite_config(&config.id, &config.config)?;
        Self::new(&config.id, cfg)
    }
}

/// Multi-target SIMD scanner locating the first `target` byte (`&` or `=`) in `haystack`.
#[inline(always)]
fn find_byte_simd(haystack: &[u8], target: u8) -> Option<usize> {
    let len = haystack.len();
    if len == 0 {
        return None;
    }
    let ptr = haystack.as_ptr();
    let mut i = 0usize;

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::*;
        let needle = _mm256_set1_epi8(target as i8);
        while i + 32 <= len {
            let chunk = _mm256_loadu_si256(ptr.add(i) as *const __m256i);
            let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, needle)) as u32;
            if mask != 0 {
                return Some(i + mask.trailing_zeros() as usize);
            }
            i += 32;
        }
    }

    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use core::arch::x86_64::*;
        let needle = _mm_set1_epi8(target as i8);
        while i + 16 <= len {
            let chunk = _mm_loadu_si128(ptr.add(i) as *const __m128i);
            let mask = _mm_movemask_epi8(_mm_cmpeq_epi8(chunk, needle)) as u32;
            if mask != 0 {
                return Some(i + mask.trailing_zeros() as usize);
            }
            i += 16;
        }
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let needle = vdupq_n_u8(target);
        while i + 16 <= len {
            let chunk = vld1q_u8(ptr.add(i));
            let eq = vceqq_u8(chunk, needle);
            // Collapse 16 lanes to two u64 words; any matched lane becomes 0xFF.
            let lo = vgetq_lane_u64(vreinterpretq_u64_u8(eq), 0);
            if lo != 0 {
                return Some(i + (lo.to_le().trailing_zeros() as usize >> 3));
            }
            let hi = vgetq_lane_u64(vreinterpretq_u64_u8(eq), 1);
            if hi != 0 {
                return Some(i + 8 + (hi.to_le().trailing_zeros() as usize >> 3));
            }
            i += 16;
        }
    }

    // Portable 64-bit SWAR fallback (also finishes tail < vector width).
    let splat = 0x0101_0101_0101_0101u64 * (target as u64);
    while i + 8 <= len {
        let word = unsafe { core::ptr::read_unaligned(ptr.add(i) as *const u64) };
        let xored = word ^ splat;
        let has_zero = (xored.wrapping_sub(0x0101_0101_0101_0101)) & !xored & 0x8080_8080_8080_8080;
        if has_zero != 0 {
            return Some(i + (has_zero.to_le().trailing_zeros() as usize >> 3));
        }
        i += 8;
    }

    while i < len {
        if unsafe { *ptr.add(i) } == target {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[inline(always)]
fn push_span(
    stack: &mut [PathMatchSpan; STACK_SPAN_CAP],
    stack_len: &mut usize,
    overflow: &mut Vec<PathMatchSpan>,
    span: PathMatchSpan,
) {
    if *stack_len < STACK_SPAN_CAP {
        stack[*stack_len] = span;
        *stack_len += 1;
    } else {
        if overflow.is_empty() {
            overflow.reserve(STACK_SPAN_CAP * 2);
            overflow.extend_from_slice(&stack[..*stack_len]);
        }
        overflow.push(span);
    }
}

#[inline(always)]
fn spans_slice<'a>(
    stack: &'a [PathMatchSpan; STACK_SPAN_CAP],
    stack_len: usize,
    overflow: &'a [PathMatchSpan],
) -> &'a [PathMatchSpan] {
    if overflow.is_empty() {
        &stack[..stack_len]
    } else {
        overflow
    }
}

/// Copies `slice` into uninitialized/spare tail of `BytesMut` via SIMD without bounds loops.
#[inline(always)]
fn append_simd(dst: &mut BytesMut, src: &[u8]) {
    let len = src.len();
    if len == 0 {
        return;
    }
    let old_len = dst.len();
    debug_assert!(dst.capacity() - old_len >= len);
    unsafe {
        simd_copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr().add(old_len), len);
        dst.set_len(old_len + len);
    }
}

impl PreUpstreamPlugin for UrlRewritePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        let path_bytes = req.uri.path().as_bytes();
        let query_str = req.uri.query();
        let query_bytes = query_str.unwrap_or("").as_bytes();

        // 1. Single-pass Aho-Corasick literal path plan (stack spans -> 0 heap alloc for <= 8 matches).
        let mut stack_spans = [PathMatchSpan {
            start: 0,
            end: 0,
            rule_idx: 0,
        }; STACK_SPAN_CAP];
        let mut stack_len = 0usize;
        let mut overflow_spans = Vec::new();
        let mut new_path_len = path_bytes.len();
        let mut path_changed = false;

        if let Some(pe) = &self.path_engine {
            let mut delta: isize = 0;
            for m in pe.ac.find_iter(path_bytes) {
                let rule_idx = m.pattern().as_usize();
                let start = m.start();
                let end = m.end();
                let rep = pe.replacements[rule_idx].as_bytes();
                if rep != &path_bytes[start..end] {
                    path_changed = true;
                }
                delta += rep.len() as isize - (end - start) as isize;
                push_span(
                    &mut stack_spans,
                    &mut stack_len,
                    &mut overflow_spans,
                    PathMatchSpan {
                        start,
                        end,
                        rule_idx,
                    },
                );
            }
            new_path_len = (path_bytes.len() as isize + delta) as usize;
        }

        // 2. Query plan: Keep (+ Aho-Corasick key removal + append) | Drop | Replace.
        let mut kept_query_spans = [(0usize, 0usize); STACK_SPAN_CAP];
        let mut kept_stack_len = 0usize;
        let mut kept_overflow: Vec<(usize, usize)> = Vec::new();
        let mut kept_query_len = 0usize;
        let mut query_static_tail: &[u8] = b"";
        let mut has_output_query = false;
        let mut query_changed = false;

        match &self.query_engine {
            CompiledQueryEngine::Drop => {
                query_changed = query_str.is_some();
            }
            CompiledQueryEngine::Replace(rep) => {
                let rep_bytes = rep.as_bytes();
                query_changed = query_str != Some(rep.as_ref());
                query_static_tail = rep_bytes;
                has_output_query = true;
            }
            CompiledQueryEngine::Keep {
                remove_ac,
                remove_lens,
                append,
            } => {
                let mut filtered = false;
                if let Some(ac) = remove_ac
                    && !query_bytes.is_empty()
                    && ac.is_match(query_bytes)
                {
                    let mut pos = 0usize;
                    while pos <= query_bytes.len() {
                        let rest = &query_bytes[pos..];
                        let seg_end = find_byte_simd(rest, b'&')
                            .map(|idx| pos + idx)
                            .unwrap_or(query_bytes.len());
                        let seg = &query_bytes[pos..seg_end];
                        let key = match find_byte_simd(seg, b'=') {
                            Some(eq) => &seg[..eq],
                            None => seg,
                        };

                        let matched_remove = if key.is_empty() {
                            false
                        } else if let Some(m) = ac.find(Input::new(key).anchored(Anchored::Yes)) {
                            remove_lens[m.pattern().as_usize()] == key.len() && m.end() == key.len()
                        } else {
                            false
                        };

                        if matched_remove {
                            filtered = true;
                        } else if !seg.is_empty() {
                            if kept_query_len > 0 {
                                kept_query_len += 1; // '&' separator
                            }
                            kept_query_len += seg.len();
                            if kept_stack_len < STACK_SPAN_CAP {
                                kept_query_spans[kept_stack_len] = (pos, seg_end);
                                kept_stack_len += 1;
                            } else {
                                if kept_overflow.is_empty() {
                                    kept_overflow.reserve(STACK_SPAN_CAP * 2);
                                    kept_overflow
                                        .extend_from_slice(&kept_query_spans[..kept_stack_len]);
                                }
                                kept_overflow.push((pos, seg_end));
                            }
                        }

                        if seg_end == query_bytes.len() {
                            break;
                        }
                        pos = seg_end + 1;
                    }
                }

                if filtered {
                    query_changed = true;
                } else if !query_bytes.is_empty() {
                    kept_query_len = query_bytes.len();
                    kept_query_spans[0] = (0, query_bytes.len());
                    kept_stack_len = 1;
                }

                if let Some(app) = append {
                    query_static_tail = app.as_bytes();
                    query_changed = true;
                }

                has_output_query = kept_query_len > 0 || !query_static_tail.is_empty();
            }
        }

        // 3. Zero-allocation fast bypass when neither path nor query changed.
        if !path_changed && !query_changed {
            return Action::Continue;
        }

        // 4. Single contiguous BytesMut allocation assembled with SIMD copy.
        let need_amp_between = kept_query_len > 0 && !query_static_tail.is_empty();
        let total_query_len =
            kept_query_len + usize::from(need_amp_between) + query_static_tail.len();
        let total_len = new_path_len
            + if has_output_query {
                1 + total_query_len
            } else {
                0
            };

        let mut buf = BytesMut::with_capacity(total_len);

        if path_changed {
            let pe = self
                .path_engine
                .as_ref()
                .expect("path_changed implies path_engine");
            let spans = spans_slice(&stack_spans, stack_len, &overflow_spans);
            let mut cursor = 0usize;
            for sp in spans {
                append_simd(&mut buf, &path_bytes[cursor..sp.start]);
                append_simd(&mut buf, pe.replacements[sp.rule_idx].as_bytes());
                cursor = sp.end;
            }
            append_simd(&mut buf, &path_bytes[cursor..]);
        } else {
            append_simd(&mut buf, path_bytes);
        }

        if has_output_query {
            buf.put_u8(b'?');
            if kept_query_len > 0 {
                let kspans: &[(usize, usize)] = if kept_overflow.is_empty() {
                    &kept_query_spans[..kept_stack_len]
                } else {
                    &kept_overflow
                };
                for (i, &(s, e)) in kspans.iter().enumerate() {
                    if i > 0 {
                        buf.put_u8(b'&');
                    }
                    append_simd(&mut buf, &query_bytes[s..e]);
                }
            }
            if need_amp_between {
                buf.put_u8(b'&');
            }
            append_simd(&mut buf, query_static_tail);
        }

        let Ok(pq) = PathAndQuery::from_maybe_shared(buf.freeze()) else {
            return Action::Reject(RejectReason::BadRequest);
        };
        if !pq.path().starts_with('/') {
            return Action::Reject(RejectReason::BadRequest);
        }

        let mut parts = req.uri.clone().into_parts();
        parts.path_and_query = Some(pq);
        match Uri::from_parts(parts) {
            Ok(updated_uri) => {
                req.uri = updated_uri;
                Action::Continue
            }
            Err(_) => Action::Reject(RejectReason::BadRequest),
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::Write, AccessLevel::None)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_upstream::url_rewrite::config::PathRewriteRule;
    use http::{Method, Version};
    use velda_core::{Body, HeaderWorkspace};

    fn req_with(uri: &str) -> L7Request {
        L7Request::new(
            Method::GET,
            uri.parse().unwrap(),
            Version::HTTP_11,
            HeaderWorkspace::empty(0),
            Body::Empty,
        )
    }

    #[test]
    fn test_aho_leftmost_longest_literal_path_and_no_magic() {
        let plugin = UrlRewritePlugin::new(
            "rw-multi",
            UrlRewriteConfig {
                path_rules: vec![
                    PathRewriteRule {
                        find: "/api/v1".into(),
                        replace: "/v2".into(),
                    },
                    PathRewriteRule {
                        find: "/api/v1/legacy".into(),
                        replace: "/modern".into(),
                    },
                    PathRewriteRule {
                        find: "/old/".into(),
                        replace: "/new/".into(),
                    },
                ],
                query_mode: QueryRewriteMode::Keep,
                query_replace: None,
                remove_params: Vec::new(),
                query_append: None,
            },
        )
        .unwrap();

        let mut t = 1;
        // LeftmostLongest selects "/api/v1/legacy" over "/api/v1"
        let mut r1 = req_with("/api/v1/legacy/items");
        assert!(matches!(
            plugin.on_upstream_request(&mut r1, &mut t),
            Action::Continue
        ));
        assert_eq!(r1.uri.path(), "/modern/items");

        // Exact literal behavior (no magic boundary trimming)
        let mut r2 = req_with("/api/v1/old/users");
        assert!(matches!(
            plugin.on_upstream_request(&mut r2, &mut t),
            Action::Continue
        ));
        assert_eq!(r2.uri.path(), "/v2/new/users");

        // No-op request bypasses cleanly
        let mut r3 = req_with("/healthz?a=1");
        assert!(matches!(
            plugin.on_upstream_request(&mut r3, &mut t),
            Action::Continue
        ));
        assert_eq!(r3.uri.to_string(), "/healthz?a=1");
    }

    #[test]
    fn test_strip_literal_empty_result_rejects_cleanly() {
        let plugin = UrlRewritePlugin::new(
            "rw-strip",
            UrlRewriteConfig {
                path_rules: vec![PathRewriteRule {
                    find: "/api/v1".into(),
                    replace: String::new(),
                }],
                query_mode: QueryRewriteMode::Keep,
                query_replace: None,
                remove_params: Vec::new(),
                query_append: None,
            },
        )
        .unwrap();

        let mut t = 1;
        let mut r1 = req_with("/api/v1/orders");
        assert!(matches!(
            plugin.on_upstream_request(&mut r1, &mut t),
            Action::Continue
        ));
        assert_eq!(r1.uri.path(), "/orders");

        // Literal replacement of entire "/api/v1" with "" yields "" -> fails missing '/' (no magic '/')
        let mut r2 = req_with("/api/v1");
        assert!(matches!(
            plugin.on_upstream_request(&mut r2, &mut t),
            Action::Reject(RejectReason::BadRequest)
        ));
    }

    #[test]
    fn test_query_modes_drop_replace_and_aho_remove_params_boundary() {
        // 1. Replace mode replaces (does not append!)
        let replace_plugin = UrlRewritePlugin::from_config(&PluginConfig {
            id: "url_rewrite".into(),
            config: serde_json::json!({
                "path_rules": [],
                "query_mode": "replace",
                "query_replace": "ver=2",
                "remove_params": [],
                "query_append": null
            }),
        })
        .unwrap();
        let mut t = 1;
        let mut r1 = req_with("/items?ver=1&extra=9");
        assert!(matches!(
            replace_plugin.on_upstream_request(&mut r1, &mut t),
            Action::Continue
        ));
        assert_eq!(r1.uri.to_string(), "/items?ver=2");

        // 2. Drop mode clears query completely
        let drop_plugin = UrlRewritePlugin::from_config(&PluginConfig {
            id: "url_rewrite".into(),
            config: serde_json::json!({
                "path_rules": [],
                "query_mode": "drop",
                "query_replace": null,
                "remove_params": [],
                "query_append": null
            }),
        })
        .unwrap();
        let mut r2 = req_with("/items?a=1");
        assert!(matches!(
            drop_plugin.on_upstream_request(&mut r2, &mut t),
            Action::Continue
        ));
        assert_eq!(r2.uri.to_string(), "/items");

        // 3. Keep + remove_params + append respects exact key boundaries (`my_token` preserved)
        let filter_plugin = UrlRewritePlugin::from_config(&PluginConfig {
            id: "url_rewrite".into(),
            config: serde_json::json!({
                "path_rules": [{ "find": "/v1/", "replace": "/v2/" }],
                "query_mode": "keep",
                "query_replace": null,
                "remove_params": ["token", "utm_source"],
                "query_append": "edge=1"
            }),
        })
        .unwrap();
        let mut r3 = req_with("/v1/pay?token=secret&my_token=ok&utm_source=ads&flag");
        assert!(matches!(
            filter_plugin.on_upstream_request(&mut r3, &mut t),
            Action::Continue
        ));
        assert_eq!(r3.uri.to_string(), "/v2/pay?my_token=ok&flag&edge=1");

        // When all original params removed, query_append stands alone without leading '&'
        let mut r4 = req_with("/v1/pay?token=secret");
        assert!(matches!(
            filter_plugin.on_upstream_request(&mut r4, &mut t),
            Action::Continue
        ));
        assert_eq!(r4.uri.to_string(), "/v2/pay?edge=1");
    }

    #[test]
    fn test_simd_find_byte_matches_scalar_all_lengths() {
        for len in 0..=96 {
            let mut buf = vec![b'a'; len];
            assert_eq!(find_byte_simd(&buf, b'&'), None);
            if len > 0 {
                for pos in [0, len / 2, len - 1] {
                    buf.fill(b'a');
                    buf[pos] = b'&';
                    assert_eq!(find_byte_simd(&buf, b'&'), Some(pos));
                }
            }
        }
    }
}
