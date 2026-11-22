//! Single-Pass Zero-Allocation Byte Scanner for LLM Token Usage.
//!
//! Tail-Window Scan Mechanism:
//!
//! ```text
//!   body[0 .. N]
//!        │
//!        ▼  Slice only last <= 4096 B (fits in L1d cache, ignores earlier SSE chunks)
//!   search_window = body[N.saturating_sub(4096) .. N]
//!        │
//!        ├─► Reverse-scan (`rposition`) for last `"usage"` followed by `:` and `{`
//!        │
//!        └─► Single forward byte loop (in CPU registers, max 1024 B window):
//!              brace depth == 1..n:
//!                "key" : <digits> ──► accumulate u64 directly (no float, no Vec, no JSON DOM)
//!                  ├─ prompt_tokens / input_tokens / promptTokenCount
//!                  ├─ completion_tokens / output_tokens / candidatesTokenCount
//!                  ├─ total_tokens / totalTokenCount
//!                  └─ cached_tokens / cache_read_input_tokens / cachedContentTokenCount
//! ```

/// Standardized token usage parsed from upstream LLM response bodies.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cached_prompt_tokens: u64,
}

#[inline(always)]
const fn is_ws(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\r' || b == b'\n'
}

/// Scans the tail `4096` bytes of `body` for the last `"usage": { ... }` object
/// and extracts all token counters in a single linear pass.
pub fn scan_token_usage(body: &[u8]) -> Option<TokenUsage> {
    if body.is_empty() {
        return None;
    }

    let search_window = if body.len() > 4096 {
        &body[body.len() - 4096..]
    } else {
        body
    };

    let mut search_slice = search_window;
    let mut usage_obj_slice = None;

    while let Some(idx) = rfind_subslice(search_slice, b"\"usage\"") {
        let after = &search_slice[idx + 7..];
        let mut p = 0;
        while p < after.len() && is_ws(after[p]) {
            p += 1;
        }
        if p < after.len() && after[p] == b':' {
            p += 1;
            while p < after.len() && is_ws(after[p]) {
                p += 1;
            }
            if p < after.len() && after[p] == b'{' {
                let obj_end = (p + 1024).min(after.len());
                usage_obj_slice = Some(&after[p..obj_end]);
                break;
            }
        }
        search_slice = &search_slice[..idx];
    }

    let obj = usage_obj_slice?;
    let mut usage = TokenUsage::default();
    let mut found_any = false;

    // Single linear pass over the `"usage": { ... }` object.
    let mut i = 1;
    let mut depth: i32 = 1;
    while i < obj.len() && depth > 0 {
        match obj[i] {
            b'{' => {
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth -= 1;
                i += 1;
            }
            b'"' => {
                let key_start = i + 1;
                let mut key_end = key_start;
                while key_end < obj.len() && obj[key_end] != b'"' {
                    if obj[key_end] == b'\\' {
                        key_end += 2;
                    } else {
                        key_end += 1;
                    }
                }
                if key_end >= obj.len() {
                    break;
                }
                let key = &obj[key_start..key_end];
                i = key_end + 1;

                while i < obj.len() && is_ws(obj[i]) {
                    i += 1;
                }
                if i < obj.len() && obj[i] == b':' {
                    i += 1;
                    while i < obj.len() && is_ws(obj[i]) {
                        i += 1;
                    }
                    if i < obj.len() && obj[i].is_ascii_digit() {
                        let mut val: u64 = 0;
                        while i < obj.len() && obj[i].is_ascii_digit() {
                            val = val
                                .saturating_mul(10)
                                .saturating_add((obj[i] - b'0') as u64);
                            i += 1;
                        }
                        match key {
                            b"prompt_tokens" | b"input_tokens" | b"promptTokenCount" => {
                                usage.prompt_tokens = val;
                                found_any = true;
                            }
                            b"completion_tokens" | b"output_tokens" | b"candidatesTokenCount" => {
                                usage.completion_tokens = val;
                                found_any = true;
                            }
                            b"total_tokens" | b"totalTokenCount" => {
                                usage.total_tokens = val;
                                found_any = true;
                            }
                            b"cached_tokens"
                            | b"cache_read_input_tokens"
                            | b"cachedContentTokenCount" => {
                                usage.cached_prompt_tokens = val;
                                found_any = true;
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => i += 1,
        }
    }

    if !found_any {
        return None;
    }

    if usage.total_tokens == 0 && (usage.prompt_tokens > 0 || usage.completion_tokens > 0) {
        usage.total_tokens = usage.prompt_tokens.saturating_add(usage.completion_tokens);
    }

    Some(usage)
}

#[inline(always)]
fn rfind_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_unary_and_sse_single_pass() {
        let openai = br#"{"id":"1","model":"gpt-4o","usage":{"prompt_tokens":1540,"completion_tokens":320,"total_tokens":1860,"prompt_tokens_details":{"cached_tokens":1024}}}"#;
        let u = scan_token_usage(openai).unwrap();
        assert_eq!(u.prompt_tokens, 1540);
        assert_eq!(u.completion_tokens, 320);
        assert_eq!(u.total_tokens, 1860);
        assert_eq!(u.cached_prompt_tokens, 1024);

        let sse = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":45,\"completion_tokens\":12,\"total_tokens\":57}}\n\ndata: [DONE]\n\n";
        let u2 = scan_token_usage(sse).unwrap();
        assert_eq!(u2.prompt_tokens, 45);
        assert_eq!(u2.completion_tokens, 12);
        assert_eq!(u2.total_tokens, 57);
        assert_eq!(u2.cached_prompt_tokens, 0);
    }
}
