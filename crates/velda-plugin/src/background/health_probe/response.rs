//! Zero-Allocation L1D Cache-Resident Static HTTP/1.1 Wire Responses & Adaptive SWAR Evaluator.
//!
//! Emits pre-compiled HTTP/1.1 response frames directly from a single contiguous, 64-byte aligned
//! memory block resident in L1 Data Cache (`Content-Length: 0`, `Connection: close`), completely
//! eliminating dynamic string formatting, heap allocation, and double pointer dereference overhead.
//!
//! # Architecture & Microarchitectural Invariants
//! - **Contiguous L1D Cache-Resident Wire Responses**: All 3 response payloads (200, 503, 404)
//!   are packed contiguously into a single 193-byte struct (`#[repr(C, align(64))]`), spanning
//!   exactly 4 cache lines ($\approx 0.5\%$ of 32KB L1D).
//! - **Adaptive Register SWAR Matching**: Evaluates path tokens using either a single 64-bit
//!   general-purpose register (`u64`) or dual 32-bit registers (`u32_lo`, `u32_hi`) for legacy/embedded
//!   targets or 64-bit register pressure scenarios.
//! - **Bootstrap Function Pointer Resolution**: The optimal evaluator function (`eval_swar_64`,
//!   `eval_swar_32x2`, or `eval_fallback`) is resolved once at initialization, giving 0 hardware sniffing
//!   on the request hot path.
//! - **Branchless Index Calculation**: Translates probe match and health flags into wire response
//!   indices using pure ALU arithmetic: `index = 2 - probe_match - probe_ok`.

use super::plugin::HealthProbeConfig;
use super::state::HealthState;

// ============================================================================
// Contiguous L1D Cache-Resident Wire Response Payloads
// ============================================================================

/// Pre-compiled HTTP/1.1 200 OK wire response frame.
pub const RESP_200: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// Pre-compiled HTTP/1.1 503 Service Unavailable wire response frame.
pub const RESP_503: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// Pre-compiled HTTP/1.1 404 Not Found wire response frame.
pub const RESP_404: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

pub const LEN_200: usize = RESP_200.len(); // 57
pub const LEN_503: usize = RESP_503.len(); // 74
pub const LEN_404: usize = RESP_404.len(); // 64
pub const TOTAL_WIRE_LEN: usize = LEN_200 + LEN_503 + LEN_404; // 195

/// Contiguous 64-byte aligned table holding all 3 HTTP/1.1 wire responses back-to-back.
///
/// Total size: 195 bytes (57 + 74 + 64). Fits comfortably inside 4 contiguous 64-byte
/// cache lines of CPU L1 Data Cache.
#[repr(C, align(64))]
pub struct ContiguousWireResponses {
    /// Raw HTTP/1.1 wire frames:
    /// - `[0..57]`   : HTTP/1.1 200 OK (57 bytes)
    /// - `[57..131]` : HTTP/1.1 503 Service Unavailable (74 bytes)
    /// - `[131..195]`: HTTP/1.1 404 Not Found (64 bytes)
    pub raw: [u8; TOTAL_WIRE_LEN],
}

const fn make_wire_responses() -> ContiguousWireResponses {
    let mut raw = [0u8; TOTAL_WIRE_LEN];
    let mut i = 0;
    let mut j = 0;
    while j < LEN_200 {
        raw[i] = RESP_200[j];
        i += 1;
        j += 1;
    }
    j = 0;
    while j < LEN_503 {
        raw[i] = RESP_503[j];
        i += 1;
        j += 1;
    }
    j = 0;
    while j < LEN_404 {
        raw[i] = RESP_404[j];
        i += 1;
        j += 1;
    }
    ContiguousWireResponses { raw }
}

/// Static immutable wire response payloads baked directly into `.rodata` and aligned to 64 bytes.
pub static WIRE_RESPONSES: ContiguousWireResponses = make_wire_responses();

/// Byte offsets for response index `0` (200), `1` (503), and `2` (404).
const OFFSETS: [u8; 3] = [0, 57, 131];

/// Byte lengths for response index `0` (200), `1` (503), and `2` (404).
const LENGTHS: [u8; 3] = [57, 74, 64];

/// Returns a reference to the pre-baked wire response slice from L1D cache by index.
///
/// # Indices
/// - `0`: `HTTP/1.1 200 OK`
/// - `1`: `HTTP/1.1 503 Service Unavailable`
/// - `2`: `HTTP/1.1 404 Not Found` (default fallback for any invalid index)
#[inline(always)]
pub fn get_wire_response(index: usize) -> &'static [u8] {
    let idx = index.min(2);
    let start = OFFSETS[idx] as usize;
    let end = start + LENGTHS[idx] as usize;
    &WIRE_RESPONSES.raw[start..end]
}

// ============================================================================
// SWAR Tokens & Pre-computation
// ============================================================================

/// Pre-computed SWAR bitmasks and match tokens for an 8-byte path prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarToken {
    /// 64-bit integer representation of target path.
    pub val_u64: u64,
    /// 64-bit bitmask isolating relevant path bytes.
    pub mask_u64: u64,
    /// Lower 32-bit word (bytes 0..4).
    pub lo: u32,
    /// Bitmask for lower 32-bit word.
    pub mask_lo: u32,
    /// Upper 32-bit word (bytes 4..8).
    pub hi: u32,
    /// Bitmask for upper 32-bit word.
    pub mask_hi: u32,
    /// Byte offset for trailing space check (12 for 8-byte paths; 0 if included in 8-byte token).
    pub trail_offset: u8,
    /// Bitmask for trailing byte check (0xFF if checking trailing space, 0x00 otherwise).
    pub trail_mask: u8,
}

impl SwarToken {
    /// Creates an empty token representing no match.
    pub const fn empty() -> Self {
        Self {
            val_u64: 0,
            mask_u64: 0,
            lo: 0,
            mask_lo: 0,
            hi: 0,
            mask_hi: 0,
            trail_offset: 0,
            trail_mask: 0,
        }
    }
}

/// Builds an 8-byte SWAR token from a given path string.
///
/// Supports paths up to 8 bytes (e.g. `/livez`, `/readyz`, `/healthz`, `/ping`).
fn build_token(path: &str) -> Option<SwarToken> {
    let bytes = path.as_bytes();
    if bytes.is_empty() || bytes.len() > 8 {
        return None;
    }

    if bytes.len() <= 7 {
        // Path + space delimiter fits completely within 8 bytes
        let mut val = [0u8; 8];
        val[..bytes.len()].copy_from_slice(bytes);
        val[bytes.len()] = b' ';

        let mut mask = [0u8; 8];
        for b in mask.iter_mut().take(bytes.len() + 1) {
            *b = 0xFF;
        }

        let val_u64 = u64::from_le_bytes(val);
        let mask_u64 = u64::from_le_bytes(mask);
        let lo = u32::from_le_bytes([val[0], val[1], val[2], val[3]]);
        let mask_lo = u32::from_le_bytes([mask[0], mask[1], mask[2], mask[3]]);
        let hi = u32::from_le_bytes([val[4], val[5], val[6], val[7]]);
        let mask_hi = u32::from_le_bytes([mask[4], mask[5], mask[6], mask[7]]);

        Some(SwarToken {
            val_u64,
            mask_u64,
            lo,
            mask_lo,
            hi,
            mask_hi,
            trail_offset: 0,
            trail_mask: 0,
        })
    } else {
        // Exactly 8 bytes (e.g. "/healthz"); delimiter sits at offset 12 (4 bytes "GET " + 8 bytes path)
        let mut val = [0u8; 8];
        val.copy_from_slice(bytes);

        let val_u64 = u64::from_le_bytes(val);
        let mask_u64 = u64::MAX;
        let lo = u32::from_le_bytes([val[0], val[1], val[2], val[3]]);
        let mask_lo = u32::MAX;
        let hi = u32::from_le_bytes([val[4], val[5], val[6], val[7]]);
        let mask_hi = u32::MAX;

        Some(SwarToken {
            val_u64,
            mask_u64,
            lo,
            mask_lo,
            hi,
            mask_hi,
            trail_offset: 12,
            trail_mask: 0xFF,
        })
    }
}

/// Pre-computed SWAR tokens for liveness and readiness probe endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthProbeTokens {
    /// Token for liveness path.
    pub live: SwarToken,
    /// Token for readiness path.
    pub ready: SwarToken,
    /// True if both endpoints are compatible with SWAR path matching.
    pub can_use_swar: bool,
}

impl HealthProbeTokens {
    /// Pre-computes SWAR tokens from configured liveness and readiness path strings.
    pub fn from_paths(liveness_path: &str, readiness_path: &str) -> Self {
        let live_tok = build_token(liveness_path);
        let ready_tok = build_token(readiness_path);

        match (live_tok, ready_tok) {
            (Some(live), Some(ready)) => Self {
                live,
                ready,
                can_use_swar: true,
            },
            _ => Self {
                live: SwarToken::empty(),
                ready: SwarToken::empty(),
                can_use_swar: false,
            },
        }
    }
}

// ============================================================================
// Evaluator Function Pointer & Implementations
// ============================================================================

/// Function pointer signature for probe evaluation.
pub type ProbeEvalFn = fn(
    req_buf: &[u8],
    state: &HealthState,
    tokens: &HealthProbeTokens,
    config: &HealthProbeConfig,
) -> &'static [u8];

/// High-performance 64-bit register SWAR evaluator.
///
/// Uses a single 64-bit general-purpose register to inspect 8 bytes of the request path
/// simultaneously in 1 CPU cycle. Computes response index branchlessly via ALU arithmetic:
/// `index = 2 - probe_match - probe_ok`.
#[inline(always)]
pub fn eval_swar_64(
    req_buf: &[u8],
    state: &HealthState,
    tokens: &HealthProbeTokens,
    _config: &HealthProbeConfig,
) -> &'static [u8] {
    if req_buf.len() < 12 {
        return get_wire_response(2);
    }
    // Check "GET " method prefix in 1 32-bit register (0x20544547 is u32::from_le_bytes(*b"GET "))
    let method = u32::from_le_bytes([req_buf[0], req_buf[1], req_buf[2], req_buf[3]]);
    if method != 0x20544547 {
        return get_wire_response(2);
    }

    let word = u64::from_le_bytes([
        req_buf[4],
        req_buf[5],
        req_buf[6],
        req_buf[7],
        req_buf[8],
        req_buf[9],
        req_buf[10],
        req_buf[11],
    ]);

    let diff_live = (word ^ tokens.live.val_u64) & tokens.live.mask_u64;
    let trail_live = (req_buf
        .get(tokens.live.trail_offset as usize)
        .copied()
        .unwrap_or(0)
        ^ b' ')
        & tokens.live.trail_mask;
    let m_live = ((diff_live == 0) as u32) & ((trail_live == 0) as u32);

    let diff_ready = (word ^ tokens.ready.val_u64) & tokens.ready.mask_u64;
    let trail_ready = (req_buf
        .get(tokens.ready.trail_offset as usize)
        .copied()
        .unwrap_or(0)
        ^ b' ')
        & tokens.ready.trail_mask;
    let m_ready = ((diff_ready == 0) as u32) & ((trail_ready == 0) as u32);

    let probe_match = m_live | m_ready;
    let live_ok = state.is_live() as u32;
    let ready_ok = (state.is_ready() & !state.is_draining()) as u32;
    let probe_ok = (m_live & live_ok) | (m_ready & ready_ok);

    // Branchless index calculation:
    // probe_match == 0 -> index = 2 (404)
    // probe_match == 1, probe_ok == 0 -> index = 1 (503)
    // probe_match == 1, probe_ok == 1 -> index = 0 (200)
    let index = (2 - probe_match - probe_ok) as usize;
    get_wire_response(index)
}

/// High-performance dual 32-bit register SWAR evaluator.
///
/// Designed for 32-bit hardware architectures (ARMv7, i686, RISC-V 32) or systems under
/// 64-bit register pressure. Evaluates path tokens using two 32-bit GP registers (`u32_lo` and `u32_hi`)
/// without requiring native 64-bit register support. Computes response index branchlessly.
#[inline(always)]
pub fn eval_swar_32x2(
    req_buf: &[u8],
    state: &HealthState,
    tokens: &HealthProbeTokens,
    _config: &HealthProbeConfig,
) -> &'static [u8] {
    if req_buf.len() < 12 {
        return get_wire_response(2);
    }
    let method = u32::from_le_bytes([req_buf[0], req_buf[1], req_buf[2], req_buf[3]]);
    if method != 0x20544547 {
        return get_wire_response(2);
    }

    let lo = u32::from_le_bytes([req_buf[4], req_buf[5], req_buf[6], req_buf[7]]);
    let hi = u32::from_le_bytes([req_buf[8], req_buf[9], req_buf[10], req_buf[11]]);

    let diff_live = ((lo ^ tokens.live.lo) & tokens.live.mask_lo)
        | ((hi ^ tokens.live.hi) & tokens.live.mask_hi);
    let trail_live = (req_buf
        .get(tokens.live.trail_offset as usize)
        .copied()
        .unwrap_or(0)
        ^ b' ')
        & tokens.live.trail_mask;
    let m_live = ((diff_live == 0) as u32) & ((trail_live == 0) as u32);

    let diff_ready = ((lo ^ tokens.ready.lo) & tokens.ready.mask_lo)
        | ((hi ^ tokens.ready.hi) & tokens.ready.mask_hi);
    let trail_ready = (req_buf
        .get(tokens.ready.trail_offset as usize)
        .copied()
        .unwrap_or(0)
        ^ b' ')
        & tokens.ready.trail_mask;
    let m_ready = ((diff_ready == 0) as u32) & ((trail_ready == 0) as u32);

    let probe_match = m_live | m_ready;
    let live_ok = state.is_live() as u32;
    let ready_ok = (state.is_ready() & !state.is_draining()) as u32;
    let probe_ok = (m_live & live_ok) | (m_ready & ready_ok);

    let index = (2 - probe_match - probe_ok) as usize;
    get_wire_response(index)
}

/// Fallback evaluator for custom paths exceeding 8 bytes or non-standard token lengths.
#[inline(always)]
pub fn eval_fallback(
    req_buf: &[u8],
    state: &HealthState,
    _tokens: &HealthProbeTokens,
    config: &HealthProbeConfig,
) -> &'static [u8] {
    let req_str = std::str::from_utf8(req_buf).unwrap_or("");
    let line = req_str.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("");
    if method != "GET" {
        return get_wire_response(2);
    }
    let path = parts.next().unwrap_or("");
    evaluate_path(config, state, path)
}

/// Evaluates incoming probe request buffer directly using SWAR or fallback,
/// without indirect function pointer dispatch overhead.
#[inline(always)]
pub fn eval_probe_request(
    req_buf: &[u8],
    state: &HealthState,
    tokens: &HealthProbeTokens,
    config: &HealthProbeConfig,
) -> &'static [u8] {
    if !tokens.can_use_swar {
        eval_fallback(req_buf, state, tokens, config)
    } else if cfg!(target_pointer_width = "64") {
        eval_swar_64(req_buf, state, tokens, config)
    } else {
        eval_swar_32x2(req_buf, state, tokens, config)
    }
}

/// Resolves the optimal probe evaluation function pointer at bootstrap / initialization.
///
/// Ensures zero dynamic hardware checking or CPU feature sniffing on the request hot path.
pub fn resolve_eval_fn(tokens: &HealthProbeTokens) -> ProbeEvalFn {
    if !tokens.can_use_swar {
        eval_fallback
    } else if cfg!(target_pointer_width = "64") {
        eval_swar_64
    } else {
        eval_swar_32x2
    }
}

/// Evaluates an incoming HTTP request path string against configured liveness/readiness paths
/// and returns the appropriate zero-byte static wire response.
#[inline(always)]
pub fn evaluate_path(config: &HealthProbeConfig, state: &HealthState, path: &str) -> &'static [u8] {
    if path == config.liveness_path {
        if state.is_live() {
            get_wire_response(0)
        } else {
            get_wire_response(1)
        }
    } else if path == config.readiness_path {
        if state.is_ready() && !state.is_draining() {
            get_wire_response(0)
        } else {
            get_wire_response(1)
        }
    } else {
        get_wire_response(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_responses_exact_lengths_and_offsets() {
        assert_eq!(WIRE_RESPONSES.raw.len(), 195);
        assert_eq!(
            RESP_200,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert_eq!(
            RESP_503,
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert_eq!(
            RESP_404,
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert_eq!(get_wire_response(0), RESP_200);
        assert_eq!(get_wire_response(1), RESP_503);
        assert_eq!(get_wire_response(2), RESP_404);
        assert_eq!(get_wire_response(999), RESP_404);
    }

    #[test]
    fn test_swar_64_and_32x2_exact_matches() {
        let config = HealthProbeConfig {
            liveness_path: "/livez".into(),
            readiness_path: "/readyz".into(),
            ..Default::default()
        };
        let tokens = HealthProbeTokens::from_paths(&config.liveness_path, &config.readiness_path);
        assert!(tokens.can_use_swar);

        let state = HealthState::new();

        // 1. /livez: default is live -> 200
        let req_live = b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert_eq!(eval_swar_64(req_live, &state, &tokens, &config), RESP_200);
        assert_eq!(eval_swar_32x2(req_live, &state, &tokens, &config), RESP_200);

        state.set_live(false);
        assert_eq!(eval_swar_64(req_live, &state, &tokens, &config), RESP_503);
        assert_eq!(eval_swar_32x2(req_live, &state, &tokens, &config), RESP_503);

        // 2. /readyz: default is not ready -> 503
        let req_ready = b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert_eq!(eval_swar_64(req_ready, &state, &tokens, &config), RESP_503);
        assert_eq!(
            eval_swar_32x2(req_ready, &state, &tokens, &config),
            RESP_503
        );

        state.set_ready(true);
        assert_eq!(eval_swar_64(req_ready, &state, &tokens, &config), RESP_200);
        assert_eq!(
            eval_swar_32x2(req_ready, &state, &tokens, &config),
            RESP_200
        );

        // Draining readiness flips to 503
        state.set_draining(true);
        assert_eq!(eval_swar_64(req_ready, &state, &tokens, &config), RESP_503);
        assert_eq!(
            eval_swar_32x2(req_ready, &state, &tokens, &config),
            RESP_503
        );

        // 3. Unknown path -> 404
        let req_unknown = b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert_eq!(
            eval_swar_64(req_unknown, &state, &tokens, &config),
            RESP_404
        );
        assert_eq!(
            eval_swar_32x2(req_unknown, &state, &tokens, &config),
            RESP_404
        );

        // 4. Invalid method or short request -> 404
        assert_eq!(
            eval_swar_64(b"POST /livez HTTP/1.1", &state, &tokens, &config),
            RESP_404
        );
        assert_eq!(eval_swar_64(b"GET /", &state, &tokens, &config), RESP_404);
        assert_eq!(
            eval_swar_32x2(b"POST /livez HTTP/1.1", &state, &tokens, &config),
            RESP_404
        );
    }

    #[test]
    fn test_swar_8_byte_path_trailing_space() {
        let config = HealthProbeConfig {
            liveness_path: "/healthz".into(),
            readiness_path: "/readyz".into(),
            ..Default::default()
        };
        let tokens = HealthProbeTokens::from_paths(&config.liveness_path, &config.readiness_path);
        assert!(tokens.can_use_swar);
        assert_eq!(tokens.live.trail_offset, 12);
        assert_eq!(tokens.live.trail_mask, 0xFF);

        let state = HealthState::new();
        let req_healthz = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert_eq!(
            eval_swar_64(req_healthz, &state, &tokens, &config),
            RESP_200
        );
        assert_eq!(
            eval_swar_32x2(req_healthz, &state, &tokens, &config),
            RESP_200
        );

        // Path with extra suffix should NOT match /healthz
        let req_healthz_sub = b"GET /healthz/sub HTTP/1.1\r\n";
        assert_eq!(
            eval_swar_64(req_healthz_sub, &state, &tokens, &config),
            RESP_404
        );
        assert_eq!(
            eval_swar_32x2(req_healthz_sub, &state, &tokens, &config),
            RESP_404
        );
    }

    #[test]
    fn test_fallback_evaluator() {
        let config = HealthProbeConfig {
            liveness_path: "/custom/very/long/liveness/path".into(),
            readiness_path: "/custom/very/long/readiness/path".into(),
            ..Default::default()
        };
        let tokens = HealthProbeTokens::from_paths(&config.liveness_path, &config.readiness_path);
        assert!(!tokens.can_use_swar);

        let eval_fn = resolve_eval_fn(&tokens);
        let state = HealthState::new();

        let req = b"GET /custom/very/long/liveness/path HTTP/1.1\r\n\r\n";
        assert_eq!(eval_fn(req, &state, &tokens, &config), RESP_200);

        let req_unknown = b"GET /other HTTP/1.1\r\n\r\n";
        assert_eq!(eval_fn(req_unknown, &state, &tokens, &config), RESP_404);
    }
}
