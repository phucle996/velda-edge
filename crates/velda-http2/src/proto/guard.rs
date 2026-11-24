//! HTTP/2 Security & DoS / CVE Protection Guards (RFC 9113 §10.5).
//!
//! ```text
//! Isolates protocol-level abuse defenses so individual CVE patches & regression tests
//! remain self-contained away from framing, HPACK, and async I/O drivers:
//!
//!   1. RapidResetGuard (CVE-2023-44487)
//!      RST_STREAM -> consecutive_resets += 1 (> max_consecutive_resets => ENHANCE_YOUR_CALM)
//!      Reset to 0 ONLY upon clean stream completion (not on new HEADERS intake).
//!
//!   2. ControlFloodGuard (CVE-2019-9512 / 9515 / 9518)
//!      SETTINGS / PING / PRIORITY / WINDOW_UPDATE -> count += 1 (> limit => ENHANCE_YOUR_CALM)
//!      Reset to 0 upon productive HEADERS or DATA payload.
//!
//!   3. ContinuationGuard (CVE-2024-27316)
//!      In-flight HEADERS block -> enforces same stream_id, max_continuation_frames,
//!      and compressed header buffer cap.
//! ```

use bytes::BytesMut;

use crate::proto::frame::H2ErrorCode;

/// Detects CVE-2023-44487 style HTTP/2 Rapid Reset attacks.
#[derive(Debug, Clone)]
pub struct RapidResetGuard {
    consecutive_resets: u32,
    max_consecutive_resets: u32,
}

impl RapidResetGuard {
    #[inline]
    pub fn new(max_consecutive_resets: u32) -> Self {
        Self {
            consecutive_resets: 0,
            max_consecutive_resets,
        }
    }

    #[inline]
    pub fn on_rst_stream(&mut self) -> Result<(), (H2ErrorCode, &'static str)> {
        self.consecutive_resets = self.consecutive_resets.saturating_add(1);
        if self.max_consecutive_resets > 0 && self.consecutive_resets > self.max_consecutive_resets
        {
            return Err((
                H2ErrorCode::ENHANCE_YOUR_CALM,
                "HTTP/2 Rapid Reset threshold exceeded",
            ));
        }
        Ok(())
    }

    #[inline]
    pub fn on_clean_stream_complete(&mut self) {
        self.consecutive_resets = 0;
    }
}

/// Detects excessive control-frame floods (`PING`, `SETTINGS`, `PRIORITY`, `WINDOW_UPDATE`)
/// arriving without intervening `HEADERS`/`DATA` traffic (CVE-2019-9512 / 9515 / 9518).
#[derive(Debug, Clone)]
pub struct ControlFloodGuard {
    unproductive_control_frames: u32,
    limit: u32,
}

impl ControlFloodGuard {
    #[inline]
    pub fn new(max_pending_control_frames: u32) -> Self {
        Self {
            unproductive_control_frames: 0,
            limit: max_pending_control_frames.max(64) * 4,
        }
    }

    #[inline]
    pub fn on_control_frame(&mut self) -> Result<(), (H2ErrorCode, &'static str)> {
        self.unproductive_control_frames = self.unproductive_control_frames.saturating_add(1);
        if self.unproductive_control_frames > self.limit {
            return Err((
                H2ErrorCode::ENHANCE_YOUR_CALM,
                "HTTP/2 excessive control frame flood detected",
            ));
        }
        Ok(())
    }

    #[inline]
    pub fn on_productive_frame(&mut self) {
        self.unproductive_control_frames = 0;
    }
}

/// Tracks and bounds an in-flight `HEADERS` + `CONTINUATION` block against CVE-2024-27316 floods.
#[derive(Debug)]
pub struct PendingHeaderAssembly {
    pub stream_id: u32,
    pub end_stream: bool,
    continuation_count: u32,
    pub buf: BytesMut,
}

impl PendingHeaderAssembly {
    #[inline]
    pub fn start(stream_id: u32, end_stream: bool, initial_fragment: &[u8]) -> Self {
        let mut buf = BytesMut::with_capacity(initial_fragment.len() * 2);
        buf.extend_from_slice(initial_fragment);
        Self {
            stream_id,
            end_stream,
            continuation_count: 0,
            buf,
        }
    }

    #[inline]
    pub fn push_continuation(
        &mut self,
        chunk: &[u8],
        max_continuation_frames: u32,
        max_header_list_size: u32,
    ) -> Result<(), (H2ErrorCode, &'static str)> {
        self.continuation_count = self.continuation_count.saturating_add(1);
        if max_continuation_frames > 0 && self.continuation_count > max_continuation_frames {
            return Err((
                H2ErrorCode::ENHANCE_YOUR_CALM,
                "CONTINUATION flood threshold exceeded",
            ));
        }

        let max_compressed_cap = (max_header_list_size as usize)
            .saturating_mul(2)
            .max(64 * 1024);
        if self.buf.len().saturating_add(chunk.len()) > max_compressed_cap {
            return Err((
                H2ErrorCode::ENHANCE_YOUR_CALM,
                "compressed header block exceeds safety limit",
            ));
        }

        self.buf.extend_from_slice(chunk);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rapid_reset_and_continuation_guards() {
        let mut rr = RapidResetGuard::new(2);
        assert!(rr.on_rst_stream().is_ok());
        assert!(rr.on_rst_stream().is_ok());
        assert_eq!(
            rr.on_rst_stream().unwrap_err().0,
            H2ErrorCode::ENHANCE_YOUR_CALM
        );
        rr.on_clean_stream_complete();
        assert!(rr.on_rst_stream().is_ok());

        let mut hdrs = PendingHeaderAssembly::start(1, false, b"\x82");
        assert!(hdrs.push_continuation(b"\x86", 1, 16_384).is_ok());
        assert_eq!(
            hdrs.push_continuation(b"\x84", 1, 16_384).unwrap_err().0,
            H2ErrorCode::ENHANCE_YOUR_CALM
        );
    }
}
