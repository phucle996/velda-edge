//! HTTP/2 Two-Level Flow Control Accounting (RFC 9113 §5.2 & §6.9).
//!
//! ```text
//!  RFC 9113 §6.9 Two-Level Window Accounting:
//!    Connection Window [conn_recv / conn_send]  <── caps aggregate DATA across ALL streams
//!         ∩
//!    Per-Stream Window [recv_win / send_win]    <── caps DATA on a single stream_id
//!
//!  - Receive path: subtracts full wire DATA length (including padding), emits batched
//!    WINDOW_UPDATE once unacked bytes >= initial_window / 2.
//!  - Send path: min(conn.send_window, stream.send_window) bounds flush; peer
//!    SETTINGS_INITIAL_WINDOW_SIZE applies signed delta across active streams (overflow -> Err).
//! ```

use crate::proto::frame::{DEFAULT_INITIAL_WINDOW_SIZE, H2ErrorCode, MAX_WINDOW_SIZE};

/// Connection-level HTTP/2 send & receive window tracker.
#[derive(Debug, Clone)]
pub struct ConnFlowControl {
    pub recv_window: i64,
    pub recv_consumed_unacked: u32,
    pub send_window: i64,
    initial_conn_recv_window: u32,
}

impl ConnFlowControl {
    #[inline]
    pub fn new(initial_conn_recv_window: u32) -> Self {
        Self {
            recv_window: initial_conn_recv_window as i64,
            recv_consumed_unacked: 0,
            send_window: DEFAULT_INITIAL_WINDOW_SIZE as i64,
            initial_conn_recv_window,
        }
    }

    /// Accounts `wire_len` bytes against the connection receive window.
    #[inline]
    pub fn consume_recv(&mut self, wire_len: i64) -> Result<(), H2ErrorCode> {
        if self.recv_window < wire_len {
            return Err(H2ErrorCode::FLOW_CONTROL_ERROR);
        }
        self.recv_window -= wire_len;
        Ok(())
    }

    /// Records `bytes` consumed by the application, returning a batched `WINDOW_UPDATE`
    /// increment once unacknowledged consumption reaches half of the initial window.
    #[inline]
    pub fn release_recv(&mut self, bytes: u32) -> Option<u32> {
        if bytes == 0 {
            return None;
        }
        self.recv_consumed_unacked = self.recv_consumed_unacked.saturating_add(bytes);
        let threshold = (self.initial_conn_recv_window / 2).max(1);
        if self.recv_consumed_unacked >= threshold {
            let inc = self.recv_consumed_unacked;
            self.recv_consumed_unacked = 0;
            self.recv_window += inc as i64;
            Some(inc)
        } else {
            None
        }
    }

    /// Credits peer `WINDOW_UPDATE(stream_id = 0, increment)`, rejecting > `2^31 - 1`.
    #[inline]
    pub fn apply_peer_window_update(&mut self, increment: u32) -> Result<(), H2ErrorCode> {
        let next = self.send_window + increment as i64;
        if next > MAX_WINDOW_SIZE as i64 {
            return Err(H2ErrorCode::FLOW_CONTROL_ERROR);
        }
        self.send_window = next;
        Ok(())
    }
}

/// Per-stream HTTP/2 send & receive window tracker.
#[derive(Debug, Clone)]
pub struct StreamFlowControl {
    pub recv_window: i64,
    pub recv_consumed_unacked: u32,
    pub send_window: i64,
    initial_stream_recv_window: u32,
}

impl StreamFlowControl {
    #[inline]
    pub fn new(initial_stream_recv_window: u32, peer_initial_send_window: u32) -> Self {
        Self {
            recv_window: initial_stream_recv_window as i64,
            recv_consumed_unacked: 0,
            send_window: peer_initial_send_window as i64,
            initial_stream_recv_window,
        }
    }

    /// Accounts `wire_len` bytes against this stream's receive window.
    #[inline]
    pub fn consume_recv(&mut self, wire_len: i64) -> Result<(), H2ErrorCode> {
        if self.recv_window < wire_len {
            return Err(H2ErrorCode::FLOW_CONTROL_ERROR);
        }
        self.recv_window -= wire_len;
        Ok(())
    }

    /// Credits consumed payload bytes and returns a batched stream `WINDOW_UPDATE` increment.
    #[inline]
    pub fn release_recv(&mut self, bytes: u32) -> Option<u32> {
        if bytes == 0 {
            return None;
        }
        self.recv_consumed_unacked = self.recv_consumed_unacked.saturating_add(bytes);
        let threshold = (self.initial_stream_recv_window / 2).max(1);
        if self.recv_consumed_unacked >= threshold {
            let inc = self.recv_consumed_unacked;
            self.recv_consumed_unacked = 0;
            self.recv_window += inc as i64;
            Some(inc)
        } else {
            None
        }
    }

    /// Credits peer `WINDOW_UPDATE(stream_id, increment)`, rejecting > `2^31 - 1`.
    #[inline]
    pub fn apply_peer_window_update(&mut self, increment: u32) -> Result<(), H2ErrorCode> {
        let next = self.send_window + increment as i64;
        if next > MAX_WINDOW_SIZE as i64 {
            return Err(H2ErrorCode::FLOW_CONTROL_ERROR);
        }
        self.send_window = next;
        Ok(())
    }

    /// Applies signed `SETTINGS_INITIAL_WINDOW_SIZE` delta per RFC 9113 §6.9.2.
    #[inline]
    pub fn apply_initial_window_delta(&mut self, delta: i64) -> Result<(), H2ErrorCode> {
        let next = self.send_window + delta;
        if next > MAX_WINDOW_SIZE as i64 {
            return Err(H2ErrorCode::FLOW_CONTROL_ERROR);
        }
        self.send_window = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conn_and_stream_window_overflow_and_batched_updates() {
        let mut conn = ConnFlowControl::new(128 * 1024);
        assert!(conn.consume_recv(64 * 1024).is_ok());
        assert!(conn.release_recv(32 * 1024).is_none());
        assert_eq!(conn.release_recv(32 * 1024), Some(64 * 1024));

        // Connection window overflow > 2^31 - 1 -> FLOW_CONTROL_ERROR
        assert_eq!(
            conn.apply_peer_window_update(MAX_WINDOW_SIZE),
            Err(H2ErrorCode::FLOW_CONTROL_ERROR)
        );

        let mut st = StreamFlowControl::new(64 * 1024, DEFAULT_INITIAL_WINDOW_SIZE);
        // Negative delta shrinks send window cleanly to 0 without error
        assert!(
            st.apply_initial_window_delta(-(DEFAULT_INITIAL_WINDOW_SIZE as i64))
                .is_ok()
        );
        assert_eq!(st.send_window, 0);

        // Positive delta causing overflow -> FLOW_CONTROL_ERROR
        st.send_window = MAX_WINDOW_SIZE as i64 - 10;
        assert_eq!(
            st.apply_initial_window_delta(100),
            Err(H2ErrorCode::FLOW_CONTROL_ERROR)
        );
    }
}
