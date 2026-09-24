//! TCP listener and socket configuration.

use std::time::Duration;

/// Configuration parameters for TCP listeners and accepted sockets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpListenerConfig {
    /// Enable `TCP_NODELAY` (disable Nagle's algorithm) on accepted connections.
    pub nodelay: bool,
    /// Keepalive probe interval.
    pub keepalive: Option<Duration>,
    /// Socket listen backlog depth.
    pub backlog: u32,
    /// Receive buffer size hint.
    pub recv_buffer_size: Option<usize>,
    /// Send buffer size hint.
    pub send_buffer_size: Option<usize>,
}

impl Default for TcpListenerConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            keepalive: Some(Duration::from_secs(60)),
            backlog: 1024,
            recv_buffer_size: None,
            send_buffer_size: None,
        }
    }
}

impl TcpListenerConfig {
    /// Creates a new configuration with sensible high-performance defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to enable `TCP_NODELAY`.
    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = nodelay;
        self
    }

    /// Sets the socket listen backlog size.
    pub fn with_backlog(mut self, backlog: u32) -> Self {
        self.backlog = backlog;
        self
    }

    /// Sets the TCP keepalive duration.
    pub fn with_keepalive(mut self, keepalive: Option<Duration>) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Sets the receive buffer size hint.
    pub fn with_recv_buffer_size(mut self, size: usize) -> Self {
        self.recv_buffer_size = Some(size);
        self
    }

    /// Sets the send buffer size hint.
    pub fn with_send_buffer_size(mut self, size: usize) -> Self {
        self.send_buffer_size = Some(size);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = TcpListenerConfig::default();
        assert!(cfg.nodelay);
        assert_eq!(cfg.backlog, 1024);
        assert_eq!(cfg.keepalive, Some(Duration::from_secs(60)));
        assert_eq!(cfg.recv_buffer_size, None);
        assert_eq!(cfg.send_buffer_size, None);
    }

    #[test]
    fn test_builder_pattern() {
        let cfg = TcpListenerConfig::new()
            .with_nodelay(false)
            .with_backlog(2048)
            .with_keepalive(None)
            .with_recv_buffer_size(65536)
            .with_send_buffer_size(32768);

        assert!(!cfg.nodelay);
        assert_eq!(cfg.backlog, 2048);
        assert_eq!(cfg.keepalive, None);
        assert_eq!(cfg.recv_buffer_size, Some(65536));
        assert_eq!(cfg.send_buffer_size, Some(32768));
    }
}
