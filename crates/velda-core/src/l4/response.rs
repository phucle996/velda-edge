use std::net::SocketAddr;

/// Action to perform on an L4 connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L4Action {
    /// Forward the connection to the specified target address.
    Forward,
    /// Close the connection cleanly.
    Close,
    /// Reject the connection immediately (e.g. TCP RST).
    Reject,
}

/// Response returned at the L4 transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L4Response {
    pub action: L4Action,
    pub target: Option<SocketAddr>,
}

impl L4Response {
    /// Creates a new L4 response.
    #[inline]
    pub const fn new(action: L4Action, target: Option<SocketAddr>) -> Self {
        Self { action, target }
    }

    /// Creates an L4 forward response directing traffic to a target socket.
    #[inline]
    pub const fn forward(target: SocketAddr) -> Self {
        Self {
            action: L4Action::Forward,
            target: Some(target),
        }
    }

    /// Creates an L4 close response terminating the connection cleanly.
    #[inline]
    pub const fn close() -> Self {
        Self {
            action: L4Action::Close,
            target: None,
        }
    }

    /// Creates an L4 reject response resetting/terminating the connection.
    #[inline]
    pub const fn reject() -> Self {
        Self {
            action: L4Action::Reject,
            target: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l4_response_constructors() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let fwd = L4Response::forward(addr);
        assert_eq!(fwd.action, L4Action::Forward);
        assert_eq!(fwd.target, Some(addr));

        let close = L4Response::close();
        assert_eq!(close.action, L4Action::Close);
        assert_eq!(close.target, None);

        let reject = L4Response::reject();
        assert_eq!(reject.action, L4Action::Reject);
        assert_eq!(reject.target, None);
    }
}
