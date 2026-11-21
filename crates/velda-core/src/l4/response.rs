//! Layer 4 Transport Decision & Target (`velda_core::l4::response`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                            L4Response ACTION DISPATCH                                   │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  L4Response { action, target_ip: u128, target_port_be: u16 }                            │
//! │    ├── Forward ──► Connect / splice / datagram-forward to (target_ip, target_port_be)   │
//! │    ├── Close   ──► Graceful FIN / socket close                                          │
//! │    └── Reject  ──► Immediate RST / drop                                                 │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L4Response {
    pub action: L4Action,
    pub target_ip: u128,
    pub target_port_be: u16,
}

impl L4Response {
    /// Creates a new L4 response.
    #[inline]
    pub const fn new(action: L4Action, target_ip: u128, target_port_be: u16) -> Self {
        Self {
            action,
            target_ip,
            target_port_be,
        }
    }

    /// Creates an L4 forward response directing traffic to a target `(ip, port_be)`.
    #[inline]
    pub const fn forward(target_ip: u128, target_port_be: u16) -> Self {
        Self {
            action: L4Action::Forward,
            target_ip,
            target_port_be,
        }
    }

    /// Creates an L4 close response terminating the connection cleanly.
    #[inline]
    pub const fn close() -> Self {
        Self {
            action: L4Action::Close,
            target_ip: 0,
            target_port_be: 0,
        }
    }

    /// Creates an L4 reject response resetting/terminating the connection.
    #[inline]
    pub const fn reject() -> Self {
        Self {
            action: L4Action::Reject,
            target_ip: 0,
            target_port_be: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l4_response_constructors() {
        let ip = crate::lut::ip::v4_to_u128(0x7F00_0001);
        let port_be = 8080u16.to_be();
        let fwd = L4Response::forward(ip, port_be);
        assert_eq!(fwd.action, L4Action::Forward);
        assert_eq!(fwd.target_ip, ip);
        assert_eq!(fwd.target_port_be, port_be);

        let close = L4Response::close();
        assert_eq!(close.action, L4Action::Close);
        assert_eq!(close.target_ip, 0);
        assert_eq!(close.target_port_be, 0);

        let reject = L4Response::reject();
        assert_eq!(reject.action, L4Action::Reject);
        assert_eq!(reject.target_ip, 0);
        assert_eq!(reject.target_port_be, 0);
    }
}
