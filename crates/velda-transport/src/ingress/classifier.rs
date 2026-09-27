//! Traffic dispatch path for incoming transport listeners.

use std::fmt;

/// Identified traffic dispatch path for an incoming connection or datagram.
///
/// Determined strictly by the listener's declared `application.protocol`:
/// - `application.protocol == "raw"` -> [`PathKind::L4Direct`] (fast-path byte forwarding).
/// - `application.protocol != "raw"` -> [`PathKind::L7Handoff`] (handed off to Composer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathKind {
    /// Pure L4 direct byte/datagram proxying to upstream backend (application: raw).
    L4Direct,
    /// L7 application protocol traffic requiring handoff to Composer (application != raw).
    L7Handoff,
}

impl PathKind {
    /// Returns `true` if this path is raw L4 direct forwarding.
    #[inline]
    pub const fn is_l4(&self) -> bool {
        matches!(self, Self::L4Direct)
    }

    /// Returns `true` if this path requires L7 protocol handoff.
    #[inline]
    pub const fn is_l7(&self) -> bool {
        matches!(self, Self::L7Handoff)
    }
}

impl fmt::Display for PathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::L4Direct => f.write_str("L4Direct"),
            Self::L7Handoff => f.write_str("L7Handoff"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_kind_predicates() {
        assert!(PathKind::L4Direct.is_l4());
        assert!(!PathKind::L4Direct.is_l7());
        assert_eq!(PathKind::L4Direct.to_string(), "L4Direct");

        assert!(PathKind::L7Handoff.is_l7());
        assert!(!PathKind::L7Handoff.is_l4());
        assert_eq!(PathKind::L7Handoff.to_string(), "L7Handoff");
    }
}
