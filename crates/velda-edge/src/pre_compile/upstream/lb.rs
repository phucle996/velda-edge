//! Pre-compiled load balancing algorithm variants supporting zero-vtable static dispatch.

use velda_core::Endpoint;
use velda_lb::{
    IpHash, LeastConnections, LoadBalancer, PowerOfTwoChoices, Random, RoundRobin,
    SelectionContext, WeightedRoundRobin,
};
use velda_upstream::Upstream;

/// Pre-compiled load balancing algorithm variants.
///
/// Dispatches statically via inline pattern matching without dynamic trait object overhead.
pub enum LbAlgorithm {
    /// [PRE-COMPILED]: Monotonic atomic counter round-robin selection.
    RoundRobin(RoundRobin),
    /// [PRE-COMPILED]: Nginx-style smooth weighted round-robin with thread-sharded state.
    WeightedRoundRobin(WeightedRoundRobin),
    /// [PRE-COMPILED]: Lowest active connections selector reading zero-copy metrics slices.
    LeastConnections(LeastConnections),
    /// [PRE-COMPILED]: Nanosecond thread-local XorShift64 PRNG uniform random selector.
    Random(Random),
    /// [PRE-COMPILED]: Modulo FNV-1a IP/attribute hash deterministic selector.
    IpHash(IpHash),
    /// [PRE-COMPILED]: Power-of-Two-Choices (P2C) bounded load balancing.
    PowerOfTwoChoices(PowerOfTwoChoices),
}

impl std::fmt::Debug for LbAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RoundRobin(_) => write!(f, "RoundRobin"),
            Self::WeightedRoundRobin(_) => write!(f, "WeightedRoundRobin"),
            Self::LeastConnections(_) => write!(f, "LeastConnections"),
            Self::Random(_) => write!(f, "Random"),
            Self::IpHash(_) => write!(f, "IpHash"),
            Self::PowerOfTwoChoices(_) => write!(f, "PowerOfTwoChoices"),
        }
    }
}

impl LoadBalancer for LbAlgorithm {
    #[inline]
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        match self {
            Self::RoundRobin(lb) => lb.select_index(endpoints, ctx),
            Self::WeightedRoundRobin(lb) => lb.select_index(endpoints, ctx),
            Self::LeastConnections(lb) => lb.select_index(endpoints, ctx),
            Self::Random(lb) => lb.select_index(endpoints, ctx),
            Self::IpHash(lb) => lb.select_index(endpoints, ctx),
            Self::PowerOfTwoChoices(lb) => lb.select_index(endpoints, ctx),
        }
    }
}

impl LbAlgorithm {
    /// Compiles declarative algorithm string into pre-compiled balancer variant.
    pub fn from_name(name: &str) -> Self {
        match name.trim().to_ascii_lowercase().as_str() {
            "least_connections" | "least_conn" => Self::LeastConnections(LeastConnections::new()),
            "random" => Self::Random(Random::new()),
            "ip_hash" | "hash" => Self::IpHash(IpHash::new()),
            "weighted_round_robin" | "wrr" => Self::WeightedRoundRobin(WeightedRoundRobin::new()),
            "p2c" | "power_of_two_choices" => Self::PowerOfTwoChoices(PowerOfTwoChoices::new()),
            _ => Self::RoundRobin(RoundRobin::new()),
        }
    }
}

/// Unified Edge upstream pipeline type backed by static LbAlgorithm.
pub type EdgeUpstream = Upstream<LbAlgorithm>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_lb_algorithm_from_name_and_debug() {
        let rr = LbAlgorithm::from_name("round_robin");
        assert_eq!(format!("{rr:?}"), "RoundRobin");

        let lc = LbAlgorithm::from_name("least_connections");
        assert_eq!(format!("{lc:?}"), "LeastConnections");

        let rnd = LbAlgorithm::from_name("random");
        assert_eq!(format!("{rnd:?}"), "Random");

        let ip = LbAlgorithm::from_name("ip_hash");
        assert_eq!(format!("{ip:?}"), "IpHash");

        let wrr = LbAlgorithm::from_name("weighted_round_robin");
        assert_eq!(format!("{wrr:?}"), "WeightedRoundRobin");

        let p2c = LbAlgorithm::from_name("p2c");
        assert_eq!(format!("{p2c:?}"), "PowerOfTwoChoices");

        let default_fallback = LbAlgorithm::from_name("unknown_algorithm");
        assert_eq!(format!("{default_fallback:?}"), "RoundRobin");
    }

    #[test]
    fn test_lb_algorithm_selection() {
        let ep1 = Endpoint::new("ep1", "127.0.0.1:8081".parse::<SocketAddr>().unwrap(), 1);
        let ep2 = Endpoint::new("ep2", "127.0.0.1:8082".parse::<SocketAddr>().unwrap(), 1);
        let endpoints = vec![ep1, ep2];

        for name in &[
            "round_robin",
            "random",
            "ip_hash",
            "p2c",
            "least_connections",
        ] {
            let lb = LbAlgorithm::from_name(name);
            let selected = lb.select_index(&endpoints, &SelectionContext::NONE);
            assert!(selected.is_some());
            assert!(selected.unwrap() < endpoints.len());
        }
    }
}
