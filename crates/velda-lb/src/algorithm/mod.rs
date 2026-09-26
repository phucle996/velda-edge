//! Load balancing algorithms implementations.

pub mod hash;
pub mod least_conn;
pub mod maglev;
pub mod p2c;
pub mod peak_ewma;
pub mod random;
pub mod ring_hash;
pub mod round_robin;
pub mod weighted_round_robin;

pub use hash::{GenericHash, IpHash, fnv1a_hash};
pub use least_conn::{LeastConnections, LeastRequests, WeightedLeastRequests};
pub use maglev::{MAGLEV_TABLE_SIZE, Maglev};
pub use p2c::PowerOfTwoChoices;
pub use peak_ewma::PeakEwma;
pub use random::{Random, WeightedRandom};
pub use ring_hash::RingHash;
pub use round_robin::RoundRobin;
pub use weighted_round_robin::WeightedRoundRobin;
