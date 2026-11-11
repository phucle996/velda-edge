//! Standard Stream Log Shipper Configuration (`std_log_shipper::config`).
//!
//! # Strict 100% Explicit Invariant
//! All configuration fields must be explicitly supplied in `plugins.json`. No implicit fallbacks,
//! hidden defaults, or magic values. Validation occurs strictly at config sync time.

use std::path::PathBuf;

pub use velda_sync::StdLogShipperConfig;

/// Output destination target for standard log records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStream {
    Stdout,
    Stderr,
    File(PathBuf),
}

impl TargetStream {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdout" => Self::Stdout,
            "stderr" => Self::Stderr,
            _ => Self::File(PathBuf::from(s.trim())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_target_stream_parse() {
        assert_eq!(TargetStream::parse("stdout"), TargetStream::Stdout);
        assert_eq!(TargetStream::parse("STDOUT"), TargetStream::Stdout);
        assert_eq!(TargetStream::parse("stderr"), TargetStream::Stderr);
        assert_eq!(TargetStream::parse("STDERR"), TargetStream::Stderr);
        assert_eq!(
            TargetStream::parse("/var/log/access.log"),
            TargetStream::File(PathBuf::from("/var/log/access.log"))
        );
    }
}
