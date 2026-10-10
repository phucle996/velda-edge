//! Resource saturation detection, memory usage sampling, and overload tracking.
//!
//! Owns memory usage probing (supporting cgroups v1 & v2 and Linux `/proc/self/statm`),
//! overload level state machine, and hysteresis watermarks to protect against OOMKilled
//! without creating thrashing / flapping oscillation.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

/// Canonical resource saturation classification levels.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OverloadLevel {
    /// Normal operation: RAM and CPU within safe bounds. All requests served normally.
    Normal = 0,
    /// Shedding mode: Approaching high watermark. Heavy streaming/uploads rejected with 429.
    Shedding = 1,
    /// Critical mode: Memory/CPU near exhaustion. Emergency load shedding and connection drain.
    Critical = 2,
}

impl OverloadLevel {
    /// Returns whether this level is normal.
    #[inline(always)]
    pub const fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    /// Returns whether this level requires shedding heavy traffic.
    #[inline(always)]
    pub const fn is_shedding(&self) -> bool {
        matches!(self, Self::Shedding | Self::Critical)
    }

    /// Returns whether this level is critical.
    #[inline(always)]
    pub const fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }

    /// Converts from primitive `u8` value.
    #[inline(always)]
    pub const fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::Normal,
            1 => Self::Shedding,
            _ => Self::Critical,
        }
    }

    /// Returns the string representation.
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Shedding => "shedding",
            Self::Critical => "critical",
        }
    }
}

/// Tunable watermark thresholds for overload detection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OverloadConfig {
    /// Ratio of memory limit to enter Shedding (default: 0.82 = 82% RAM).
    pub shedding_high_watermark: f64,
    /// Ratio of memory limit to exit Shedding (default: 0.75 = 75% RAM).
    pub shedding_low_watermark: f64,
    /// Ratio of memory limit to enter Critical (default: 0.90 = 90% RAM).
    pub critical_high_watermark: f64,
    /// Ratio of memory limit to exit Critical (default: 0.82 = 82% RAM).
    pub critical_low_watermark: f64,
}

impl Default for OverloadConfig {
    fn default() -> Self {
        Self {
            shedding_high_watermark: 0.82,
            shedding_low_watermark: 0.75,
            critical_high_watermark: 0.90,
            critical_low_watermark: 0.82,
        }
    }
}

/// Thread-safe tracker maintaining the active resource overload level with hysteresis.
#[derive(Debug)]
pub struct OverloadTracker {
    config: OverloadConfig,
    level: AtomicU8,
    total_memory_bytes: usize,
    current_memory_bytes: AtomicUsize,
}

impl Default for OverloadTracker {
    fn default() -> Self {
        Self::auto()
    }
}

impl OverloadTracker {
    /// Creates a new [`OverloadTracker`] with explicit config and total memory capacity.
    pub fn new(config: OverloadConfig, total_memory_bytes: usize) -> Self {
        Self {
            config,
            level: AtomicU8::new(OverloadLevel::Normal as u8),
            total_memory_bytes: total_memory_bytes.max(1),
            current_memory_bytes: AtomicUsize::new(0),
        }
    }

    /// Probes host topology and constructs an [`OverloadTracker`] scaled for active hardware.
    pub fn auto() -> Self {
        let total = crate::hardware::global_hardware_topology().memory_bytes;
        Self::new(OverloadConfig::default(), total)
    }

    /// Returns the current overload level (reads atomic with Relaxed ordering, ~1ns).
    #[inline(always)]
    pub fn level(&self) -> OverloadLevel {
        OverloadLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    /// Returns whether active overload state is shedding or higher.
    #[inline(always)]
    pub fn is_shedding(&self) -> bool {
        self.level().is_shedding()
    }

    /// Returns whether active overload state is critical.
    #[inline(always)]
    pub fn is_critical(&self) -> bool {
        self.level().is_critical()
    }

    /// Returns the last sampled memory usage in bytes.
    #[inline(always)]
    pub fn current_memory_bytes(&self) -> usize {
        self.current_memory_bytes.load(Ordering::Relaxed)
    }

    /// Returns the configured total memory limit in bytes.
    #[inline(always)]
    pub fn total_memory_bytes(&self) -> usize {
        self.total_memory_bytes
    }

    /// Updates tracker state given an observed memory usage in bytes.
    ///
    /// Implements hysteresis state machine to prevent flapping between states.
    pub fn update_with_usage(&self, current_bytes: usize) -> OverloadLevel {
        self.current_memory_bytes
            .store(current_bytes, Ordering::Relaxed);
        let ratio = current_bytes as f64 / self.total_memory_bytes as f64;
        let current_lvl = self.level();

        let new_lvl = match current_lvl {
            OverloadLevel::Normal => {
                if ratio >= self.config.critical_high_watermark {
                    OverloadLevel::Critical
                } else if ratio >= self.config.shedding_high_watermark {
                    OverloadLevel::Shedding
                } else {
                    OverloadLevel::Normal
                }
            }
            OverloadLevel::Shedding => {
                if ratio >= self.config.critical_high_watermark {
                    OverloadLevel::Critical
                } else if ratio <= self.config.shedding_low_watermark {
                    OverloadLevel::Normal
                } else {
                    OverloadLevel::Shedding
                }
            }
            OverloadLevel::Critical => {
                if ratio <= self.config.shedding_low_watermark {
                    OverloadLevel::Normal
                } else if ratio <= self.config.critical_low_watermark {
                    OverloadLevel::Shedding
                } else {
                    OverloadLevel::Critical
                }
            }
        };

        if new_lvl != current_lvl {
            self.level.store(new_lvl as u8, Ordering::Release);
        }

        new_lvl
    }

    /// Probes current process memory usage and updates internal state.
    pub fn sample_and_update(&self) -> OverloadLevel {
        let usage = probe_memory_usage();
        self.update_with_usage(usage)
    }
}

/// Probes the active memory usage of the process or container in bytes.
///
/// Priority ladder:
/// 1. Container cgroup v2: `/sys/fs/cgroup/memory.current`
/// 2. Container cgroup v1: `/sys/fs/cgroup/memory/memory.usage_in_bytes`
/// 3. Linux proc statm: `/proc/self/statm` (Resident Set Size pages * 4096)
/// 4. Fallback: 0 bytes.
pub fn probe_memory_usage() -> usize {
    // 1. Container cgroup v2: /sys/fs/cgroup/memory.current
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        && let Ok(bytes) = content.trim().parse::<usize>()
    {
        return bytes;
    }

    // 2. Container cgroup v1: /sys/fs/cgroup/memory/memory.usage_in_bytes
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        && let Ok(bytes) = content.trim().parse::<usize>()
    {
        return bytes;
    }

    // 3. Linux /proc/self/statm (2nd column is resident set size in pages)
    if let Ok(content) = std::fs::read_to_string("/proc/self/statm") {
        let mut parts = content.split_whitespace();
        let _size_pages = parts.next();
        if let Some(resident_pages_str) = parts.next()
            && let Ok(resident_pages) = resident_pages_str.parse::<usize>()
        {
            // Typical page size on Linux is 4096 bytes (4 KB)
            return resident_pages.saturating_mul(4096);
        }
    }

    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overload_hysteresis_transitions() {
        let config = OverloadConfig {
            shedding_high_watermark: 0.80,
            shedding_low_watermark: 0.70,
            critical_high_watermark: 0.90,
            critical_low_watermark: 0.85,
        };
        let total = 1_000_000_000; // 1 GB
        let tracker = OverloadTracker::new(config, total);

        // 1. Start at Normal (500MB = 50%)
        assert_eq!(
            tracker.update_with_usage(500_000_000),
            OverloadLevel::Normal
        );
        assert!(!tracker.is_shedding());

        // 2. Reach 820MB = 82% -> Enters Shedding
        assert_eq!(
            tracker.update_with_usage(820_000_000),
            OverloadLevel::Shedding
        );
        assert!(tracker.is_shedding());
        assert!(!tracker.is_critical());

        // 3. Drop to 750MB = 75% -> Must STAY in Shedding due to Hysteresis (low mark is 70%)
        assert_eq!(
            tracker.update_with_usage(750_000_000),
            OverloadLevel::Shedding
        );

        // 4. Drop to 680MB = 68% -> Falls below low mark (70%) -> Exits to Normal
        assert_eq!(
            tracker.update_with_usage(680_000_000),
            OverloadLevel::Normal
        );

        // 5. Jump directly to 920MB = 92% -> Enters Critical
        assert_eq!(
            tracker.update_with_usage(920_000_000),
            OverloadLevel::Critical
        );
        assert!(tracker.is_critical());

        // 6. Drop to 870MB = 87% -> Must STAY in Critical (low mark is 85%)
        assert_eq!(
            tracker.update_with_usage(870_000_000),
            OverloadLevel::Critical
        );

        // 7. Drop to 840MB = 84% -> Drops below 85% -> De-escalates to Shedding
        assert_eq!(
            tracker.update_with_usage(840_000_000),
            OverloadLevel::Shedding
        );

        // 8. Drop to 600MB = 60% -> Drops below 70% -> De-escalates to Normal
        assert_eq!(
            tracker.update_with_usage(600_000_000),
            OverloadLevel::Normal
        );
    }
}
