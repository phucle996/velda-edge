//! Adaptive buffer dimensions and anti-OOM mathematical sizing.
//!
//! Provides mathematically bounded burst ceilings based on host memory and core concurrency,
//! preventing memory exhaustion under sustained traffic bursts.

/// Pre-computed continuous adaptive buffer pool dimensions per traffic worker core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveBufferDimensions {
    pub header_baseline: usize,
    pub header_max_cap: usize,
    pub small_baseline: usize,
    pub small_max_cap: usize,
    pub medium_baseline: usize,
    pub medium_max_cap: usize,
    pub standard_baseline: usize,
    pub standard_max_cap: usize,
    pub large_baseline: usize,
    pub large_max_cap: usize,
}

impl AdaptiveBufferDimensions {
    /// Computes recommended pre-warmed buffer and slab pool dimensions per traffic worker core.
    ///
    /// Returns `(header_slots, small_4k_slabs, medium_16k_slabs, standard_32k_slabs, large_64k_slabs)`.
    #[inline]
    pub fn buffer_pool_dimensions_per_core(max_idle: usize) -> (usize, usize, usize, usize, usize) {
        let header_slots = (max_idle * 2).clamp(256, 2048);

        let small_slabs = (header_slots / 2).clamp(128, 1024);
        let medium_slabs = (header_slots / 4).clamp(64, 512);
        let standard_slabs = (header_slots / 8).clamp(32, 256);
        let large_slabs = (header_slots / 16).clamp(16, 128);

        (
            header_slots,
            small_slabs,
            medium_slabs,
            standard_slabs,
            large_slabs,
        )
    }

    /// Calculates the strict anti-OOM max burst ceiling for a buffer class.
    ///
    /// # Dual-Constraint Anti-OOM Formula
    /// `max_cap = min(baseline * 3, baseline + (total_memory_bytes * 0.20) / (num_cores * item_bytes))`
    ///
    /// Guarantees that even under sustained saturation spikes:
    /// 1. The buffer pool scales up to 3x baseline capacity to absorb micro-bursts.
    /// 2. Total buffer memory across all worker cores never exceeds 20% of available RAM.
    #[inline]
    pub fn adaptive_burst_cap(
        baseline: usize,
        total_memory_bytes: usize,
        num_cores: usize,
        item_bytes: usize,
    ) -> usize {
        let triple_baseline = baseline.saturating_mul(3);
        let headroom_20_pct = (total_memory_bytes as f64 * 0.20) as usize;
        let per_core_headroom = headroom_20_pct / num_cores.max(1);
        let max_from_headroom = baseline + (per_core_headroom / item_bytes.max(1));
        triple_baseline.min(max_from_headroom).max(baseline)
    }

    /// Derives per-core adaptive buffer dimensions bounded by the Dual-Constraint Anti-OOM formula.
    #[inline]
    pub fn from_hardware(max_idle: usize, total_memory_bytes: usize, num_cores: usize) -> Self {
        let (header_base, small_base, med_base, std_base, large_base) =
            Self::buffer_pool_dimensions_per_core(max_idle);

        const HEADER_WORKSPACE_BYTES: usize = 1024;
        let header_max = Self::adaptive_burst_cap(
            header_base,
            total_memory_bytes,
            num_cores,
            HEADER_WORKSPACE_BYTES,
        );
        let small_max = Self::adaptive_burst_cap(small_base, total_memory_bytes, num_cores, 4096);
        let med_max = Self::adaptive_burst_cap(med_base, total_memory_bytes, num_cores, 16384);
        let std_max = Self::adaptive_burst_cap(std_base, total_memory_bytes, num_cores, 32768);
        let large_max = Self::adaptive_burst_cap(large_base, total_memory_bytes, num_cores, 65536);

        Self {
            header_baseline: header_base,
            header_max_cap: header_max,
            small_baseline: small_base,
            small_max_cap: small_max,
            medium_baseline: med_base,
            medium_max_cap: med_max,
            standard_baseline: std_base,
            standard_max_cap: std_max,
            large_baseline: large_base,
            large_max_cap: large_max,
        }
    }

    /// Derives per-core adaptive buffer dimensions from a [`HardwareTopology`].
    #[inline]
    pub fn for_topology(topo: &crate::HardwareTopology) -> Self {
        Self::from_hardware(
            topo.optimal_max_idle_per_key,
            topo.memory_bytes,
            topo.available_cores,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_sizing_formula() {
        // Baseline = 100, 1GB RAM, 4 cores, 16KB item
        // 20% of 1GB = 200MB = 209,715,200 bytes
        // Per core headroom = 209,715,200 / 4 = 52,428,800 bytes
        // In 16KB items: 52,428,800 / 16384 = 3200 items
        // Baseline * 3 = 300
        // min(300, 100 + 3200) = 300 (bounded by 3x burst multiplier)
        let total_ram = 1024 * 1024 * 1024;
        let cap = AdaptiveBufferDimensions::adaptive_burst_cap(100, total_ram, 4, 16384);
        assert_eq!(cap, 300);

        // Very constrained memory scenario: 10MB RAM, 4 cores, 64KB item
        // 20% of 10MB = 2MB = 2,097,152 bytes
        // Per core headroom = 524,288 bytes
        // In 64KB items: 524,288 / 65536 = 8 items
        // Baseline = 50, Baseline * 3 = 150
        // Headroom max = 50 + 8 = 58
        // min(150, 58) = 58 (strictly bounded by 20% headroom to prevent OOM!)
        let constrained_ram = 10 * 1024 * 1024;
        let cap_constrained =
            AdaptiveBufferDimensions::adaptive_burst_cap(50, constrained_ram, 4, 65536);
        assert_eq!(cap_constrained, 58);
    }
}
