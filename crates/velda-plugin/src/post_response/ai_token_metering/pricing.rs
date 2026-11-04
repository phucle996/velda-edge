//! Micro-USD Fixed-Point Pricing Calculation Engine.
//!
//! # Design & Financial Invariants
//! - **Zero Floating-Point**: All rates and computed costs are represented as integer
//!   micro-USD ($10^{-6}$, where 1,000,000 micro-USD = $1.00) to eliminate IEEE-754 precision drift.
//! - **Overflow-Free Arithmetic**: Intermediates are cast to `u128` before division:
//!   `cost = (tokens as u128 * rate_per_million as u128) / 1_000_000`.
//! - **Tiered Cache Accounting**: Separates uncached prompt, cached prompt (prompt cache discount),
//!   and completion tokens into independent cost components before final aggregation.

use serde::{Deserialize, Serialize};

/// Pricing rates for an LLM model in micro-USD per 1,000,000 tokens ($10^{-6}$).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPricing {
    /// Cost for 1,000,000 uncached prompt tokens in micro-USD (e.g. $2.50 = 2_500_000).
    pub prompt_micro_usd_per_million: u64,
    /// Cost for 1,000,000 completion tokens in micro-USD (e.g. $10.00 = 10_000_000).
    pub completion_micro_usd_per_million: u64,
    /// Cost for 1,000,000 cached prompt tokens in micro-USD (e.g. $1.25 = 1_250_000).
    pub cached_prompt_micro_usd_per_million: u64,
}

impl ModelPricing {
    /// Computes total cost in micro-USD for the given token usage.
    #[inline]
    pub fn compute_cost_micro_usd(
        &self,
        prompt_tokens: u64,
        completion_tokens: u64,
        cached_tokens: u64,
    ) -> u64 {
        let effective_cached = cached_tokens.min(prompt_tokens);
        let uncached_prompt = prompt_tokens.saturating_sub(effective_cached);

        let uncached_cost =
            (uncached_prompt as u128 * self.prompt_micro_usd_per_million as u128) / 1_000_000;
        let cached_cost = (effective_cached as u128
            * self.cached_prompt_micro_usd_per_million as u128)
            / 1_000_000;
        let comp_cost =
            (completion_tokens as u128 * self.completion_micro_usd_per_million as u128) / 1_000_000;

        (uncached_cost + cached_cost + comp_cost) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_cost_deepseek() {
        // deepseek-chat: $0.14 prompt, $0.014 cached, $0.28 completion
        let pricing = ModelPricing {
            prompt_micro_usd_per_million: 140_000,
            cached_prompt_micro_usd_per_million: 14_000,
            completion_micro_usd_per_million: 280_000,
        };

        // 1540 prompt (1024 cached, 516 uncached), 320 completion
        // uncached: 516 * 140_000 / 1_000_000 = 72
        // cached: 1024 * 14_000 / 1_000_000 = 14
        // comp: 320 * 280_000 / 1_000_000 = 89
        // total: 72 + 14 + 89 = 175 micro-USD
        let cost = pricing.compute_cost_micro_usd(1540, 320, 1024);
        assert_eq!(cost, 175);
    }

    #[test]
    fn test_compute_cost_gpt4o() {
        // gpt-4o: $2.50 prompt, $1.25 cached, $10.00 completion
        let pricing = ModelPricing {
            prompt_micro_usd_per_million: 2_500_000,
            cached_prompt_micro_usd_per_million: 1_250_000,
            completion_micro_usd_per_million: 10_000_000,
        };

        let cost = pricing.compute_cost_micro_usd(1_000_000, 1_000_000, 500_000);
        // uncached: 500k * 2.50 = $1.25 (1_250_000)
        // cached: 500k * 1.25 = $0.625 (625_000)
        // comp: 1M * 10.00 = $10.00 (10_000_000)
        // total: 11_875_000 micro-USD ($11.875)
        assert_eq!(cost, 11_875_000);
    }
}
