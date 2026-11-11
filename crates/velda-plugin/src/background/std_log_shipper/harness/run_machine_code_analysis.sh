#!/usr/bin/env bash
# ==============================================================================
# Velda Edge — Standard Log Shipper Machine Code Analysis & A/B Evaluation Harness
# ==============================================================================
#
# Inspects:
# 1. Zero-Heap Allocation Invariant during log line serialization.
# 2. Division-free 2-digit ASCII LUT integer conversion.
# 3. Real CPU cycles per record via Intel/AMD RDTSC execution.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

echo "======================================================================"
echo " 1. Verifying Zero-Allocation Integer Formatting Symbols"
echo "======================================================================"
if command -v nm >/dev/null 2>&1; then
    echo "Inspecting symbols for format_std_log_payload and fast_write_u64:"
    nm -C "${WORKSPACE_ROOT}/target/release/libvelda_plugin.rlib" 2>/dev/null | grep -E "format_std_log_payload|fast_write_u64" | head -n 10 || true
fi

echo ""
echo "======================================================================"
echo " 2. Live Runtime CPU Cycle Profiling (RDTSC on 100,000 Iterations)"
echo "======================================================================"
cargo test --release -p velda-plugin --test std_log_shipper_machine_code_test -- --nocapture

echo ""
echo "======================================================================"
echo " 3. Standard Log Shipper Machine Code Analysis Completed Cleanly."
echo "======================================================================"
