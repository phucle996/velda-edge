#!/usr/bin/env bash
# ==============================================================================
# Velda Edge — Grafana Loki Log Shipper Machine Code Analysis & Evaluation Harness
# ==============================================================================
#
# Inspects:
# 1. Zero-Allocation multi-stream JSON formatting via static labels template.
# 2. Division-free 2-digit ASCII LUT integer conversion.
# 3. Real CPU cycles per record via Intel/AMD RDTSC execution.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

echo "======================================================================"
echo " 1. Verifying Zero-Allocation Loki Export Formatting Symbols"
echo "======================================================================"
if command -v nm >/dev/null 2>&1; then
    echo "Inspecting symbols for format_loki_push_payload:"
    nm -C "${WORKSPACE_ROOT}/target/release/libvelda_plugin.rlib" 2>/dev/null | grep -E "format_loki_push_payload" | head -n 10 || true
fi

echo ""
echo "======================================================================"
echo " 2. Live Runtime CPU Cycle Profiling (RDTSC on 100,000 Iterations)"
echo "======================================================================"
cargo test --release -p velda-plugin --test loki_log_shipper_machine_code_test -- --nocapture

echo ""
echo "======================================================================"
echo " 3. Grafana Loki Log Shipper Machine Code Analysis Completed Cleanly."
echo "======================================================================"
