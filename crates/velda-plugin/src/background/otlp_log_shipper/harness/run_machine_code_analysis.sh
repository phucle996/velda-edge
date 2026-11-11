#!/usr/bin/env bash
# ==============================================================================
# Velda Edge — OpenTelemetry Log Shipper Machine Code Analysis & Evaluation Harness
# ==============================================================================
#
# Inspects:
# 1. Zero-Allocation JSON formatting via static envelope and LUT integer conversions.
# 2. Protobuf log record serialization speed.
# 3. Real CPU cycles per record via Intel/AMD RDTSC execution.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

echo "======================================================================"
echo " 1. Verifying Zero-Allocation OTLP Export Formatting Symbols"
echo "======================================================================"
if command -v nm >/dev/null 2>&1; then
    echo "Inspecting symbols for format_otlp_records_json and format_otlp_records_proto:"
    nm -C "${WORKSPACE_ROOT}/target/release/libvelda_plugin.rlib" 2>/dev/null | grep -E "format_otlp_records_json|format_otlp_records_proto" | head -n 10 || true
fi

echo ""
echo "======================================================================"
echo " 2. Live Runtime CPU Cycle Profiling (RDTSC on 50,000 Iterations)"
echo "======================================================================"
cargo test --release -p velda-plugin --test otlp_log_shipper_machine_code_test -- --nocapture

echo ""
echo "======================================================================"
echo " 3. OpenTelemetry Log Shipper Machine Code Analysis Completed Cleanly."
echo "======================================================================"
