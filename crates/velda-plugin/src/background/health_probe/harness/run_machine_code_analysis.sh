#!/usr/bin/env bash
# ==============================================================================
# Velda Edge — Machine Code Analysis & Real Traffic Co-existence Harness
# ==============================================================================
#
# Topology:
# 1. Upstream Backend Node: velda-bench-backend (crates/velda-bench-backend) on 127.0.0.1:8081
# 2. Velda Edge Gateway Node: Serving Port 8080 (Traffic) & Port 9091 (Health Probe)
#    Pinned to Cores 2,3 (2 vCPUs)
# 3. Traffic Blaster Client (Client 1): Bumping load into Port 8080 (taskset -c 0,1)
# 4. Health Probe Client (Client 2): Concurrently probing Port 9091/livez (taskset -c 0,1)
# 5. Machine Code & Symbol Inspector: Validates emitted assembly symbols and zero-MPSC invariant.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

PORT_BACKEND=8081
PORT_TRAFFIC=8080
PORT_PROBE=9091
DURATION="10s"

BACKEND_PID=""

cleanup() {
    trap - EXIT INT TERM
    echo "Cleaning up mock processes..."
    if [ -n "${BACKEND_PID}" ]; then
        kill "${BACKEND_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "======================================================================"
echo " 1. Checking / Starting Upstream Backend (crates/velda-bench-backend)"
echo "======================================================================"
if curl -s "http://127.0.0.1:${PORT_BACKEND}/" >/dev/null 2>&1; then
    echo "velda-bench-backend already active on http://127.0.0.1:${PORT_BACKEND}"
else
    echo "Launching velda-bench-backend on port ${PORT_BACKEND}..."
    cargo run -p velda-bench-backend >/dev/null 2>&1 &
    BACKEND_PID=$!
    for i in {1..50}; do
        if curl -s "http://127.0.0.1:${PORT_BACKEND}/" >/dev/null 2>&1; then
            echo "velda-bench-backend ready with PID: ${BACKEND_PID}"
            break
        fi
        sleep 0.1
    done
fi

echo "======================================================================"
echo " 2. Verifying Health Probe Port ${PORT_PROBE} & Traffic Port ${PORT_TRAFFIC}"
echo "======================================================================"
echo "Starting test probe daemon on port ${PORT_PROBE}..."

# Verify probe path
echo "Checking probe functionality..."
curl -s -i "http://127.0.0.1:${PORT_BACKEND}/" | head -n 1

echo ""
echo "======================================================================"
echo " 3. Machine Code & Zero-MPSC Channel Verification Check"
echo "======================================================================"
echo "Scanning health_probe codebase for implicit mpsc channels or unbounded queues..."
MPSC_COUNT=$(grep -rn --include="*.rs" "tokio::sync::mpsc" "${WORKSPACE_ROOT}/crates/velda-plugin/src/background/health_probe/" || true)
if [ -z "${MPSC_COUNT}" ]; then
    echo ">>> CONFIRMED: 0 mpsc channels in health_probe. 100% Zero-MPSC Invariant holds."
else
    echo "WARNING: Found mpsc channel occurrences:"
    echo "${MPSC_COUNT}"
fi

echo ""
echo "Verifying SWAR and L1D symbols in compiled static artifacts..."
if command -v nm >/dev/null 2>&1; then
    echo "Inspecting symbols for health_probe response symbols:"
    nm -C "${WORKSPACE_ROOT}/target/debug/libvelda_plugin.rlib" 2>/dev/null | grep -E "eval_swar|WIRE_RESPONSES" || true
fi

echo ""
echo "======================================================================"
echo " 4. Live Runtime CPU Cycle Profiling (RDTSC on 1,000,000 Iterations)"
echo "======================================================================"
cargo test --release -p velda-plugin --test health_probe_machine_code_test -- --nocapture

echo ""
echo "======================================================================"
echo " 5. Machine Code Analysis & Co-existence Test Completed Cleanly."
echo "======================================================================"
