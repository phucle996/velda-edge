#!/usr/bin/env bash
# 01_http1_saturation.sh — HTTP/1.1 High-Concurrency Saturation Test
set -euo pipefail

TARGET_URL="${1:-http://127.0.0.1:8080/echo}"
WRK_BIN=$(which wrk || echo "$HOME/.local/bin/wrk")

if [ ! -x "$WRK_BIN" ]; then
    echo "[Error] wrk not found at $WRK_BIN" >&2
    exit 1
fi

echo "=========================================================================="
echo " [E2E] Scenario 01: HTTP/1.1 Saturation & Throughput Test"
echo " Target: $TARGET_URL"
echo " wrk binary: $WRK_BIN"
echo "=========================================================================="

# Check if target is responding
if ! curl -sf -m 2 "$TARGET_URL" > /dev/null 2>&1; then
    echo "[Error] Target $TARGET_URL is not reachable!" >&2
    echo "Make sure velda-edge and e2e backend are running." >&2
    exit 1
fi

echo ""
echo ">>> [Phase 1] Warm-up: 50 connections, 4 threads, 5s..."
"$WRK_BIN" -t4 -c50 -d5s --latency "$TARGET_URL"

echo ""
echo ">>> [Phase 2] High Load: 200 connections, 8 threads, 10s..."
"$WRK_BIN" -t8 -c200 -d10s --latency "$TARGET_URL"

echo ""
echo ">>> [Phase 3] Peak Saturation: 500 connections, 8 threads, 10s..."
"$WRK_BIN" -t8 -c500 -d10s --latency "$TARGET_URL"

echo ""
echo ">>> [Phase 4] Extreme Concurrency: 1,000 connections, 12 threads, 10s..."
"$WRK_BIN" -t12 -c1000 -d10s --latency "$TARGET_URL"

echo ""
echo "=========================================================================="
echo " [E2E] Saturation test completed."
echo "=========================================================================="
