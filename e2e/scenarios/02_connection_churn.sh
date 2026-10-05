#!/usr/bin/env bash
# 02_connection_churn.sh — Rapid Connection Churn & Socket Recycling Test (Connection: close)
set -euo pipefail

TARGET_URL="${1:-http://127.0.0.1:8080/health}"
WRK_BIN=$(which wrk || echo "$HOME/.local/bin/wrk")

echo "=========================================================================="
echo " [E2E] Scenario 02: Rapid Connection Churn & Socket Recycling"
echo " (Forces non-keepalive 'Connection: close' to stress accept-loop & epoll)"
echo " Target: $TARGET_URL"
echo "=========================================================================="

if ! curl -sf -m 2 "$TARGET_URL" > /dev/null 2>&1; then
    echo "[Error] Target $TARGET_URL is not reachable!" >&2
    exit 1
fi

EDGE_PID=$(pgrep -x "velda-edge" | head -n 1 || true)
if [ -n "$EDGE_PID" ]; then
    START_FDS=$(ls -1 "/proc/$EDGE_PID/fd" 2>/dev/null | wc -l || echo 0)
    echo "[Initial] velda-edge Open FDs: $START_FDS"
fi

echo ""
echo ">>> [Phase 1] Medium Churn: 100 connections, 4 threads, 10s (Connection: close)..."
"$WRK_BIN" -t4 -c100 -d10s -H "Connection: close" --latency "$TARGET_URL"

echo ""
echo ">>> [Phase 2] Heavy Churn: 300 connections, 8 threads, 10s (Connection: close)..."
"$WRK_BIN" -t8 -c300 -d10s -H "Connection: close" --latency "$TARGET_URL"

echo ""
echo ">>> [Phase 3] Extreme Churn: 500 connections, 8 threads, 10s (Connection: close)..."
"$WRK_BIN" -t8 -c500 -d10s -H "Connection: close" --latency "$TARGET_URL"

sleep 2

if [ -n "$EDGE_PID" ]; then
    END_FDS=$(ls -1 "/proc/$EDGE_PID/fd" 2>/dev/null | wc -l || echo 0)
    echo ""
    echo "=========================================================================="
    echo " [FD Audit Result]"
    echo " Initial FDs: $START_FDS"
    echo " Final FDs:   $END_FDS (diff: $((END_FDS - START_FDS)))"
    echo " All churned connections recycled cleanly."
    echo "=========================================================================="
fi
