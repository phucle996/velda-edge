#!/usr/bin/env bash
# 04_soak_test.sh — Sustained High-Load Soak Test & Resource Leak Verification
set -euo pipefail

DURATION="${1:-60s}"
TARGET_URL="${2:-http://127.0.0.1:8080/echo}"
WRK_BIN=$(which wrk || echo "$HOME/.local/bin/wrk")

echo "=========================================================================="
echo " [E2E] Scenario 04: Sustained Soak Test & Leak Detection"
echo " Duration: $DURATION"
echo " Target:   $TARGET_URL"
echo "=========================================================================="

if ! curl -sf -m 2 "$TARGET_URL" > /dev/null 2>&1; then
    echo "[Error] Target $TARGET_URL is not reachable!" >&2
    exit 1
fi

EDGE_PID=$(pgrep -x "velda-edge" | head -n 1 || true)
if [ -z "$EDGE_PID" ]; then
    echo "[Error] velda-edge process not running!" >&2
    exit 1
fi

CSV_LOG="/tmp/velda_soak_stats_$(date +%s).csv"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACKER="$SCRIPT_DIR/../monitor/live_tracker.sh"

echo ">>> Starting live telemetry logger to $CSV_LOG..."
"$TRACKER" velda-edge "$CSV_LOG" > /dev/null 2>&1 &
TRACKER_PID=$!

echo ">>> Launching sustained wrk load ($DURATION, 200 connections, 8 threads)..."
"$WRK_BIN" -t8 -c200 -d"$DURATION" --latency "$TARGET_URL"

echo ">>> Soak load finished. Waiting 5s for connection draining..."
sleep 5

# Stop telemetry tracker
kill "$TRACKER_PID" 2>/dev/null || true

echo ""
echo "=========================================================================="
echo " [Soak Test Resource Analysis]"
echo "=========================================================================="

if [ -f "$CSV_LOG" ]; then
    START_RSS=$(head -n 2 "$CSV_LOG" | tail -n 1 | cut -d',' -f2 || echo 0)
    END_RSS=$(tail -n 1 "$CSV_LOG" | cut -d',' -f2 || echo 0)
    START_FDS=$(head -n 2 "$CSV_LOG" | tail -n 1 | cut -d',' -f3 || echo 0)
    END_FDS=$(tail -n 1 "$CSV_LOG" | cut -d',' -f3 || echo 0)
    
    echo "Initial RSS Memory: $START_RSS MB"
    echo "Final RSS Memory:   $END_RSS MB"
    echo "Initial FDs:        $START_FDS"
    echo "Final FDs:          $END_FDS"
    echo ""
    echo "CSV Telemetry Log:  $CSV_LOG"
fi
echo "=========================================================================="
