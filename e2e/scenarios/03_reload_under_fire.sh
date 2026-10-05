#!/usr/bin/env bash
# 03_reload_under_fire.sh — Atomic Config Reload Under High Traffic
set -euo pipefail

TARGET_URL="${1:-http://127.0.0.1:8080/echo}"
WRK_BIN=$(which wrk || echo "$HOME/.local/bin/wrk")
STORAGE_DIR="${2:-/home/phucle/Desktop/velda-edge/storage}"

echo "=========================================================================="
echo " [E2E] Scenario 03: Atomic Hot-Reload Under Heavy Fire"
echo " (Floods traffic while continuously triggering config reloads)"
echo " Target: $TARGET_URL"
echo "=========================================================================="

if ! curl -sf -m 2 "$TARGET_URL" > /dev/null 2>&1; then
    echo "[Error] Target $TARGET_URL is not reachable!" >&2
    exit 1
fi

LOG_OUTPUT=$(mktemp)

echo ">>> Launching wrk background flood (200 connections, 8 threads, 25 seconds)..."
"$WRK_BIN" -t8 -c200 -d25s --latency "$TARGET_URL" > "$LOG_OUTPUT" &
WRK_PID=$!

echo ">>> Traffic is flowing (wrk PID: $WRK_PID)."
echo ">>> Triggering 5 consecutive config sync & atomic reload cycles every 3 seconds..."

ROUTES_FILE="$STORAGE_DIR/config/routes.json"

for i in {1..5}; do
    sleep 3
    echo "[Cycle $i/5] Triggering reload notification..."
    if [ -f "$ROUTES_FILE" ]; then
        # Touch file to update mtime and trigger reconciler
        touch "$ROUTES_FILE"
    fi
    echo "[Cycle $i/5] Timestamp: $(date +%H:%M:%S)"
done

echo ">>> Waiting for wrk flood to complete..."
wait "$WRK_PID"

echo ""
echo "=========================================================================="
echo " [wrk Benchmark Report Under Continuous Atomic Reloads]"
echo "=========================================================================="
cat "$LOG_OUTPUT"
rm -f "$LOG_OUTPUT"

echo ""
echo ">>> Checking journal logs for dropped requests or errors..."
journalctl --user -u velda-edge.service -n 10 --no-pager
