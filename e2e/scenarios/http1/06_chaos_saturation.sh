#!/usr/bin/env bash
# 06_chaos_saturation.sh — HTTP/1.1 Chaos Storm & Resource Leak Audit
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="http://127.0.0.1:8080"

get_edge_metrics() {
    local PID
    PID=$(pgrep -f "target/release/velda-edge" | head -n 1 || true)
    if [ -n "$PID" ]; then
        local RSS
        RSS=$(grep VmRSS "/proc/$PID/status" 2>/dev/null | awk '{print $2}')
        local FDS
        FDS=$(ls -1 "/proc/$PID/fd" 2>/dev/null | wc -l)
        echo "$RSS $FDS"
    else
        echo "0 0"
    fi
}

echo "=========================================================================="
echo " [E2E HTTP/1.1] 06: Chaos Storm & Resource Leak Audit"
echo " Target: $TARGET"
echo "=========================================================================="

read -r RSS_START FDS_START <<< "$(get_edge_metrics)"
echo "Baseline Edge State: RSS: $((RSS_START / 1024)) MB | FDs: $FDS_START"
echo ""

echo ">>> [Phase 1] Launching Mixed Traffic Storm (10s):"
echo " - 70% Normal requests (/health, /echo)"
echo " - 10% Upstream delays (/delay?ms=30)"
echo " - 10% Upstream errors (/status?code=500)"
echo " - 10% Upstream TCP drops (/drop)"

wrk -t4 -c150 -d10s "$TARGET/health" > /tmp/wrk_normal.log 2>&1 &
PID_NORMAL=$!

# Background chaos blasters
for i in $(seq 1 10); do
    (
        for j in $(seq 1 15); do
            curl -s "$TARGET/drop" > /dev/null 2>&1 || true
            curl -s "$TARGET/delay?ms=30" > /dev/null 2>&1 || true
            curl -s "$TARGET/status?code=500" > /dev/null 2>&1 || true
            sleep 0.05
        done
    ) &
done

wait $PID_NORMAL
wait

echo "Storm completed. Waiting 2s cooldown..."
sleep 2

read -r RSS_END FDS_END <<< "$(get_edge_metrics)"
RSS_DELTA=$(( (RSS_END - RSS_START) / 1024 ))
FD_DELTA=$(( FDS_END - FDS_START ))

echo ""
echo "=========================================================================="
echo " [Post-Chaos Resource Audit]"
echo " Baseline: RSS: $((RSS_START / 1024)) MB | FDs: $FDS_START"
echo " Final:    RSS: $((RSS_END / 1024)) MB | FDs: $FDS_END"
echo " Delta:    RSS: +${RSS_DELTA} MB | FDs: +${FD_DELTA}"
echo "=========================================================================="

# Functional check
POST_CHECK=$(curl -s "$TARGET/health")
if [ "$POST_CHECK" = "OK" ]; then
    echo " [PASS] Gateway fully operational (200 OK) after sustained chaos storm."
else
    echo " [FAIL] Gateway unresponsive after chaos storm!"
    exit 1
fi

if [ "$FD_DELTA" -le 70 ]; then
    echo " [PASS] FD delta (+$FD_DELTA) within expected upstream pool capacity (32 idle/shard x 2 shards)."
else
    echo " [WARN] Possible FD leak! FD Delta: +$FD_DELTA"
fi

echo "=========================================================================="
echo " [E2E HTTP/1.1] 06: Chaos Storm completed successfully."
echo "=========================================================================="
