#!/usr/bin/env bash
# live_tracker.sh — Real-time resource and telemetry monitor for Velda Edge
set -euo pipefail

TARGET_NAME="${1:-velda-edge}"
PID=$(pgrep -x "$TARGET_NAME" | head -n 1 || true)

if [ -z "$PID" ]; then
    echo "[Monitor] Error: Process '$TARGET_NAME' not found!" >&2
    exit 1
fi

CSV_OUT="${2:-}"

echo "=========================================================================================="
echo " Velda Edge Live Telemetry Tracker"
echo " Target: $TARGET_NAME (PID: $PID)"
if [ -n "$CSV_OUT" ]; then
    echo " Output CSV: $CSV_OUT"
    echo "timestamp,rss_mb,open_fds,threads,tcp_estab,tcp_tw" > "$CSV_OUT"
fi
echo "=========================================================================================="
printf "%-10s | %-10s | %-8s | %-8s | %-10s | %-10s\n" "TIME" "RSS (MB)" "OPEN FDS" "THREADS" "TCP ESTAB" "TIME_WAIT"
echo "------------------------------------------------------------------------------------------"

PAGE_SIZE=$(getconf PAGESIZE 2>/dev/null || echo 4096)

while kill -0 "$PID" 2>/dev/null; do
    TIME_STR=$(date +%H:%M:%S)
    
    # 1. Memory RSS in MB
    if [ -f "/proc/$PID/statm" ]; then
        PAGES=$(awk '{print $2}' "/proc/$PID/statm" 2>/dev/null || echo 0)
        RSS_MB=$(awk "BEGIN {printf \"%.2f\", ($PAGES * $PAGE_SIZE) / 1048576}")
    else
        RSS_MB="0"
    fi

    # 2. Open File Descriptors count
    if [ -d "/proc/$PID/fd" ]; then
        FDS=$(ls -1 "/proc/$PID/fd" 2>/dev/null | wc -l || echo 0)
    else
        FDS="0"
    fi

    # 3. Threads count
    if [ -f "/proc/$PID/status" ]; then
        THREADS=$(grep -i "Threads:" "/proc/$PID/status" | awk '{print $2}' || echo 0)
    else
        THREADS="0"
    fi

    # 4. Active TCP connections for this PID
    TCP_ESTAB=$(ss -t state established 2>/dev/null | grep -c ":8080\|:8443\|:50051\|:9000" || true)
    TCP_TW=$(ss -t state time-wait 2>/dev/null | grep -c ":8080\|:8443\|:50051\|:9000" || true)

    printf "%-10s | %-10s | %-8s | %-8s | %-10s | %-10s\n" \
        "$TIME_STR" "${RSS_MB} MB" "$FDS" "$THREADS" "$TCP_ESTAB" "$TCP_TW"

    if [ -n "$CSV_OUT" ]; then
        echo "$(date +%s),$RSS_MB,$FDS,$THREADS,$TCP_ESTAB,$TCP_TW" >> "$CSV_OUT"
    fi

    sleep 1
done

echo "[Monitor] Process $TARGET_NAME (PID: $PID) exited."
