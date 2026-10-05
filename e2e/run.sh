#!/usr/bin/env bash
# run.sh — Master CLI runner for Velda Edge E2E & High-Load Test Suite
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_PID_FILE="/tmp/velda_e2e_backend.pid"

usage() {
    cat << EOF
Velda Edge — E2E & High-Load Stress Test Suite

USAGE:
    ./e2e/run.sh <command> [arguments]

COMMANDS:
    start-backend           Build and start Go upstream test server (:8081, :19001)
    stop-backend            Stop running Go upstream test server
    status                  Check status of Edge and E2E Backend
    monitor                 Run real-time telemetry tracker in foreground
    test saturation         Run HTTP/1.1 concurrency & throughput saturation test
    test churn              Run rapid connection churn & socket recycling test
    test reload             Run atomic hot-reload test under heavy traffic flood
    test soak [duration]    Run sustained soak load test (default: 60s, e.g. 5m, 1h)
    test all                Run all test scenarios sequentially
EOF
    exit 1
}

start_backend() {
    echo "[E2E] Building latest Go upstream test server..."
    mkdir -p "$SCRIPT_DIR/../target/release"
    go build -o "$SCRIPT_DIR/../target/release/velda-e2e-backend" "$SCRIPT_DIR/backend/main.go"

    echo "[E2E] Starting velda-e2e-backend via systemd..."
    systemctl --user start velda-e2e-backend.service
    sleep 1

    if systemctl --user is-active --quiet velda-e2e-backend.service; then
        PID=$(systemctl --user show -p MainPID --value velda-e2e-backend.service)
        echo "[E2E] Upstream backend successfully running (PID: $PID, Ports: 8081, 19001)."
    else
        echo "[E2E] Error: Failed to start velda-e2e-backend.service. Journal:" >&2
        journalctl --user -u velda-e2e-backend.service -n 10 --no-pager >&2
        exit 1
    fi
}

stop_backend() {
    echo "[E2E] Stopping velda-e2e-backend.service..."
    systemctl --user stop velda-e2e-backend.service || true
    echo "[E2E] Upstream backend stopped."
}

check_status() {
    echo "============================================================"
    echo " Velda Edge E2E Environment Status"
    echo "============================================================"
    
    # 1. Check velda-edge service
    if systemctl --user is-active --quiet velda-edge.service; then
        EDGE_PID=$(systemctl --user show -p MainPID --value velda-edge.service)
        echo " [+] velda-edge:    ACTIVE (PID: $EDGE_PID)"
    else
        echo " [-] velda-edge:    INACTIVE"
    fi

    # 2. Check velda-sync service
    if systemctl --user is-active --quiet velda-sync.service; then
        SYNC_PID=$(systemctl --user show -p MainPID --value velda-sync.service)
        echo " [+] velda-sync:    ACTIVE (PID: $SYNC_PID)"
    else
        echo " [-] velda-sync:    INACTIVE"
    fi

    # 3. Check E2E Backend
    if systemctl --user is-active --quiet velda-e2e-backend.service; then
        BACKEND_PID=$(systemctl --user show -p MainPID --value velda-e2e-backend.service)
        echo " [+] e2e-backend:   ACTIVE (PID: $BACKEND_PID, Ports: 8081, 19001)"
    else
        echo " [-] e2e-backend:   STOPPED"
    fi

    # 4. Check wrk tool
    WRK_PATH=$(which wrk || echo "$HOME/.local/bin/wrk")
    if [ -x "$WRK_PATH" ]; then
        echo " [+] wrk:           INSTALLED ($WRK_PATH)"
    else
        echo " [-] wrk:           MISSING"
    fi
    echo "============================================================"
}

ensure_backend() {
    if [ ! -f "$BACKEND_PID_FILE" ] || ! kill -0 "$(cat "$BACKEND_PID_FILE")" 2>/dev/null; then
        echo "[E2E] Upstream backend not running; starting automatically..."
        start_backend
    fi
}

CMD="${1:-}"
shift || true

case "$CMD" in
    start-backend)
        start_backend
        ;;
    stop-backend)
        stop_backend
        ;;
    status)
        check_status
        ;;
    monitor)
        exec "$SCRIPT_DIR/monitor/live_tracker.sh" velda-edge
        ;;
    test)
        SUITE="${1:-}"
        shift || true
        ensure_backend
        case "$SUITE" in
            saturation)
                exec "$SCRIPT_DIR/scenarios/01_http1_saturation.sh" "$@"
                ;;
            churn)
                exec "$SCRIPT_DIR/scenarios/02_connection_churn.sh" "$@"
                ;;
            reload)
                exec "$SCRIPT_DIR/scenarios/03_reload_under_fire.sh" "$@"
                ;;
            soak)
                exec "$SCRIPT_DIR/scenarios/04_soak_test.sh" "$@"
                ;;
            all)
                echo "============================================================"
                echo " Running Full E2E Test Suite"
                echo "============================================================"
                "$SCRIPT_DIR/scenarios/01_http1_saturation.sh"
                echo ""
                "$SCRIPT_DIR/scenarios/02_connection_churn.sh"
                echo ""
                "$SCRIPT_DIR/scenarios/03_reload_under_fire.sh"
                echo ""
                "$SCRIPT_DIR/scenarios/04_soak_test.sh" "30s"
                echo ""
                echo "============================================================"
                echo " ALL E2E SUITES PASSED SUCCESSFULLY!"
                echo "============================================================"
                ;;
            *)
                echo "Unknown test suite: '$SUITE'" >&2
                usage
                ;;
        esac
        ;;
    *)
        usage
        ;;
esac
