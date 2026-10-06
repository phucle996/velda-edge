#!/usr/bin/env bash
# 09_protocol_cross_pool.sh — Cross-Protocol Parallel Saturation Stress Test
# Tests concurrent load across L4 Raw TCP, HTTP/1.1, and HTTP/2 running simultaneously
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

WRK_BIN=$(which wrk || echo "$HOME/.local/bin/wrk")
H2LOAD_BIN=$(which h2load || echo "$HOME/.local/bin/h2load")
BLASTER_BIN="$ROOT_DIR/target/release/velda-tcp-blaster"

DURATION="20s"

echo "=========================================================================="
echo " [E2E] Scenario 09: Cross-Protocol Parallel Saturation Stress Test"
echo " Blasting L4 TCP (:9000), HTTP/1.1 (:8080), and HTTP/2 (:8443) simultaneously!"
echo " Duration: $DURATION"
echo "=========================================================================="

if [ ! -x "$BLASTER_BIN" ]; then
    echo "[E2E] Building velda-tcp-blaster..."
    go build -o "$BLASTER_BIN" "$ROOT_DIR/e2e/backend/tcp_blaster.go"
fi

OUTPUT_DIR="/tmp/velda_cross_protocol"
mkdir -p "$OUTPUT_DIR"

echo ""
echo ">>> Launching parallel blasters for $DURATION..."

# 1. Background Worker 1: L4 TCP Blaster (100 conns, 1KB messages)
echo " [1/3] Starting L4 Raw TCP Blaster on :9000 (100 conns)..."
"$BLASTER_BIN" -target "127.0.0.1:9000" -conns 100 -size 1024 -duration "$DURATION" > "$OUTPUT_DIR/l4_tcp.log" 2>&1 &
PID_L4=$!

# 2. Background Worker 2: HTTP/2 Multiplexer on :8443 (50 conns, 50 streams/conn)
echo " [2/3] Starting HTTP/2 Multiplexing Storm on :8443 (50 conns, 50 streams/conn)..."
"$H2LOAD_BIN" -D "$DURATION" -c 50 -m 50 --connect-to="127.0.0.1:8443" "https://api.example.com:8443/api/echo" > "$OUTPUT_DIR/http2.log" 2>&1 &
PID_H2=$!

# 3. Background Worker 3: HTTP/1.1 Saturation on :8080 (100 conns, 4 threads)
echo " [3/3] Starting HTTP/1.1 High-Load Blaster on :8080 (100 conns)..."
"$WRK_BIN" -t4 -c100 -d"$DURATION" --latency "http://127.0.0.1:8080/echo" > "$OUTPUT_DIR/http1.log" 2>&1 &
PID_H1=$!

echo ""
echo ">>> All 3 protocol engines are under full fire simultaneously. Waiting for completion..."
wait "$PID_L4"
wait "$PID_H2"
wait "$PID_H1"

echo ""
echo "=========================================================================="
echo " CROSS-PROTOCOL BENCHMARK RESULTS"
echo "=========================================================================="

echo "--- 1. L4 Raw TCP Splice Engine Results ---"
cat "$OUTPUT_DIR/l4_tcp.log" | grep -E "Throughput:|Data Transferred:|Latency|Errors|PASS|FAIL" || cat "$OUTPUT_DIR/l4_tcp.log"

echo ""
echo "--- 2. HTTP/2 TLS 1.3 Multiplexing Engine Results ---"
cat "$OUTPUT_DIR/http2.log" | grep -E "finished in|requests:|status codes:|traffic:" || cat "$OUTPUT_DIR/http2.log"

echo ""
echo "--- 3. HTTP/1.1 RFC 9112 Pipeline Engine Results ---"
cat "$OUTPUT_DIR/http1.log" | grep -E "Requests/sec:|Transfer/sec:|Latency " || cat "$OUTPUT_DIR/http1.log"

rm -rf "$OUTPUT_DIR"
echo ""
echo "=========================================================================="
echo " [E2E] Scenario 09 Cross-Protocol Stress completed successfully."
echo "=========================================================================="
