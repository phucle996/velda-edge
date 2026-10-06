#!/usr/bin/env bash
# 10_attack_vectors.sh — Protocol Attack Vectors & Resource Exhaustion Defense Audit
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

TEST_NAME="${1:-all}"
SECURITY_TESTER="$ROOT_DIR/target/release/velda-security-tester"

echo "=========================================================================="
echo " [E2E] Scenario 10: Protocol Attack Vectors & Resource Exhaustion Audit"
echo " Attack Vector Suite: $TEST_NAME"
echo "=========================================================================="

if [ ! -x "$SECURITY_TESTER" ]; then
    echo "[E2E] Building velda-security-tester..."
    (cd "$ROOT_DIR/e2e/backend" && go build -o "$SECURITY_TESTER" ./cmd/security-tester)
fi

"$SECURITY_TESTER" -test "$TEST_NAME" \
    -h1 "127.0.0.1:8080" \
    -h2 "127.0.0.1:8443" \
    -tcp "127.0.0.1:9000"
