#!/usr/bin/env bash
# Runs the Gatling load test (EncryptDecryptLoadSimulation) against an already
# running hsm-core-service. Run from anywhere. See java/docs/PERFORMANCE_TESTING.md.
#
# Usage:
#   scripts/run-load-test.sh [-Dhsm.singleUsers=50 -Dhsm.holdSeconds=60 ...]
#
# Examples:
#   scripts/run-load-test.sh                            # defaults: 20 single + 5 batch users, 10s ramp, 30s hold
#   scripts/run-load-test.sh -Dhsm.singleUsers=50        # heavier single-item load
#   scripts/run-load-test.sh -Dhsm.baseUrl=http://localhost:8080
#   HSM_API_VERSION=V2 scripts/run-load-test.sh          # load API v2 (/api/sensec/hsm/v2)
#
# API version: HSM_API_VERSION=V1 (default) or V2 selects the prefix that is
# health-checked and loaded (passed on as -Dhsm.apiVersion). Switch back with
# V1 or unset. HSM_API_PREFIX overrides the prefix outright.
#
# hsm-core-service must already be running (DEMO_MODE=true java -jar
# target/hsm-core-service.jar) -- this script only drives load against it, it
# does not start the service itself. All tuning knobs are documented in
# EncryptDecryptLoadSimulation's own javadoc.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAVA_ROOT="$(cd "$MODULE_ROOT/.." && pwd)"

BASE_URL="${HSM_BASE_URL:-http://localhost:3005}"
API_VERSION="$(echo "${HSM_API_VERSION:-V1}" | tr '[:lower:]' '[:upper:]')"
case "$API_VERSION" in
  V1) DEFAULT_PREFIX=/api/sensec/hsm/v1 ;;
  V2) DEFAULT_PREFIX=/api/sensec/hsm/v2 ;;
  *) echo "HSM_API_VERSION must be V1 or V2, got '$API_VERSION'" >&2; exit 1 ;;
esac
API_PREFIX="${HSM_API_PREFIX:-$DEFAULT_PREFIX}"
if ! curl -sS -m 3 -o /dev/null "$BASE_URL$API_PREFIX/admin/health"; then
  echo "hsm-core-service doesn't look reachable at $BASE_URL -- start it first:" >&2
  echo "  DEMO_MODE=true java -jar $MODULE_ROOT/target/hsm-core-service.jar" >&2
  exit 1
fi

echo "Running EncryptDecryptLoadSimulation against $BASE_URL$API_PREFIX ($API_VERSION) ..."
mvn -q -f "$JAVA_ROOT/pom.xml" -pl hsm-core-service gatling:test \
  -Dgatling.simulationClass=com.hsm.core.loadtest.EncryptDecryptLoadSimulation \
  -Dhsm.apiVersion="$API_VERSION" -Dhsm.apiPrefix="$API_PREFIX" \
  "$@"

echo
echo "Report: $MODULE_ROOT/target/gatling/<run-id>/index.html (see the 'Reports generated' line above for the exact path)"
