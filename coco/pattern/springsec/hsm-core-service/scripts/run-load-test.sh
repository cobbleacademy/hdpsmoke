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
if ! curl -sS -m 3 -o /dev/null "$BASE_URL/api/sensec/hsm/v1/admin/health"; then
  echo "hsm-core-service doesn't look reachable at $BASE_URL -- start it first:" >&2
  echo "  DEMO_MODE=true java -jar $MODULE_ROOT/target/hsm-core-service.jar" >&2
  exit 1
fi

echo "Running EncryptDecryptLoadSimulation against $BASE_URL ..."
mvn -q -f "$JAVA_ROOT/pom.xml" -pl hsm-core-service gatling:test \
  -Dgatling.simulationClass=com.hsm.core.loadtest.EncryptDecryptLoadSimulation \
  "$@"

echo
echo "Report: $MODULE_ROOT/target/gatling/<run-id>/index.html (see the 'Reports generated' line above for the exact path)"
