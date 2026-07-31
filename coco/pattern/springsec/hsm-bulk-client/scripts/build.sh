#!/usr/bin/env bash
# Builds hsm-bulk-client.jar. Run from anywhere -- resolves the Maven reactor
# root (java/) relative to this script's own location, not the caller's cwd.
#
# Usage:
#   scripts/build.sh              # skips tests (fast, matches how this module
#                                  # was built for every verification run so far)
#   scripts/build.sh --with-tests # runs hsm-bulk-client's own test suite first
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAVA_REACTOR_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

SKIP_TESTS="-DskipTests"
if [[ "${1:-}" == "--with-tests" ]]; then
  SKIP_TESTS=""
fi

cd "$JAVA_REACTOR_ROOT"
echo "Building hsm-bulk-client from $JAVA_REACTOR_ROOT ..."
mvn -pl hsm-bulk-client -am package $SKIP_TESTS

JAR_PATH="$JAVA_REACTOR_ROOT/hsm-bulk-client/target/hsm-bulk-client.jar"
if [[ -f "$JAR_PATH" ]]; then
  echo ""
  echo "Built: $JAR_PATH"
else
  echo "Build reported success but $JAR_PATH is missing -- something's wrong." >&2
  exit 1
fi
