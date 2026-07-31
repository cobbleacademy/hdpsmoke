#!/usr/bin/env bash
# Runs hsm-bulk-client.jar against a job config file. Run from anywhere.
#
# Usage:
#   scripts/run.sh <path-to-job-config.yml> [extra --spring.* args...]
#
# Examples:
#   scripts/run.sh config-examples/db-encrypt-example.yml
#   scripts/run.sh config-examples/file-decrypt-example.yml
#
# The config file is layered on top of src/main/resources/application.yml via
# --spring.config.additional-location (values there win over the packaged
# defaults) -- exactly the pattern used to verify both the BULK DB and BULK
# File jobs end-to-end (see java/docs/TIER3_POC_BUILD.md). Build the jar first
# with scripts/build.sh if target/hsm-bulk-client.jar doesn't exist yet.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_PATH="$MODULE_ROOT/target/hsm-bulk-client.jar"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-job-config.yml> [extra --spring.* args...]" >&2
  exit 1
fi

CONFIG_FILE="$1"
shift

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: $CONFIG_FILE" >&2
  exit 1
fi
# Resolve to an absolute path -- --spring.config.additional-location needs one
# regardless of what directory this script is invoked from.
CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Jar not found at $JAR_PATH -- build it first: scripts/build.sh" >&2
  exit 1
fi

echo "Running hsm-bulk-client with config: $CONFIG_FILE"
java -jar "$JAR_PATH" \
  --spring.config.additional-location="file:$CONFIG_FILE" \
  "$@"
