#!/usr/bin/env bash
# Port-forwards a deployed hsm-core-service Service and runs the Gatling load
# test (scripts/run-load-test.sh) against it. No new infra -- the in-cluster
# alternative is helm/hsm-core-service-loadtest-job. See
# java/docs/PERFORMANCE_TESTING.md's "Deployed environment" section for the
# tradeoff between the two.
#
# Deployed hsm-core-service runs with DEMO_MODE=false -- MockJwtValidator's
# demo-token-* strings do NOT work here. You must supply a real bearer token
# (HSM_TOKEN) for a real app_id (HSM_APP_ID) registered with both encrypt and
# decrypt scopes. A real Azure AD JWT is short-lived (~1h) -- mint a fresh one
# right before running this for anything but a quick pass.
#
# Usage:
#   HSM_APP_ID=<app-id> HSM_TOKEN=<real-jwt> \
#     scripts/run-load-test-k8s.sh -n <namespace> -r <release-name> [-p <local-port>] [extra -D args...]
#
# Example:
#   HSM_APP_ID=payments-svc HSM_TOKEN=eyJhbGciOi... \
#     scripts/run-load-test-k8s.sh -n hsm-prod -r hsm-core-service -Dhsm.singleUsers=50
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NAMESPACE=""
RELEASE=""
LOCAL_PORT="13005"   # arbitrary, avoids colliding with a locally-running demo instance on 3005

while getopts "n:r:p:" opt; do
  case "$opt" in
    n) NAMESPACE="$OPTARG" ;;
    r) RELEASE="$OPTARG" ;;
    p) LOCAL_PORT="$OPTARG" ;;
    *) echo "Usage: $0 -n <namespace> -r <release-name> [-p <local-port>] [extra -D args...]" >&2; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

if [[ -z "$NAMESPACE" || -z "$RELEASE" ]]; then
  echo "Usage: $0 -n <namespace> -r <release-name> [-p <local-port>] [extra -D args...]" >&2
  exit 1
fi
if [[ -z "${HSM_APP_ID:-}" || -z "${HSM_TOKEN:-}" ]]; then
  echo "HSM_APP_ID and HSM_TOKEN must be set -- a real deployment doesn't accept demo mode's fixed tokens." >&2
  exit 1
fi

# service.yaml exposes port 3005 under the Helm release name directly (see
# helm/hsm-core-service/templates/_helpers.tpl's hsm.fullname: just
# .Release.Name, no chart-name suffix) -- $RELEASE below IS the Service name.
echo "Port-forwarding svc/$RELEASE (namespace $NAMESPACE) -> localhost:$LOCAL_PORT ..."
kubectl -n "$NAMESPACE" port-forward "svc/$RELEASE" "$LOCAL_PORT:3005" >/tmp/hsm-port-forward.log 2>&1 &
PF_PID=$!
trap 'echo "Stopping port-forward (pid $PF_PID)"; kill "$PF_PID" 2>/dev/null || true' EXIT

export HSM_BASE_URL="http://localhost:$LOCAL_PORT"
for _ in $(seq 1 15); do
  if curl -sS -m 2 -o /dev/null "$HSM_BASE_URL/api/sensec/hsm/v1/admin/health"; then
    break
  fi
  sleep 1
done
if ! curl -sS -m 2 -o /dev/null "$HSM_BASE_URL/api/sensec/hsm/v1/admin/health"; then
  echo "Port-forward didn't come up -- check /tmp/hsm-port-forward.log (svc/$RELEASE in namespace $NAMESPACE reachable?)" >&2
  exit 1
fi

echo "Running EncryptDecryptLoadSimulation against the deployed instance via $HSM_BASE_URL ..."
"$SCRIPT_DIR/run-load-test.sh" \
  -Dhsm.baseUrl="$HSM_BASE_URL" \
  -Dhsm.appId="$HSM_APP_ID" \
  -Dhsm.token="$HSM_TOKEN" \
  "$@"
