#!/usr/bin/env bash
# CI entry point, deliberately CI-system-agnostic: any pipeline (Azure Pipelines,
# GitHub Actions, Jenkins, ...) calls this one script, so the build is identical
# locally and in CI.
#
#   scripts/ci/build.sh                 # every stage
#   scripts/ci/build.sh java helm       # only the named stages (e.g. no Docker on the agent)
#   scripts/ci/build.sh --install-python python-udf
#
# Stages:
#   java        mvn verify over the whole java/ reactor (unit + integration tests,
#               ApiContractBaselineTest). Needs JDK 21+. Testcontainers tests run when
#               a Docker daemon is reachable and are skipped otherwise.
#   python-udf  pytest for hsm-databricks-udf (live-interop tests stay opt-in via
#               HSM_LIVE_TEST_BASE_URL).
#   helm        helm lint + helm template for every chart under helm/.
#   docker      docker build of every java/docker/Dockerfile.* -- the deployable
#               images, built exactly as a release would. Needs a Docker daemon.
#
# Python: uses $PYTHON if set, else ./.venv/bin/python if present, else python3.
# --install-python creates ./.ci-venv and installs hsm-databricks-udf with its test
# dependencies first (what a clean CI agent needs).
#
# The legacy Python service (app/, tests/, scheduler/) is not active and is
# deliberately not built here.
#
# Exit status is non-zero if any selected stage fails; every selected stage still
# runs, so one report shows all failures.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ALL_STAGES=(java python-udf helm docker)
INSTALL_PYTHON=false
STAGES=()

for arg in "$@"; do
    case "$arg" in
        --install-python) INSTALL_PYTHON=true ;;
        java|python-udf|helm|docker) STAGES+=("$arg") ;;
        -h|--help) sed -n '2,28p' "$0"; exit 0 ;;
        *) echo "unknown argument: $arg (stages: ${ALL_STAGES[*]})" >&2; exit 2 ;;
    esac
done
[ ${#STAGES[@]} -eq 0 ] && STAGES=("${ALL_STAGES[@]}")

if [ -n "${PYTHON:-}" ]; then
    :
elif [ "$INSTALL_PYTHON" = true ]; then
    PYTHON="$ROOT/.ci-venv/bin/python"
elif [ -x "$ROOT/.venv/bin/python" ]; then
    PYTHON="$ROOT/.venv/bin/python"
else
    PYTHON="python3"
fi

install_python() {
    python3 -m venv "$ROOT/.ci-venv" &&
        "$PYTHON" -m pip install --quiet --upgrade pip &&
        "$PYTHON" -m pip install --quiet -e "$ROOT/hsm-databricks-udf" pytest
}

stage_java() {
    (cd "$ROOT/java" && mvn -B -ntp verify)
}

stage_python_udf() {
    (cd "$ROOT/hsm-databricks-udf" && "$PYTHON" -m pytest -q)
}

# Charts whose templates `required` environment-specific values carry placeholder
# values under <chart>/ci/*-values.yaml (the chart-testing convention); each such
# file is rendered separately. Other charts render with their defaults.
stage_helm() {
    local chart values status=0
    for chart in "$ROOT"/helm/*/; do
        chart="${chart%/}"
        [ -f "$chart/Chart.yaml" ] || continue
        local value_files=("$chart"/ci/*-values.yaml)
        if [ ! -e "${value_files[0]}" ]; then
            echo "-- $(basename "$chart") (default values)"
            helm lint --quiet "$chart" || status=1
            helm template ci "$chart" > /dev/null || status=1
            continue
        fi
        for values in "${value_files[@]}"; do
            echo "-- $(basename "$chart") ($(basename "$values"))"
            helm lint --quiet "$chart" -f "$values" || status=1
            helm template ci "$chart" -f "$values" > /dev/null || status=1
        done
    done
    return $status
}

# Images are what actually ships, and they build from a partial copy of the
# reactor (each Dockerfile lists the modules it needs), so a passing `java` stage
# does not prove they build -- e.g. a new module in java/pom.xml that a Dockerfile
# doesn't copy breaks every image while every test still passes.
stage_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "no Docker daemon reachable -- start one, or run the other stages explicitly"
        return 1
    fi
    local dockerfile name status=0
    for dockerfile in "$ROOT"/java/docker/Dockerfile.*; do
        name="${dockerfile##*/Dockerfile.}"
        echo "-- $name"
        docker build --quiet -f "$dockerfile" -t "hsm-ci/$name:ci" "$ROOT/java" > /dev/null || status=1
    done
    return $status
}

FAILED=()
run() {
    local name="$1"; shift
    echo
    echo "==== [$name] ===================================================="
    local start=$SECONDS
    if "$@"; then
        echo "==== [$name] passed in $((SECONDS - start))s"
    else
        echo "==== [$name] FAILED after $((SECONDS - start))s"
        FAILED+=("$name")
    fi
}

if [ "$INSTALL_PYTHON" = true ]; then
    run install-python install_python
fi

for stage in "${STAGES[@]}"; do
    case "$stage" in
        java)       run java stage_java ;;
        python-udf) run python-udf stage_python_udf ;;
        helm)       run helm stage_helm ;;
        docker)     run docker stage_docker ;;
    esac
done

echo
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "CI FAILED: ${FAILED[*]}"
    exit 1
fi
echo "CI passed: ${STAGES[*]}"
