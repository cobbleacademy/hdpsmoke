#!/usr/bin/env bash
# Runs every hsm-databricks-udf test, including the Spark job and the surrogate
# store, in Docker: a Spark 4.0 container and a Postgres 16 one on a private network.
#
#   hsm-databricks-udf/tests/deid/spark/run-tests.sh            # all tests
#   hsm-databricks-udf/tests/deid/spark/run-tests.sh -k job     # pytest arguments pass through
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
image=hsm-deid-spark-test:1
net="hsm-deid-test-$$"
pg="hsm-deid-pg-$$"

docker build -q -t "$image" "$here" >/dev/null
docker network create "$net" >/dev/null
cleanup() { docker rm -f "$pg" >/dev/null 2>&1 || true; docker network rm "$net" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$pg" --network "$net" -e POSTGRES_PASSWORD=deid-test -e POSTGRES_DB=deid postgres:16-alpine >/dev/null
for _ in $(seq 1 60); do
  docker exec "$pg" pg_isready -U postgres -d deid >/dev/null 2>&1 && break
  sleep 1
done

docker run --rm --network "$net" -v "$repo:/repo:ro" -w /repo/hsm-databricks-udf \
  -e DEID_TEST_PG_HOST="$pg" -e DEID_TEST_PG_PASSWORD=deid-test -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=/repo/hsm-databricks-udf/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  "$image" python3 -m pytest -p no:cacheprovider -q tests "$@"
