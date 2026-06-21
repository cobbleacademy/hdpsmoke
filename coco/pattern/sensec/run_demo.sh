#!/usr/bin/env bash
# Launches the HSM Encryption Service in DEMO_MODE with zero external
# dependencies (no real Azure Key Vault, no Postgres, no Splunk).
# Opens the live UI at http://localhost:3005
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  PYBIN="$(command -v python3.14 || echo /usr/local/Cellar/python@3.14/3.14.6/bin/python3.14)"
  if [ ! -x "$PYBIN" ]; then
    PYBIN="python3.12"
  fi
  "$PYBIN" -m venv .venv
fi

source .venv/bin/activate
pip install --quiet -e ".[dev]" 2>/dev/null || pip install --quiet -e .
pip install --quiet aiosqlite

cp -n .env.demo .env || true

echo "Starting demo service at http://localhost:3005 ..."
uvicorn app.main:app --host 0.0.0.0 --port 3005 --reload
