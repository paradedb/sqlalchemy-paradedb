#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Expected executable Python at ${PYTHON_BIN}. Create the virtualenv first (for example: python3 -m venv .venv)." >&2
  exit 1
fi

source "${SCRIPT_DIR}/run_paradedb.sh"

PORT="${PARADEDB_PORT}"
USER="${PARADEDB_USER}"
PASSWORD="${PARADEDB_PASSWORD}"
DB="${PARADEDB_DB}"

export PARADEDB_INTEGRATION=1
export PARADEDB_TEST_DSN="postgresql+psycopg://${USER}:${PASSWORD}@localhost:${PORT}/${DB}"
export PGPASSWORD="${PASSWORD}"

if [[ $# -gt 0 ]]; then
  "${PYTHON_BIN}" -m pytest "$@"
else
  "${PYTHON_BIN}" -m pytest -m integration
fi
