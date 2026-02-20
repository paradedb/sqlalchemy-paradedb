#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/run_paradedb.sh"

PORT="${PARADEDB_PORT}"
USER="${PARADEDB_USER}"
PASSWORD="${PARADEDB_PASSWORD}"
DB="${PARADEDB_DB}"

export PARADEDB_INTEGRATION=1
export PARADEDB_TEST_DSN="postgresql+psycopg://${USER}:${PASSWORD}@localhost:${PORT}/${DB}"
export PGPASSWORD="${PASSWORD}"

if [[ $# -gt 0 ]]; then
  ./.venv/bin/python -m pytest "$@"
else
  ./.venv/bin/python -m pytest -m integration
fi
