#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run integration tests." >&2
  echo "Install uv, then rerun this script." >&2
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

PYTEST_CMD=(uv run --extra test pytest)

cd "${REPO_ROOT}"

if [[ $# -gt 0 ]]; then
  "${PYTEST_CMD[@]}" "$@"
else
  "${PYTEST_CMD[@]}" -m integration
fi
