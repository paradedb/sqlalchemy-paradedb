#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run integration tests." >&2
  echo "Install uv, then rerun this script." >&2
  exit 1
fi

if [[ -z "${PARADEDB_TEST_DSN:-${DATABASE_URL:-}}" ]]; then
  # shellcheck source=scripts/run_paradedb.sh
  source "${SCRIPT_DIR}/run_paradedb.sh"
fi

export PARADEDB_TEST_DSN="${PARADEDB_TEST_DSN:-${DATABASE_URL}}"
export DATABASE_URL="${DATABASE_URL:-${PARADEDB_TEST_DSN}}"
export PGPASSWORD="${PGPASSWORD:-${PARADEDB_PASSWORD:-postgres}}"

export PARADEDB_INTEGRATION=1

PYTEST_CMD=(uv run --extra test pytest)

cd "${REPO_ROOT}"

if [[ $# -gt 0 ]]; then
  "${PYTEST_CMD[@]}" "$@"
else
  "${PYTEST_CMD[@]}" -m integration
fi
