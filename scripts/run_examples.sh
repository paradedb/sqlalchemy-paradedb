#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run examples." >&2
  echo "Install uv, then rerun this script." >&2
  exit 1
fi

if [[ "${SKIP_PARADEDB_START:-0}" != "1" ]]; then
  source "${SCRIPT_DIR}/run_paradedb.sh"
fi

PORT="${PARADEDB_PORT:-5443}"
USER="${PARADEDB_USER:-postgres}"
PASSWORD="${PARADEDB_PASSWORD:-postgres}"
DB="${PARADEDB_DB:-postgres}"

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://${USER}:${PASSWORD}@localhost:${PORT}/${DB}}"

PYTHON_CMD=(uv run --extra test python)

cd "${REPO_ROOT}"

"${PYTHON_CMD[@]}" examples/quickstart.py
"${PYTHON_CMD[@]}" examples/autocomplete.py
"${PYTHON_CMD[@]}" examples/more_like_this.py
"${PYTHON_CMD[@]}" examples/faceted_search.py
"${PYTHON_CMD[@]}" examples/hybrid_rrf.py
"${PYTHON_CMD[@]}" examples/rag.py
