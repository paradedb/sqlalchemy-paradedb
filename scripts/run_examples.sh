#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Expected executable Python at ${PYTHON_BIN}. Create the virtualenv first (for example: python3 -m venv .venv)." >&2
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

"${PYTHON_BIN}" examples/quickstart.py
"${PYTHON_BIN}" examples/autocomplete.py
"${PYTHON_BIN}" examples/more_like_this.py
"${PYTHON_BIN}" examples/faceted_search.py
"${PYTHON_BIN}" examples/hybrid_rrf.py
"${PYTHON_BIN}" examples/rag.py
