#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${SKIP_PARADEDB_START:-0}" != "1" ]]; then
  source "${SCRIPT_DIR}/run_paradedb.sh"
fi

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://postgres:postgres@localhost:${PARADEDB_PORT:-5443}/postgres}"

./.venv/bin/python examples/quickstart.py
./.venv/bin/python examples/autocomplete.py
./.venv/bin/python examples/more_like_this.py
./.venv/bin/python examples/faceted_search.py
./.venv/bin/python examples/hybrid_rrf.py
./.venv/bin/python examples/rag.py
