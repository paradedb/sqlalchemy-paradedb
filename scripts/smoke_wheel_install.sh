#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    echo "Unable to find a Python interpreter. Set PYTHON_BIN or install Python." >&2
    exit 1
  fi
fi

DIST_DIR="${WORK_DIR}/dist"
if command -v uv >/dev/null 2>&1; then
  uv build --wheel --out-dir "${DIST_DIR}"
else
  "${PYTHON_BIN}" -m pip wheel . --no-build-isolation -w "${DIST_DIR}"
fi

"${PYTHON_BIN}" -m venv "${WORK_DIR}/venv"
PYTHON_BIN="${WORK_DIR}/venv/bin/python"
PIP_BIN="${WORK_DIR}/venv/bin/pip"

"${PIP_BIN}" install --upgrade pip
"${PIP_BIN}" install "${DIST_DIR}"/sqlalchemy_paradedb-*.whl

cd "${WORK_DIR}"

PYTHONPATH="" "${PYTHON_BIN}" - <<'PY'
from sqlalchemy import column, select, table
from sqlalchemy.dialects import postgresql

from paradedb import match_all

products = table("products", column("description"))
stmt = select(products.c.description).where(match_all(products.c.description, "shoes"))
sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

if "&&&" not in sql:
    raise SystemExit(f"Wheel smoke test failed: expected ParadeDB SQL operator in compiled SQL: {sql}")
PY
