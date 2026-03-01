#!/usr/bin/env bash

set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Expected executable Python at ${PYTHON_BIN}. Create the virtualenv first (for example: python3 -m venv .venv)." >&2
  exit 1
fi

if [[ $# -gt 0 ]]; then
  "${PYTHON_BIN}" -m pytest "$@"
else
  "${PYTHON_BIN}" -m pytest -m "not integration"
fi
