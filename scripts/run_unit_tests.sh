#!/usr/bin/env bash

set -euo pipefail

if [[ $# -gt 0 ]]; then
  ./.venv/bin/python -m pytest "$@"
else
  ./.venv/bin/python -m pytest -m "not integration"
fi
