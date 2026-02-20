#!/usr/bin/env bash

# Check if script is being run directly or sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  RUNNING=1
  set -euo pipefail
else
  RUNNING=0
fi

PARADEDB_IMAGE="${PARADEDB_IMAGE:-paradedb/paradedb:0.21.8-pg18}"
PARADEDB_CONTAINER_NAME="${PARADEDB_CONTAINER_NAME:-paradedb-sqlalchemy-integration}"
PARADEDB_PORT="${PARADEDB_PORT:-5443}"
PARADEDB_USER="${PARADEDB_USER:-postgres}"
PARADEDB_PASSWORD="${PARADEDB_PASSWORD:-postgres}"
PARADEDB_DB="${PARADEDB_DB:-postgres}"

export PARADEDB_IMAGE
export PARADEDB_CONTAINER_NAME
export PARADEDB_PORT
export PARADEDB_USER
export PARADEDB_PASSWORD
export PARADEDB_DB

IMAGE="${PARADEDB_IMAGE}"
CONTAINER_NAME="${PARADEDB_CONTAINER_NAME}"
PORT="${PARADEDB_PORT}"
USER="${PARADEDB_USER}"
PASSWORD="${PARADEDB_PASSWORD}"
DB="${PARADEDB_DB}"

DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://${USER}:${PASSWORD}@localhost:${PORT}/${DB}}"
export DATABASE_URL

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run ParadeDB" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

if ! docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"; then
  echo "Starting ParadeDB container ${CONTAINER_NAME} from ${IMAGE}..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=${USER}" \
    -e "POSTGRES_PASSWORD=${PASSWORD}" \
    -e "POSTGRES_DB=${DB}" \
    -p "${PORT}:5432" \
    "${IMAGE}" >/dev/null
else
  echo "Container ${CONTAINER_NAME} already exists; starting it..."
  docker start "${CONTAINER_NAME}" >/dev/null
fi

echo "Waiting for ParadeDB to become ready..."
for _ in {1..30}; do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker exec "${CONTAINER_NAME}" pg_isready -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
  echo "ParadeDB did not become ready in time" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

echo "ParadeDB is running in container ${CONTAINER_NAME}."
echo "DATABASE_URL is set to: ${DATABASE_URL}"

if [[ "$RUNNING" == "0" ]]; then
  echo "You can now use integration tests in your current shell."
fi
