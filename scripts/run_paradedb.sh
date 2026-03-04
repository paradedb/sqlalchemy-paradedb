#!/usr/bin/env bash

# Check if script is being run directly or sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  RUNNING=1
  set -euo pipefail
else
  RUNNING=0
fi

IMAGE="${PARADEDB_IMAGE:-paradedb/paradedb:0.21.10-pg18}"
CONTAINER_NAME="${PARADEDB_CONTAINER_NAME:-paradedb-sqlalchemy-integration}"
export PARADEDB_PORT="${PARADEDB_PORT:-5443}"
export PARADEDB_USER="${PARADEDB_USER:-postgres}"
export PARADEDB_PASSWORD="${PARADEDB_PASSWORD:-postgres}"
export PARADEDB_DB="${PARADEDB_DB:-postgres}"
PORT="${PARADEDB_PORT}"
USER="${PARADEDB_USER}"
PASSWORD="${PARADEDB_PASSWORD}"
DB="${PARADEDB_DB}"

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

DATABASE_URL="postgresql+psycopg://${USER}:${PASSWORD}@localhost:${PORT}/${DB}"
export DATABASE_URL

echo "Waiting for ParadeDB to become ready..."
for _ in $(seq 1 "${PARADEDB_WAIT_ATTEMPTS:-45}"); do
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
