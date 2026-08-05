#!/usr/bin/env bash

# Check if script is being run directly or sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  RUNNING=1
  set -euo pipefail
else
  RUNNING=0
fi

IMAGE="${PARADEDB_IMAGE:-paradedb/paradedb:0.25.0-pg18}"
CONTAINER_NAME="${PARADEDB_CONTAINER_NAME:-sqlalchemy-paradedb}"
export PARADEDB_PORT="${PARADEDB_PORT:-5432}"
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

# A container left over from a failed run can exist without publishing the
# port, in which case reusing it yields confusing connection errors. Drop it
# so the normal creation path below builds a working one.
if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}" &&
! docker port "${CONTAINER_NAME}" 5432 2>/dev/null | grep -q ":${PORT}$"; then
  echo "Container ${CONTAINER_NAME} exists but does not publish port ${PORT}; recreating it..."
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
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

# Check readiness over TCP: during first-time initialization the image runs a
# temporary socket-only server that seeds extensions and sample data, and it
# must not be mistaken for the real one.
echo "Waiting for ParadeDB to become ready..."
for _ in $(seq 1 "${PARADEDB_WAIT_ATTEMPTS:-45}"); do
  if docker exec "${CONTAINER_NAME}" pg_isready -h 127.0.0.1 -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker exec "${CONTAINER_NAME}" pg_isready -h 127.0.0.1 -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
  echo "ParadeDB did not become ready in time" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

echo "ParadeDB is running in container ${CONTAINER_NAME}."
echo "DATABASE_URL is set to: ${DATABASE_URL}"

if [[ "$RUNNING" == "0" ]]; then
  echo "You can now use integration tests in your current shell."
fi
