#!/usr/bin/env bash

# Check if script is being run directly or sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  RUNNING=1
  set -euo pipefail
else
  RUNNING=0
fi

PARADEDB_IMAGE="${PARADEDB_IMAGE:-paradedb/paradedb:0.21.9-pg18}"
PARADEDB_CONTAINER_NAME="${PARADEDB_CONTAINER_NAME:-paradedb-sqlalchemy-integration}"
PARADEDB_PORT="${PARADEDB_PORT:-5443}"
PARADEDB_USER="${PARADEDB_USER:-postgres}"
PARADEDB_PASSWORD="${PARADEDB_PASSWORD:-postgres}"
PARADEDB_DB="${PARADEDB_DB:-postgres}"
PARADEDB_WAIT_ATTEMPTS="${PARADEDB_WAIT_ATTEMPTS:-45}"

export PARADEDB_IMAGE
export PARADEDB_CONTAINER_NAME
export PARADEDB_PORT
export PARADEDB_USER
export PARADEDB_PASSWORD
export PARADEDB_DB
export PARADEDB_WAIT_ATTEMPTS

IMAGE="${PARADEDB_IMAGE}"
CONTAINER_NAME="${PARADEDB_CONTAINER_NAME}"
PORT="${PARADEDB_PORT}"
USER="${PARADEDB_USER}"
PASSWORD="${PARADEDB_PASSWORD}"
DB="${PARADEDB_DB}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run ParadeDB" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

container_exists=0
if docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"; then
  container_exists=1
fi

if [[ "$container_exists" == "0" ]]; then
  echo "Starting ParadeDB container ${CONTAINER_NAME} from ${IMAGE}..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=${USER}" \
    -e "POSTGRES_PASSWORD=${PASSWORD}" \
    -e "POSTGRES_DB=${DB}" \
    -p "${PORT}:5432" \
    "${IMAGE}" >/dev/null
else
  mapped_port="$(docker port "${CONTAINER_NAME}" 5432/tcp 2>/dev/null | head -n1 | awk -F: '{print $NF}')"
  if [[ -n "${mapped_port}" && "${mapped_port}" != "${PORT}" ]]; then
    echo "Container ${CONTAINER_NAME} is already mapped to host port ${mapped_port}; using that port."
    PORT="${mapped_port}"
  elif [[ -z "${mapped_port}" ]]; then
    echo "Container ${CONTAINER_NAME} has no published 5432 port; recreating with ${PORT}:5432..."
    docker rm -f "${CONTAINER_NAME}" >/dev/null
    docker run -d \
      --name "${CONTAINER_NAME}" \
      -e "POSTGRES_USER=${USER}" \
      -e "POSTGRES_PASSWORD=${PASSWORD}" \
      -e "POSTGRES_DB=${DB}" \
      -p "${PORT}:5432" \
      "${IMAGE}" >/dev/null
  fi
  echo "Container ${CONTAINER_NAME} already exists; starting it..."
  docker start "${CONTAINER_NAME}" >/dev/null
fi

export PARADEDB_PORT="${PORT}"
DATABASE_URL="postgresql+psycopg://${USER}:${PASSWORD}@localhost:${PORT}/${DB}"
export DATABASE_URL

echo "Waiting for ParadeDB to become ready..."
for _ in $(seq 1 "${PARADEDB_WAIT_ATTEMPTS}"); do
  in_container_ready=0
  host_ready=0

  if docker exec "${CONTAINER_NAME}" pg_isready -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
    in_container_ready=1
  fi

  if command -v pg_isready >/dev/null 2>&1; then
    if PGPASSWORD="${PASSWORD}" pg_isready -h localhost -p "${PORT}" -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
      host_ready=1
    fi
  else
    if (echo >/dev/tcp/127.0.0.1/"${PORT}") >/dev/null 2>&1; then
      host_ready=1
    fi
  fi

  if [[ "${in_container_ready}" == "1" && "${host_ready}" == "1" ]]; then
    break
  fi
  sleep 2
done

if ! docker exec "${CONTAINER_NAME}" pg_isready -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
  echo "ParadeDB did not become ready in time (container check failed)" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

if command -v pg_isready >/dev/null 2>&1; then
  if ! PGPASSWORD="${PASSWORD}" pg_isready -h localhost -p "${PORT}" -U "${USER}" -d "${DB}" >/dev/null 2>&1; then
    echo "ParadeDB did not become reachable on localhost:${PORT} in time" >&2
    if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
  fi
elif ! (echo >/dev/tcp/127.0.0.1/"${PORT}") >/dev/null 2>&1; then
  echo "ParadeDB TCP port localhost:${PORT} is not reachable" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

echo "ParadeDB is running in container ${CONTAINER_NAME}."
echo "DATABASE_URL is set to: ${DATABASE_URL}"

if [[ "$RUNNING" == "0" ]]; then
  echo "You can now use integration tests in your current shell."
fi
