#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${ROOT_DIR}/.tmp/replica-cluster"
BIN_PATH="${WORK_DIR}/replicated-clob-server"
LOG_DIR="${WORK_DIR}/logs"

PRIMARY_PORT="${1:-8080}"
SECONDARY_START_PORT="${2:-8081}"
SECONDARY_COUNT="${3:-3}"
PRIMARY_URL="http://127.0.0.1:${PRIMARY_PORT}"

if ! command -v go >/dev/null 2>&1; then
  echo "go is required to run the replica cluster" >&2
  exit 1
fi

if [[ "${SECONDARY_COUNT}" -lt 1 ]]; then
  echo "secondaries must be greater than or equal to 1" >&2
  exit 1
fi

mkdir -p "${WORK_DIR}" "${LOG_DIR}"

declare -a secondary_ports
declare -a secondary_urls
declare -a pids

for ((i = 0; i < SECONDARY_COUNT; i++)); do
  secondary_port=$((SECONDARY_START_PORT + i))
  secondary_ports+=("${secondary_port}")
  secondary_urls+=("http://127.0.0.1:${secondary_port}")
done

echo "Building server binary..."
go build -o "${BIN_PATH}" "${ROOT_DIR}/cmd/server"

PEERS=""
for i in "${!secondary_urls[@]}"; do
  if [[ -n "${PEERS}" ]]; then
    PEERS+=","
  fi
  PEERS+="${secondary_urls[$i]}"
done

cleanup() {
  echo
  echo "Shutting down replica cluster..."
  for pid in "${pids[@]}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" || true
    fi
  done

  for pid in "${pids[@]}"; do
    wait "${pid}" 2>/dev/null || true
  done
}

trap cleanup EXIT
trap 'exit 0' INT TERM

start_node() {
  local role=$1
  local port=$2
  local label=$3
  local peers=$4
  local primary_url=$5
  local log_file="${LOG_DIR}/${label}.log"

  echo "Starting ${label} (${role}) on port ${port}"

  local args=(--port "${port}" --mode "${role}")
  if [[ -n "${peers}" ]]; then
    args+=(--peers "${peers}")
  fi
  if [[ -n "${primary_url}" ]]; then
    args+=(--primary "${primary_url}")
  fi

  "${BIN_PATH}" "${args[@]}" >"${log_file}" 2>&1 &
  local pid=$!
  pids+=("${pid}")
  echo "  pid=${pid} log=${log_file}"
}

start_node "primary" "${PRIMARY_PORT}" "primary" "${PEERS}" ""

for ((i = 0; i < SECONDARY_COUNT; i++)); do
  secondary_port=${secondary_ports[$i]}
  secondary_label="secondary-$((i + 1))"
  secondary_peers="${PRIMARY_URL}"
  for ((j = 0; j < SECONDARY_COUNT; j++)); do
    peer_port="${secondary_ports[$j]}"
    if [[ "${peer_port}" -eq "${secondary_port}" ]]; then
      continue
    fi
    secondary_peers="${secondary_peers},${secondary_urls[$j]}"
  done

  start_node "secondary" "${secondary_port}" "${secondary_label}" "${secondary_peers}" "${PRIMARY_URL}"
done

echo
echo "Replica cluster running."
echo "Primary:   ${PRIMARY_URL}"
for secondary_url in "${secondary_urls[@]}"; do
  echo "Secondary: ${secondary_url}"
done
echo "Logs: ${LOG_DIR}"
echo "Press Ctrl-C to stop all nodes."

wait
