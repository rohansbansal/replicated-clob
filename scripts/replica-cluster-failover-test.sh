#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${ROOT_DIR}/.tmp/replica-failover-test"
BIN_PATH="${WORK_DIR}/replicated-clob-server"
LOG_DIR="${WORK_DIR}/logs"

PRIMARY_PORT="${1:-8080}"
SECONDARY_START_PORT="${2:-8081}"
SECONDARY_COUNT=3

if ! command -v go >/dev/null 2>&1; then
  echo "go is required to build and run the test cluster" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for JSON parsing (install with: brew install jq)" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to send HTTP requests" >&2
  exit 1
fi

PRIMARY_URL="http://127.0.0.1:${PRIMARY_PORT}"

mkdir -p "${WORK_DIR}" "${LOG_DIR}"

declare -a NODE_LABELS=()
declare -a NODE_PORTS=()
declare -a NODE_URLS=()
declare -a NODE_PIDS=()
declare -a NODE_ROLES=()
declare -a NODE_PEERS=()
declare -a NODE_LOGS=()

HTTP_CODE=""
HTTP_BODY=""

start_node() {
  local idx=$1
  local role=$2
  local port=$3
  local peers=$4
  local primary=$5

  local log_file="${LOG_DIR}/${NODE_LABELS[$idx]}.log"
  local -a args=(--port "${port}" --mode "${role}")

  if [[ -n "${peers}" ]]; then
    args+=(--peers "${peers}")
  fi
  if [[ -n "${primary}" ]]; then
    args+=(--primary "${primary}")
  fi

  "${BIN_PATH}" "${args[@]}" >"${log_file}" 2>&1 &
  NODE_PIDS[$idx]=$!
}

tail_node_log() {
  local idx=$1
  local label=$2
  local log_file="${NODE_LOGS[$idx]:-${LOG_DIR}/${label}.log}"

  if [[ -f "${log_file}" ]]; then
    echo "--- ${label} startup log tail (${log_file}) ---" >&2
    tail -n 40 "${log_file}" >&2
    echo "--- end startup log tail ---" >&2
  else
    echo "No startup log file at ${log_file}" >&2
  fi
}

build_node_peer_csv() {
  local -a peers=("$@")
  local csv=""
  for peer in "${peers[@]}"; do
    if [[ -n "${csv}" ]]; then
      csv+=","
    fi
    csv+="${peer}"
  done
  echo "${csv}"
}

start_cluster() {
  local -a secondary_ports=()
  local -a secondary_urls=()
  local -a secondary_peer_csvs=()

  for ((i = 0; i < SECONDARY_COUNT; i++)); do
    local port=$((SECONDARY_START_PORT + i))
    secondary_ports[i]="${port}"
    secondary_urls[i]="http://127.0.0.1:${port}"
  done

  # primary peers include all secondaries
  local primary_peers
  primary_peers="$(build_node_peer_csv "${secondary_urls[@]}")"

  NODE_LABELS[0]="primary"
  NODE_PORTS[0]="${PRIMARY_PORT}"
  NODE_URLS[0]="${PRIMARY_URL}"
  NODE_ROLES[0]="primary"
  NODE_PEERS[0]="${primary_peers}"
  NODE_LOGS[0]="${LOG_DIR}/primary.log"
  start_node 0 "primary" "${PRIMARY_PORT}" "${primary_peers}" ""

  for ((i = 0; i < SECONDARY_COUNT; i++)); do
    local idx=$((i + 1))
    local secondary_label="secondary-$((i + 1))"
    NODE_LABELS[$idx]="${secondary_label}"
    NODE_PORTS[$idx]="${secondary_ports[i]}"
    NODE_URLS[$idx]="${secondary_urls[i]}"
    NODE_ROLES[$idx]="secondary"

    local -a peers=("${PRIMARY_URL}")
    for ((j = 0; j < SECONDARY_COUNT; j++)); do
      if ((j == i)); then
        continue
      fi
      peers+=("${secondary_urls[j]}")
    done
    secondary_peer_csvs[i]="$(build_node_peer_csv "${peers[@]}")"
    NODE_PEERS[$idx]="${secondary_peer_csvs[i]}"
    NODE_LOGS[$idx]="${LOG_DIR}/${secondary_label}.log"
    start_node "$idx" "secondary" "${secondary_ports[i]}" "${secondary_peer_csvs[i]}" "${PRIMARY_URL}"
  done
}

stop_node() {
  local idx=$1
  local pid="${NODE_PIDS[$idx]:-}"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
  fi
}

stop_cluster() {
  for idx in "${!NODE_PIDS[@]}"; do
    stop_node "${idx}"
  done
}

cleanup() {
  echo
  echo "Shutting down nodes..."
  stop_cluster

  for idx in "${!NODE_PIDS[@]}"; do
    local pid="${NODE_PIDS[$idx]:-}"
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null || true
    fi
  done
}

wait_for_node_up() {
  local idx=$1
  local url="${NODE_URLS[$idx]}/internal/replica/state"
  local orders_url="${NODE_URLS[$idx]}/orders/alice"
  local label="${NODE_LABELS[$idx]}"
  local pid="${NODE_PIDS[$idx]:-}"

  for attempt in $(seq 1 200); do
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      echo "${label} exited during startup" >&2
      tail_node_log "${idx}" "${label}"
      return 1
    fi

    HTTP_BODY=""
    HTTP_CODE=""
    if do_http GET "${url}" "" 2>/dev/null; then
      if [[ "${HTTP_CODE}" == "200" ]]; then
        echo "${label} ready after ${attempt} attempts (replica state)" >&2
        return 0
      fi
    fi
    HTTP_BODY=""
    HTTP_CODE=""
    if do_http GET "${orders_url}" "" 2>/dev/null; then
      if [[ "${HTTP_CODE}" == "200" ]]; then
        echo "${label} ready after ${attempt} attempts (orders api)" >&2
        return 0
      fi
    fi
    if ((attempt % 25 == 0)); then
      debug_node_probe_state "${idx}" "${label}"
    fi
    sleep 0.1
  done

  echo "Timed out waiting for ${label} at ${url} to come up" >&2
  debug_node_probe_state "${idx}" "${label}"
  tail_node_log "${idx}" "${label}"
  return 1
}

wait_for_node_down() {
  local idx=$1
  local url="${NODE_URLS[$idx]}"
  for attempt in $(seq 1 120); do
    if ! curl -sS --max-time 1 "${url}/internal/replica/state" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for ${NODE_LABELS[$idx]} to stop" >&2
  return 1
}

do_http() {
  local method=$1
  local url=$2
  local body=$3

  if [[ -n "${body}" ]]; then
    HTTP_BODY="$(curl -sS -X "${method}" -H "Content-Type: application/json" -d "${body}" "${url}" -w '\n%{http_code}')"
  else
    HTTP_BODY="$(curl -sS -X "${method}" "${url}" -w '\n%{http_code}')"
  fi
  HTTP_CODE="${HTTP_BODY##*$'\n'}"
  HTTP_BODY="${HTTP_BODY%$'\n'*}"
}

debug_node_probe_state() {
  local idx=$1
  local label=$2
  local url="${NODE_URLS[$idx]}/internal/replica/state"

  local response
  response="$(curl -sS --max-time 2 -X GET "${url}" -w '\n%{http_code}' 2>/dev/null || true)"
  local code="000"
  local body="${response}"
  if [[ "${response}" == *$'\n'* ]]; then
    code="${response##*$'\n'}"
    body="${response%$'\n'*}"
  fi
  echo "${label} probe status=${code} body=${body}" >&2
}

post_order() {
  local user=$1
  local price=$2
  local amount=$3
  local is_bid=$4

  local payload
  payload=$(cat <<EOF
{"user":"${user}","priceLevel":${price},"amount":${amount},"isBid":${is_bid}}
EOF
)

  do_http POST "${PRIMARY_URL}/orders/post" "${payload}"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo "post_order failed for user=${user} status=${HTTP_CODE} body=${HTTP_BODY}" >&2
    return 1
  fi

  local order_id
  order_id="$(jq -r '.orderId' <<<"${HTTP_BODY}")"
  if [[ -z "${order_id}" || "${order_id}" == "null" ]]; then
    echo "post_order response missing orderId: ${HTTP_BODY}" >&2
    return 1
  fi

  echo "${order_id}"
}

cancel_order() {
  local order_id=$1

  local payload="{\"orderId\":\"${order_id}\"}"
  do_http POST "${PRIMARY_URL}/orders/cancel" "${payload}"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo "cancel_order failed for orderId=${order_id} status=${HTTP_CODE} body=${HTTP_BODY}" >&2
    return 1
  fi

  local cancelled
  cancelled="$(jq -r '.SizeCancelled // .sizeCancelled' <<<"${HTTP_BODY}")"
  echo "${cancelled}"
}

get_orders_for_user() {
  local base_url=$1
  local user=$2

  do_http GET "${base_url}/orders/${user}" ""
  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo "get_orders_for_user failed for user=${user} status=${HTTP_CODE}" >&2
    return 1
  fi
  echo "${HTTP_BODY}"
}

snapshot_for_user() {
  local base_url=$1
  local user=$2
  local body
  if ! body="$(get_orders_for_user "${base_url}" "${user}")"; then
    return 1
  fi
  jq -c '.orders | sort_by(.orderId)' <<<"${body}"
}

ensure_order_count_for_user() {
  local base_url=$1
  local user=$2
  local expected_count=$3

  local body
  if ! body="$(get_orders_for_user "${base_url}" "${user}")"; then
    return 1
  fi

  local actual_count
  actual_count="$(jq -r '.orders | length' <<<"${body}")"
  if [[ "${actual_count}" != "${expected_count}" ]]; then
    echo "Unexpected open order count for user=${user}: got=${actual_count} expected=${expected_count}" >&2
    echo "Response=${body}" >&2
    return 1
  fi
}

ensure_user_has_order() {
  local base_url=$1
  local user=$2
  local expected_order_id=$3

  local body
  if ! body="$(get_orders_for_user "${base_url}" "${user}")"; then
    return 1
  fi
  local matches
  matches="$(jq -r --arg id "${expected_order_id}" '[.orders[] | select(.orderId == $id)] | length' <<<"${body}")"
  if [[ "${matches}" != "1" ]]; then
    echo "Expected orderId=${expected_order_id} for user=${user} not found in open orders" >&2
    echo "Response=${body}" >&2
    return 1
  fi
}

ensure_user_missing_order() {
  local base_url=$1
  local user=$2
  local missing_order_id=$3

  local body
  if ! body="$(get_orders_for_user "${base_url}" "${user}")"; then
    return 1
  fi
  local matches
  matches="$(jq -r --arg id "${missing_order_id}" '[.orders[] | select(.orderId == $id)] | length' <<<"${body}")"
  if [[ "${matches}" != "0" ]]; then
    echo "Expected orderId=${missing_order_id} for user=${user} to be absent" >&2
    echo "Response=${body}" >&2
    return 1
  fi
}

ensure_node_applied_seq() {
  local base_url=$1
  local expected_seq=$2

  do_http GET "${base_url}/internal/replica/state" ""
  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo "ensure_node_applied_seq failed for ${base_url} status=${HTTP_CODE}" >&2
    return 1
  fi
  local applied
  applied="$(jq -r '.appliedSeq' <<<"${HTTP_BODY}")"
  if [[ "${applied}" != "${expected_seq}" ]]; then
    echo "Node ${base_url} expected appliedSeq=${expected_seq}, got=${applied}" >&2
    return 1
  fi
}

compare_user_snapshots_across_nodes() {
  local user=$1
  local reference_url=$2
  local reference_snapshot

  reference_snapshot="$(snapshot_for_user "${reference_url}" "${user}")"
  if [[ -z "${reference_snapshot}" ]]; then
    echo "failed to get reference snapshot for user=${user} from ${reference_url}" >&2
    return 1
  fi

  for idx in "${!NODE_URLS[@]}"; do
    local snapshot
    snapshot="$(snapshot_for_user "${NODE_URLS[$idx]}" "${user}")"
    if [[ "${snapshot}" != "${reference_snapshot}" ]]; then
      echo "Snapshot mismatch for user=${user} on ${NODE_LABELS[$idx]} (${NODE_URLS[$idx]})" >&2
      echo "expected=${reference_snapshot}" >&2
      echo "actual  =${snapshot}" >&2
      return 1
    fi
  done
}

restart_node() {
  local idx=$1
  echo "Restarting ${NODE_LABELS[$idx]} on port ${NODE_PORTS[$idx]}"
  start_node "${idx}" "${NODE_ROLES[$idx]}" "${NODE_PORTS[$idx]}" "${NODE_PEERS[$idx]}" "${PRIMARY_URL}"
}

do_build() {
  echo "Building replicated-clob server..."
  go build -o "${BIN_PATH}" "${ROOT_DIR}/cmd/server"
}

main() {
  do_build
  trap cleanup EXIT

  echo "Starting 1 primary + ${SECONDARY_COUNT} secondaries..."
  start_cluster

  for idx in "${!NODE_URLS[@]}"; do
    wait_for_node_up "${idx}"
  done
  echo "All nodes are ready"

  echo "Running baseline write operations..."
  order_alice=$(post_order alice 100 5 true)
  order_bob=$(post_order bob 102 3 true)
  order_carol=$(post_order carol 210 1 false)
  order_dave=$(post_order dave 215 1 false)
  echo "Placed baseline orders: alice=${order_alice}, bob=${order_bob}, carol=${order_carol}, dave=${order_dave}"

  cancelled_size=$(cancel_order "${order_carol}")
  if [[ "${cancelled_size}" != "1" ]]; then
    echo "Unexpected cancel size from baseline cancel: ${cancelled_size}" >&2
    exit 1
  fi

  ensure_order_count_for_user "${PRIMARY_URL}" alice 1
  ensure_order_count_for_user "${PRIMARY_URL}" bob 1
  ensure_order_count_for_user "${PRIMARY_URL}" dave 1
  ensure_user_missing_order "${PRIMARY_URL}" carol "${order_carol}"
  ensure_user_has_order "${PRIMARY_URL}" alice "${order_alice}"
  ensure_user_has_order "${PRIMARY_URL}" bob "${order_bob}"
  ensure_user_has_order "${PRIMARY_URL}" dave "${order_dave}"

  # baseline expected sequence is 5: 4 posts + 1 cancel
  expected_seq_before_failure=5
  ensure_node_applied_seq "${PRIMARY_URL}" "${expected_seq_before_failure}"
  echo "Baseline operations validated"

  echo
  echo "Killing secondary-1 and validating writes still progress..."
  kill_idx=1
  stop_node "${kill_idx}"
  wait_for_node_down "${kill_idx}"
  echo "Stopped ${NODE_LABELS[$kill_idx]}"

  order_erin=$(post_order erin 130 2 true)
  echo "Posting while degraded: erin=${order_erin}"
  cancelled_erin=$(cancel_order "${order_erin}")
  if [[ "${cancelled_erin}" != "2" ]]; then
    echo "Unexpected cancel size for degraded cancel: ${cancelled_erin}" >&2
    exit 1
  fi

  order_faye=$(post_order faye 131 2 true)
  echo "Posting surviving order while degraded: faye=${order_faye}"

  ensure_order_count_for_user "${PRIMARY_URL}" alice 1
  ensure_order_count_for_user "${PRIMARY_URL}" bob 1
  ensure_order_count_for_user "${PRIMARY_URL}" dave 1
  ensure_order_count_for_user "${PRIMARY_URL}" faye 1
  ensure_user_missing_order "${PRIMARY_URL}" erin "${order_erin}"
  ensure_user_has_order "${PRIMARY_URL}" faye "${order_faye}"
  ensure_user_has_order "${PRIMARY_URL}" bob "${order_bob}"

  # degraded writes should still advance sequence on the primary
  expected_seq_after_failure=8
  ensure_node_applied_seq "${PRIMARY_URL}" "${expected_seq_after_failure}"
  echo "Writes still progressed with one secondary down"

  echo "Restarting secondary-1..."
  restart_node "${kill_idx}"
  wait_for_node_up "${kill_idx}"
  echo "Secondary-1 restarted"

  # force read path on the recovered node to apply missing entries
  # (read repair is invoked from query handlers)
  _="$(get_orders_for_user "${NODE_URLS[${kill_idx}]}" "alice")"
  _="$(get_orders_for_user "${NODE_URLS[${kill_idx}]}" "bob")"
  _="$(get_orders_for_user "${NODE_URLS[${kill_idx}]}" "dave")"
  _="$(get_orders_for_user "${NODE_URLS[${kill_idx}]}" "faye")"

  # ensure recovered node now matches primary across all users and sequence
  for user in alice bob dave faye; do
    compare_user_snapshots_across_nodes "${user}" "${PRIMARY_URL}"
  done

  echo
  echo "Comparing state sequence across nodes..."
  ensure_node_applied_seq "${NODE_URLS[0]}" "${expected_seq_after_failure}"
  for idx in "${!NODE_URLS[@]}"; do
    ensure_node_applied_seq "${NODE_URLS[$idx]}" "${expected_seq_after_failure}"
    if (( idx != 0 )); then
      echo "Node ${NODE_LABELS[$idx]} sequence=${expected_seq_after_failure} at ${NODE_URLS[$idx]}"
    fi
  done

  echo
  echo "Replica failover/integrity test PASSED."
}

main "$@"
