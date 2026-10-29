#!/usr/bin/env bash
set -Eeuo pipefail

readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
readonly COMPOSE_FILE="$PROJECT_ROOT/compose.yaml"
readonly SECRETS_FILE="$PROJECT_ROOT/.env"
readonly DEFAULT_GENERATION="g001"
readonly COMPOSE_PROJECT="cdc-lab-g001"
readonly CONSUMER_IMAGE="cdc-lab-consumer:g001"

fail() {
  local message="$1"
  local code="${2:-1}"
  printf 'error: %s\n' "$message" >&2
  exit "$code"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1" 69
}

require_no_arguments() {
  (($# == 0)) || fail "this command does not accept arguments" 64
}

validate_generation_name() {
  local generation="$1"
  [[ "$generation" =~ ^g[0-9]{3}$ ]] \
    || fail "generation must match gNNN, got: $generation" 64
}

generation_file() {
  local generation="$1"
  printf '%s/config/generations/%s.env\n' "$PROJECT_ROOT" "$generation"
}

read_env_value() {
  local file="$1"
  local name="$2"
  local -a matches=()
  [[ -f "$file" ]] || fail "required environment file is missing: ${file#"$PROJECT_ROOT/"}" 66
  mapfile -t matches < <(grep -E "^${name}=" "$file" || true)
  ((${#matches[@]} == 1)) \
    || fail "environment file must define $name exactly once: ${file#"$PROJECT_ROOT/"}" 65
  printf '%s\n' "${matches[0]#*=}"
}

require_local_secrets() {
  local name value
  [[ -f "$SECRETS_FILE" ]] \
    || fail "local secrets file .env is missing; copy .env.example and replace placeholders" 66
  for name in POSTGRES_ADMIN_PASSWORD POSTGRES_CONNECTOR_PASSWORD; do
    value="$(read_env_value "$SECRETS_FILE" "$name")"
    [[ -n "$value" && "$value" != "replace-with-a-local-secret" ]] \
      || fail ".env contains an empty or placeholder value for $name" 65
  done
}

require_generation_contract() {
  local generation="$1"
  local file key expected actual
  validate_generation_name "$generation"
  [[ "$generation" == "$DEFAULT_GENERATION" ]] \
    || fail "runtime commands currently support only g001; isolated generation creation is implemented by R-08" 69
  file="$(generation_file "$generation")"

  while IFS='|' read -r key expected; do
    actual="$(read_env_value "$file" "$key")"
    [[ "$actual" == "$expected" ]] \
      || fail "generation metadata mismatch for $key: expected $expected, got $actual" 65
  done <<EOF
CDC_GENERATION_ID|$generation
POSTGRES_DATABASE|cdc_lab
POSTGRES_SCHEMA|public
POSTGRES_CONNECTOR_USER|cdc_connector
DEBEZIUM_CONNECTOR_NAME|postgres-cdc-$generation
POSTGRES_REPLICATION_SLOT|postgres_cdc_${generation}
POSTGRES_PUBLICATION|postgres_cdc_${generation}_pub
KAFKA_TOPIC_PREFIX|cdc.$generation
KAFKA_CUSTOMERS_TOPIC|cdc.$generation.public.customers
KAFKA_ORDERS_TOPIC|cdc.$generation.public.orders
KAFKA_PAYMENTS_TOPIC|cdc.$generation.public.payments
KAFKA_CONSUMER_GROUP|sqlite-projector-$generation
SQLITE_SINK_PATH|/data/cdc-$generation.sqlite3
EOF
}

compose() {
  local generation="${CDC_LAB_GENERATION:-$DEFAULT_GENERATION}"
  local -a validated_env_names=(
    CDC_GENERATION_ID
    POSTGRES_DATABASE
    POSTGRES_SCHEMA
    POSTGRES_CONNECTOR_USER
    DEBEZIUM_CONNECTOR_NAME
    POSTGRES_REPLICATION_SLOT
    POSTGRES_PUBLICATION
    KAFKA_TOPIC_PREFIX
    KAFKA_CUSTOMERS_TOPIC
    KAFKA_ORDERS_TOPIC
    KAFKA_PAYMENTS_TOPIC
    KAFKA_CONSUMER_GROUP
    SQLITE_SINK_PATH
    POSTGRES_ADMIN_PASSWORD
    POSTGRES_CONNECTOR_PASSWORD
  )
  require_generation_contract "$generation"
  require_local_secrets
  (
    unset "${validated_env_names[@]}"
    CDC_GENERATION="$generation" docker compose \
      --project-name "$COMPOSE_PROJECT" \
      --env-file "$SECRETS_FILE" \
      --env-file "$(generation_file "$generation")" \
      --file "$COMPOSE_FILE" \
      "$@"
  )
}

compose_config_check() {
  compose config --quiet
}

container_id() {
  local service="$1"
  compose ps --all --quiet "$service"
}

service_state() {
  local service="$1"
  local id
  id="$(container_id "$service")"
  if [[ -z "$id" ]]; then
    printf 'missing\n'
    return
  fi
  docker inspect --format '{{.State.Status}}' "$id"
}

service_health() {
  local service="$1"
  local id
  id="$(container_id "$service")"
  if [[ -z "$id" ]]; then
    printf 'missing\n'
    return
  fi
  docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$id"
}

require_running_service() {
  local service="$1"
  local state health
  state="$(service_state "$service")"
  [[ "$state" == "running" ]] \
    || fail "service $service is not running (state=$state)" 69
  health="$(service_health "$service")"
  [[ "$health" == "healthy" || "$health" == "none" ]] \
    || fail "service $service is not healthy (health=$health)" 69
}

validate_replication_slot_metric() {
  local slot_line="$1"
  local expected_slot="$2"
  local slot_name wal_status retained_wal_bytes safe_wal_size threshold extra
  [[ -n "$slot_line" && "$slot_line" != *$'\n'* ]] \
    || fail "expected exactly one replication slot metric row for $expected_slot" 69
  IFS='|' read -r slot_name wal_status retained_wal_bytes safe_wal_size threshold extra <<< "$slot_line"
  [[ "$slot_name" == "$expected_slot" && -z "$extra" ]] \
    || fail "replication slot metric row has unexpected shape" 69
  case "$wal_status" in
    reserved|extended|unreserved) ;;
    lost) fail "replication slot $expected_slot is lost; create a new CDC generation" 69 ;;
    *) fail "replication slot $expected_slot has unknown wal_status: $wal_status" 69 ;;
  esac
  [[ "$retained_wal_bytes" =~ ^[0-9]+$ \
     && "$safe_wal_size" =~ ^[0-9]+$ \
     && "$threshold" =~ ^(OK|WARNING)$ ]] \
    || fail "replication slot metric values are invalid" 69
  if ((retained_wal_bytes >= 512 * 1024 * 1024)); then
    [[ "$threshold" == "WARNING" ]] \
      || fail "replication slot warning threshold is inconsistent" 69
  else
    [[ "$threshold" == "OK" ]] \
      || fail "replication slot warning threshold is inconsistent" 69
  fi
  printf '%s\n' "$slot_line"
}

wait_for_service_state() {
  local service="$1"
  local expected="$2"
  local attempts="${3:-30}"
  local state
  for ((attempt = 1; attempt <= attempts; attempt += 1)); do
    state="$(service_state "$service")"
    if [[ "$state" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  fail "service $service did not reach state $expected (last state=$state)" 1
}
