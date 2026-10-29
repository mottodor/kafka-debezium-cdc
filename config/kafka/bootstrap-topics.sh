#!/usr/bin/env bash
set -Eeuo pipefail

readonly bootstrap_server="${KAFKA_BOOTSTRAP_SERVER:-127.0.0.1:9092}"
readonly kafka_bin_dir="${KAFKA_BIN_DIR:-/kafka/bin}"
readonly retention_ms="86400000"
readonly retention_bytes="268435456"
readonly segment_bytes="67108864"
readonly cleanup_policy="delete"

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf 'required environment variable is unset: %s\n' "$name" >&2
    exit 64
  fi
}

for name in \
  KAFKA_TOPIC_PREFIX \
  KAFKA_CUSTOMERS_TOPIC \
  KAFKA_ORDERS_TOPIC \
  KAFKA_PAYMENTS_TOPIC; do
  require_env "$name"
done

readonly expected_customers_topic="${KAFKA_TOPIC_PREFIX}.public.customers"
readonly expected_orders_topic="${KAFKA_TOPIC_PREFIX}.public.orders"
readonly expected_payments_topic="${KAFKA_TOPIC_PREFIX}.public.payments"

for topic_contract in \
  "KAFKA_CUSTOMERS_TOPIC=$expected_customers_topic" \
  "KAFKA_ORDERS_TOPIC=$expected_orders_topic" \
  "KAFKA_PAYMENTS_TOPIC=$expected_payments_topic"; do
  name="${topic_contract%%=*}"
  expected="${topic_contract#*=}"
  if [[ "${!name}" != "$expected" ]]; then
    printf 'topic variable %s must equal %s, got %s\n' "$name" "$expected" "${!name}" >&2
    exit 65
  fi
done

if [[ "$KAFKA_CUSTOMERS_TOPIC" == "$KAFKA_ORDERS_TOPIC" \
  || "$KAFKA_CUSTOMERS_TOPIC" == "$KAFKA_PAYMENTS_TOPIC" \
  || "$KAFKA_ORDERS_TOPIC" == "$KAFKA_PAYMENTS_TOPIC" ]]; then
  printf 'table topic names must be unique\n' >&2
  exit 65
fi

readonly transaction_topic="${KAFKA_TOPIC_PREFIX}.transaction"
readonly -a table_topics=(
  "$KAFKA_CUSTOMERS_TOPIC"
  "$KAFKA_ORDERS_TOPIC"
  "$KAFKA_PAYMENTS_TOPIC"
)

create_topic() {
  local topic="$1"
  local partitions="$2"

  "$kafka_bin_dir/kafka-topics.sh" \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "cleanup.policy=$cleanup_policy" \
    --config "retention.ms=$retention_ms" \
    --config "retention.bytes=$retention_bytes" \
    --config "segment.bytes=$segment_bytes"
}

verify_topic() {
  local topic="$1"
  local expected_partitions="$2"
  local summary configs pair key value
  declare -A observed=()

  summary="$(
    "$kafka_bin_dir/kafka-topics.sh" \
      --bootstrap-server "$bootstrap_server" \
      --describe \
      --topic "$topic" \
      | head -n 1
  )"

  if [[ ! "$summary" =~ PartitionCount:[[:space:]]*${expected_partitions}([^0-9]|$) ]]; then
    printf 'topic %s has unexpected partition count: %s\n' "$topic" "$summary" >&2
    return 1
  fi
  if [[ ! "$summary" =~ ReplicationFactor:[[:space:]]*1([^0-9]|$) ]]; then
    printf 'topic %s has unexpected replication factor: %s\n' "$topic" "$summary" >&2
    return 1
  fi

  configs="${summary#*Configs:}"
  IFS=',' read -r -a config_pairs <<< "$configs"
  for pair in "${config_pairs[@]}"; do
    pair="${pair#${pair%%[![:space:]]*}}"
    pair="${pair%${pair##*[![:space:]]}}"
    key="${pair%%=*}"
    value="${pair#*=}"
    observed["$key"]="$value"
  done

  for pair in \
    "cleanup.policy=$cleanup_policy" \
    "retention.ms=$retention_ms" \
    "retention.bytes=$retention_bytes" \
    "segment.bytes=$segment_bytes"; do
    key="${pair%%=*}"
    value="${pair#*=}"
    if [[ "${observed[$key]:-}" != "$value" ]]; then
      printf 'topic %s config mismatch: expected %s=%s, got %s\n' \
        "$topic" "$key" "$value" "${observed[$key]:-unset}" >&2
      return 1
    fi
  done

  printf 'verified topic=%s partitions=%s replication_factor=1 retention_ms=%s retention_bytes=%s segment_bytes=%s cleanup_policy=%s\n' \
    "$topic" "$expected_partitions" "$retention_ms" "$retention_bytes" "$segment_bytes" "$cleanup_policy"
}

for topic in "${table_topics[@]}"; do
  create_topic "$topic" 3
done

# Debezium writes transaction boundary records here when transaction metadata is enabled.
# It is generation-scoped and explicit because broker and worker topic auto-creation are disabled.
create_topic "$transaction_topic" 1

for topic in "${table_topics[@]}"; do
  verify_topic "$topic" 3
done
verify_topic "$transaction_topic" 1
