#!/usr/bin/env bash
set -Eeuo pipefail

POSTGRES_ADMIN_PASSWORD=${POSTGRES_ADMIN_PASSWORD:-${POSTGRES_PASSWORD:-}}
: "${POSTGRES_ADMIN_PASSWORD:?POSTGRES_ADMIN_PASSWORD or POSTGRES_PASSWORD is required}"
: "${POSTGRES_CONNECTOR_PASSWORD:?POSTGRES_CONNECTOR_PASSWORD is required}"

POSTGRES_HOST=${POSTGRES_HOST:-127.0.0.1}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_DATABASE=${POSTGRES_DATABASE:-cdc_lab}
POSTGRES_ADMIN_USER=${POSTGRES_ADMIN_USER:-cdc_admin}
POSTGRES_CONNECTOR_USER=${POSTGRES_CONNECTOR_USER:-cdc_connector}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
error_file=$(mktemp)
trap 'rm -f "$error_file"' EXIT

expect_sqlstate() {
  local expected_sqlstate=$1
  local description=$2
  local username=$3
  local password=$4
  local statement=$5

  : >"$error_file"
  if printf '%s\n' \
      '\set VERBOSITY verbose' \
      "$statement" \
    | PGPASSWORD=$password psql \
        "${psql_base[@]}" \
        --username "$username" \
        > /dev/null 2>"$error_file"; then
    printf '%s unexpectedly succeeded\n' "$description" >&2
    exit 1
  fi

  if ! grep -Fq "$expected_sqlstate" "$error_file"; then
    printf '%s failed, but not with expected SQLSTATE %s\n' \
      "$description" \
      "$expected_sqlstate" >&2
    cat "$error_file" >&2
    exit 1
  fi
}

expect_identity_trigger_sqlstate() {
  local table_name=$1
  local row_id=$2
  local sequence_name=$3
  local sequence_state
  local sequence_last_value
  local sequence_is_called
  local probe_status

  if [[ ! $table_name =~ ^[a-z_]+$ \
      || ! $sequence_name =~ ^[a-z_]+$ \
      || ! $row_id =~ ^[0-9]+$ ]]; then
    printf 'invalid immutable-ID probe arguments\n' >&2
    exit 64
  fi

  sequence_state=$(
    PGPASSWORD=$POSTGRES_ADMIN_PASSWORD psql \
      "${psql_base[@]}" \
      --username "$POSTGRES_ADMIN_USER" \
      --tuples-only \
      --no-align \
      --command "SELECT last_value || '|' || is_called FROM public.${sequence_name}"
  )
  if [[ ! $sequence_state =~ ^([0-9]+)\|(true|false)$ ]]; then
    printf 'unexpected sequence state for public.%s\n' "$sequence_name" >&2
    exit 1
  fi
  sequence_last_value=${BASH_REMATCH[1]}
  sequence_is_called=${BASH_REMATCH[2]}

  : >"$error_file"
  set +e
  printf '%s\n' \
      '\set VERBOSITY verbose' \
      'BEGIN;' \
      "UPDATE public.${table_name} SET id = DEFAULT WHERE id = ${row_id};" \
      'ROLLBACK;' \
    | PGPASSWORD=$POSTGRES_ADMIN_PASSWORD psql \
        "${psql_base[@]}" \
        --username "$POSTGRES_ADMIN_USER" \
        > /dev/null 2>"$error_file"
  probe_status=$?
  set -e

  PGPASSWORD=$POSTGRES_ADMIN_PASSWORD psql \
    "${psql_base[@]}" \
    --username "$POSTGRES_ADMIN_USER" \
    --command "SELECT pg_catalog.setval('public.${sequence_name}'::regclass, ${sequence_last_value}, ${sequence_is_called});" \
    > /dev/null

  if [[ $probe_status -eq 0 ]]; then
    printf '%s immutable-ID trigger probe unexpectedly succeeded\n' "$table_name" >&2
    exit 1
  fi

  if ! grep -Fq '23000' "$error_file"; then
    printf '%s immutable-ID trigger probe failed, but not with expected SQLSTATE 23000\n' \
      "$table_name" >&2
    cat "$error_file" >&2
    exit 1
  fi
}

psql_base=(
  --host "$POSTGRES_HOST"
  --port "$POSTGRES_PORT"
  --dbname "$POSTGRES_DATABASE"
  --no-psqlrc
  --set=ON_ERROR_STOP=1
)

PGPASSWORD=$POSTGRES_ADMIN_PASSWORD psql \
  "${psql_base[@]}" \
  --username "$POSTGRES_ADMIN_USER" \
  --file "$script_dir/../init/30-verify-bootstrap.sql"

seed_counts=$(
  PGPASSWORD=$POSTGRES_CONNECTOR_PASSWORD psql \
    "${psql_base[@]}" \
    --username "$POSTGRES_CONNECTOR_USER" \
    --tuples-only \
    --no-align \
    --command "SELECT (SELECT count(*) FROM public.customers) || '|' || (SELECT count(*) FROM public.orders) || '|' || (SELECT count(*) FROM public.payments)"
)

if [[ $seed_counts != '2|2|1' ]]; then
  printf 'connector SELECT verification returned unexpected counts: %s\n' "$seed_counts" >&2
  exit 1
fi

expect_sqlstate \
  '428C9' \
  'customers primary-key update' \
  "$POSTGRES_ADMIN_USER" \
  "$POSTGRES_ADMIN_PASSWORD" \
  'UPDATE public.customers SET id = 9001 WHERE id = 1;'
expect_sqlstate \
  '428C9' \
  'orders primary-key update' \
  "$POSTGRES_ADMIN_USER" \
  "$POSTGRES_ADMIN_PASSWORD" \
  'UPDATE public.orders SET id = 9001 WHERE id = 101;'
expect_sqlstate \
  '428C9' \
  'payments primary-key update' \
  "$POSTGRES_ADMIN_USER" \
  "$POSTGRES_ADMIN_PASSWORD" \
  'UPDATE public.payments SET id = 9001 WHERE id = 1001;'
expect_identity_trigger_sqlstate customers 1 customers_id_seq
expect_identity_trigger_sqlstate orders 101 orders_id_seq
expect_identity_trigger_sqlstate payments 1001 payments_id_seq
expect_sqlstate \
  '42501' \
  'connector UPDATE' \
  "$POSTGRES_CONNECTOR_USER" \
  "$POSTGRES_CONNECTOR_PASSWORD" \
  'UPDATE public.customers SET full_name = full_name WHERE id = 1;'

PGPASSWORD=$POSTGRES_ADMIN_PASSWORD psql \
  "${psql_base[@]}" \
  --username "$POSTGRES_ADMIN_USER" \
  --file "$script_dir/../init/30-verify-bootstrap.sql"

printf 'source schema, seed, immutable IDs/triggers, publication, and connector privilege checks passed\n'
