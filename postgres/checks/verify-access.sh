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

if printf '%s\n' \
    '\set VERBOSITY verbose' \
    "UPDATE public.customers SET full_name = full_name WHERE id = 1;" \
  | PGPASSWORD=$POSTGRES_CONNECTOR_PASSWORD psql \
      "${psql_base[@]}" \
      --username "$POSTGRES_CONNECTOR_USER" \
      > /dev/null 2>"$error_file"; then
  printf 'connector UPDATE unexpectedly succeeded\n' >&2
  exit 1
fi

if ! grep -Fq '42501' "$error_file"; then
  printf 'connector UPDATE failed, but not with insufficient_privilege (SQLSTATE 42501)\n' >&2
  cat "$error_file" >&2
  exit 1
fi

printf 'bootstrap catalog, seed, connector SELECT, and denied DML checks passed\n'
