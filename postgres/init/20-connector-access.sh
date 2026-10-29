#!/usr/bin/env bash
set -Eeuo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_CONNECTOR_PASSWORD:?POSTGRES_CONNECTOR_PASSWORD is required}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --no-psqlrc \
  --set=ON_ERROR_STOP=1 <<'SQL'
\getenv connector_password POSTGRES_CONNECTOR_PASSWORD

SELECT format(
    'CREATE ROLE %I LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD %L',
    'cdc_connector',
    :'connector_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_roles
    WHERE rolname = 'cdc_connector'
)
\gexec

ALTER ROLE cdc_connector WITH
    LOGIN
    REPLICATION
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    NOBYPASSRLS
    PASSWORD :'connector_password';

SELECT format('REVOKE CREATE ON DATABASE %I FROM cdc_connector', current_database())
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO cdc_connector', current_database())
\gexec

REVOKE ALL PRIVILEGES ON SCHEMA public FROM cdc_connector;
GRANT USAGE ON SCHEMA public TO cdc_connector;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM cdc_connector;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM cdc_connector;
GRANT SELECT ON TABLE
    public.customers,
    public.orders,
    public.payments
TO cdc_connector;

SELECT 'CREATE PUBLICATION postgres_cdc_g001_pub FOR TABLE public.customers, public.orders, public.payments'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_publication
    WHERE pubname = 'postgres_cdc_g001_pub'
)
\gexec
SQL
