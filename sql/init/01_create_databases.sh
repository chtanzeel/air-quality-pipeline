#!/bin/bash
# Runs once, on first start of an empty Postgres volume (docker-entrypoint-initdb.d).
# Creates, on one server: the warehouse (pipeline data), <warehouse>_test (scratch database for
# the loader tests) and the Airflow metadata database. The warehouse and Airflow databases have
# separate owner roles, so neither can touch the other.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v wh_db="$WAREHOUSE_DB" -v wh_test_db="${WAREHOUSE_DB}_test" -v wh_user="$WAREHOUSE_USER" -v wh_pw="$WAREHOUSE_PASSWORD" \
  -v af_db="$AIRFLOW_DB" -v af_user="$AIRFLOW_DB_USER" -v af_pw="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
CREATE ROLE :"wh_user" LOGIN PASSWORD :'wh_pw';
CREATE DATABASE :"wh_db" OWNER :"wh_user";
REVOKE ALL ON DATABASE :"wh_db" FROM PUBLIC;

-- Scratch database for the loader tests, so they never touch real data.
CREATE DATABASE :"wh_test_db" OWNER :"wh_user";
REVOKE ALL ON DATABASE :"wh_test_db" FROM PUBLIC;

CREATE ROLE :"af_user" LOGIN PASSWORD :'af_pw';
CREATE DATABASE :"af_db" OWNER :"af_user";
REVOKE ALL ON DATABASE :"af_db" FROM PUBLIC;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$WAREHOUSE_DB" \
  -v wh_user="$WAREHOUSE_USER" <<'EOSQL'
-- raw: flattened API data as loaded. ops: watermarks and run log.
-- dbt creates its own schemas (staging, marts) as the warehouse owner.
CREATE SCHEMA raw AUTHORIZATION :"wh_user";
CREATE SCHEMA ops AUTHORIZATION :"wh_user";
EOSQL
