"""Warehouse DDL for the tables the Python pipeline owns. Idempotent; applied before every run.

dbt owns everything downstream of `raw`.
"""

from __future__ import annotations

import psycopg

MEASURES = (
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "carbon_monoxide",
    "dust",
    "us_aqi",
)

DDL = f"""
create schema if not exists raw;
create schema if not exists ops;

create table if not exists raw.air_quality_hourly (
    city_id          text             not null,
    measured_at      timestamptz      not null,
    {", ".join(f"{m} double precision" for m in MEASURES)},
    grid_latitude    double precision not null,
    grid_longitude   double precision not null,
    _source_file     text             not null,
    _loaded_at       timestamptz      not null default now(),
    _batch_id        text             not null,
    primary key (city_id, measured_at)
);
comment on table raw.air_quality_hourly is
    'CAMS global atmospheric composition forecasts (model data, ~45 km grid) via Open-Meteo. '
    'One row per city and UTC hour. Not ground-station measurements.';
comment on column raw.air_quality_hourly.measured_at is
    'UTC hour the model value is valid for. Native model steps are 3-hourly; '
    'other hours are interpolated by the API.';

create table if not exists ops.watermarks (
    city_id          text        primary key,
    last_loaded_hour timestamptz not null,
    updated_at       timestamptz not null default now()
);

create table if not exists ops.run_log (
    run_id             text        primary key,
    mode               text        not null check (mode in ('incremental', 'backfill', 'replay')),
    status             text        not null check (status in ('running', 'success', 'failed')),
    city_ids           text[]      not null,
    window_start       date,
    window_end         date,
    files_written      integer     not null default 0,
    rows_fetched       integer     not null default 0,
    rows_dropped_future integer    not null default 0,
    rows_dropped_empty integer     not null default 0,
    rows_inserted      integer     not null default 0,
    rows_updated       integer     not null default 0,
    rows_unchanged     integer     not null default 0,
    started_at         timestamptz not null default now(),
    finished_at        timestamptz,
    duration_seconds   numeric(10, 2),
    error              text
);
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(DDL)
