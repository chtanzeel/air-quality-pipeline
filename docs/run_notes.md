# Run notes

Real runs, recorded as they happened. The README draws its numbers from here.

## 2026-10-14: manual 7-day load (Stage 3)

Local machine (Windows 11, Docker Desktop), CLI against Postgres 17.11 in Compose, live API.

| step | command | files | fetched | inserted | updated | unchanged | dropped future | wall time |
|---|---|---|---|---|---|---|---|---|
| 1. backfill | `aq backfill --start 2026-10-07 --end 2026-10-13` | 5 | 840 | 840 | 0 | 0 | 0 | 6.4 s |
| 2. same window again | same command | 5 | 840 | 0 | 0 | 840 | 0 | 6.5 s |
| 3. incremental | `aq run` at 14:44Z | 5 | 480 | 75 | 0 | 360 | 45 | 4.8 s |
| 4. replay all landed files | `aq replay landing` | 15 | 2,160 | 0 | 0 | 2,115 | 45 | |

- Step 1: 5 cities × 168 hours = 840 rows. Watermarks set to 2026-10-13T23:00Z.
- Step 2: re-running the window gave 0 inserted and 0 updated. Row count stayed at 840.
- Step 3: the window was 2026-10-11..2026-10-14 (watermark minus 48 h, widened to whole days).
  Per city: 72 lookback hours unchanged, 15 new hours (00:00–14:00Z), and 9 hours after the
  fetch time dropped. No upstream revisions turned up in the lookback this time.
- Step 4: after replaying every landed file, the table had 915 rows (5 × 183), 915 distinct
  `(city_id, measured_at)`.
- Wall times come from `ops.run_log.duration_seconds`. Each run includes one 1 s pause per
  request after the first.

## 2026-10-14: dbt (Stage 4)

`aq dbt build` on the 915-row warehouse (dbt Core 1.12.6, dbt-postgres 1.11.0):
**PASS=59 WARN=0 ERROR=0**, made up of 1 seed, 7 models (5 tables, 2 views), 47 data tests and
4 unit tests, in 3.9 s.

Negative checks, run in the `warehouse_test` scratch database:

Starting from the 915 real rows replayed into an empty scratch database:

- Injected a row with `pm2_5 = -3`: it went to `quarantine.quarantine_air_quality_hourly` with
  reason `negative pm2_5`, and the build passed (1 of 916 rows quarantined, below 1%).
- Injected 20 more bad rows (21 of 936 quarantined, 2.2%): `assert_quarantine_share_below_threshold`
  failed and `aq dbt build` exited with 1.
- In an earlier, less clean run, a `pm2_5` of 999 left over from a pytest run was quarantined
  as `pm2_5 exceeds pm10`, which exercised that rule against the database as well.
- Deleted every hour after 2026-09-30, then ran `dbt source freshness`: `ERROR STALE`, exit 1.
  On the real warehouse it passes.

CI rehearsal: replaying `tests/fixtures/landing` (five real 7-day files, 840 rows) into an empty
database, then `dbt build`, also gives PASS=59.

## 2026-10-14: Airflow and the full backfill (Stage 5)

Airflow 3.3.2 (image Python 3.13), LocalExecutor, `AIRFLOW__CORE__PARALLELISM=4`, on Docker
Desktop (WSL2) with 3.8 GiB available to Docker.

- `docker compose up -d --wait` from a built image: all services healthy in 46 s. The UI
  returns HTTP 200 on port 8080.
- Idle memory: API server 279 MiB, scheduler 254 MiB, DAG processor 227 MiB, Postgres 54 MiB
  (about 0.8 GiB total).
- The DAG integrity tests pass in the image (6 tests). They also fail as they should on a
  planted cyclic DAG (`AirflowDagCycleException`) and on a planted import error.

### Full backfill, 2022-08-04 to 2026-10-14 (`air_quality_backfill`, default params)

Started from an empty `raw.air_quality_hourly`.

| metric | value |
|---|---|
| chunks (city × 31-day window) | 250 mapped tasks, at most 3 at once, 0 retries |
| files landed | 250 |
| rows fetched / inserted | 183,960 / **183,920** |
| hours dropped (after fetch time) | 40 |
| hours dropped (all-null) | 0 |
| rows per city | 36,784 (2022-08-04T00:00Z .. 2026-10-14T15:00Z, no gaps) |
| distinct `(city_id, measured_at)` | 183,920 |
| DAG run wall time | **3 min 44 s** (15:35:07 to 15:38:51 UTC) |
| per chunk (extract + load) | 1.6 s average, 2.7 s max |
| dbt build after load | 10.1 s, PASS=59 |
| rows quarantined | 141 (0.077%) |

Quarantined rows:

| reason | rows | when | detail |
|---|---|---|---|
| negative ozone | 131 | 2022-08-04 .. 2022-09-02 | all on interpolated hours (none on 3-hourly native steps); min −12 µg/m³ |
| negative nitrogen_dioxide | 4 | 2022-08-09 .. 2022-09-06 | interpolated hours; min −1.4 µg/m³ |
| pm2_5 exceeds pm10 | 6 | Islamabad, 2023-12-15 00:00–05:00Z | two on native steps; up to 8.4 µg/m³ over |

Negative ozone appears only in Aug–Sep 2022, and only on interpolated hours. From Oct 2022
onwards ozone bottoms out at exactly 0. That fits the probe finding that the API's interpolation
can overshoot past neighbouring native values. The data suggests the earliest archive wasn't
clamped at zero, but this is an inference, not documented.

### Re-running a backfilled window changes nothing

The backfill DAG re-run with `{"start_date": "2024-10-01", "end_date": "2025-01-31"}` (the
2024-25 smog season, all cities):

| | rows | checksum of (city, hour, pm2_5, us_aqi, _batch_id) |
|---|---|---|
| before | 183,920 | `5aa3ad64d1373f56afab57a271675349` |
| after | 183,920 | `5aa3ad64d1373f56afab57a271675349` |

`ops.run_log`: 20 files, 14,760 rows fetched, **0 inserted, 0 updated, 14,760 unchanged**, 25.6 s.

### Daily DAG fetches only lookback plus new hours

`air_quality_daily` triggered at 15:41 UTC, with watermarks at 2026-10-14T15:00Z: window
2026-10-12..2026-10-14 (watermark − 48 h, whole days), 5 files, 360 rows fetched, 40 hours
after the fetch time dropped, 0 inserted, 0 updated, 320 unchanged, 19.5 s. dbt source
freshness PASS; dbt build PASS=59.

Triggered again at 16:02 UTC, after the hour turned: same window, 360 fetched, 35 future
hours dropped, **5 inserted** (the new 16:00Z hour for each city), 0 updated, 320 unchanged,
19.6 s. Watermarks moved to 2026-10-14T16:00Z.

Unpausing the DAG also created one scheduled run (`scheduled__2026-10-14T01:30`). With
`catchup=False`, Airflow still runs the most recent missed interval once. That run found the
same window, with 0 inserted and 320 unchanged.

| run | window | fetched | inserted | updated | unchanged |
|---|---|---|---|---|---|
| scheduled 01:30 interval (run on unpause) | 10-12..10-14 | 360 | 0 | 0 | 320 |
| manual 15:41Z | 10-12..10-14 | 360 | 0 | 0 | 320 |
| manual 16:02Z | 10-12..10-14 | 360 | 5 | 0 | 320 |
