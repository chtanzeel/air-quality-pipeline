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
