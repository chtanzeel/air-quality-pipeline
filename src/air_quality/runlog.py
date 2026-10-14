"""ops.run_log: one row per pipeline run, written as the run progresses so failures are kept.

Callers use an autocommit connection, so each update is durable on its own.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import psycopg

from air_quality.load import LoadResult


def start_run(conn: psycopg.Connection, run_id: str, mode: str, city_ids: list[str]) -> None:
    """Create the run row, or reset it if this run_id is being retried."""
    conn.execute(
        """
        insert into ops.run_log (run_id, mode, status, city_ids) values (%s, %s, 'running', %s)
        on conflict (run_id) do update set
            mode = excluded.mode, status = 'running', city_ids = excluded.city_ids,
            window_start = null, window_end = null, files_written = 0,
            rows_fetched = 0, rows_dropped_future = 0, rows_dropped_empty = 0,
            rows_inserted = 0, rows_updated = 0, rows_unchanged = 0,
            started_at = now(), finished_at = null, duration_seconds = null, error = null
        """,
        (run_id, mode, city_ids),
    )


def record_extract(
    conn: psycopg.Connection, run_id: str, window_start: date, window_end: date, files: int
) -> None:
    conn.execute(
        """
        update ops.run_log set
            window_start = least(window_start, %s),
            window_end = greatest(window_end, %s),
            files_written = files_written + %s
        where run_id = %s
        """,
        (window_start, window_end, files, run_id),
    )


def record_load(conn: psycopg.Connection, run_id: str, results: Iterable[LoadResult]) -> None:
    results = list(results)
    conn.execute(
        """
        update ops.run_log set
            rows_fetched = rows_fetched + %s,
            rows_dropped_future = rows_dropped_future + %s,
            rows_dropped_empty = rows_dropped_empty + %s,
            rows_inserted = rows_inserted + %s,
            rows_updated = rows_updated + %s,
            rows_unchanged = rows_unchanged + %s
        where run_id = %s
        """,
        (
            sum(r.fetched for r in results),
            sum(r.dropped_future for r in results),
            sum(r.dropped_empty for r in results),
            sum(r.inserted for r in results),
            sum(r.updated for r in results),
            sum(r.unchanged for r in results),
            run_id,
        ),
    )


def finish_run(
    conn: psycopg.Connection, run_id: str, status: str, error: str | None = None
) -> None:
    conn.execute(
        """
        update ops.run_log set
            status = %s,
            error = %s,
            finished_at = now(),
            duration_seconds = extract(epoch from now() - started_at)
        where run_id = %s
        """,
        (status, error, run_id),
    )
