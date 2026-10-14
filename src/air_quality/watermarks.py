"""Watermarks: the last loaded hour per city, and the request window derived from it.

Each incremental run asks for [watermark - lookback, today]. Recent hours can be revised
upstream, so the lookback re-fetches them and the upsert applies any changes. The API takes
whole dates, so the window is widened to full days.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import psycopg


def request_window(
    watermark: datetime | None,
    now: datetime,
    start_date: date,
    lookback: timedelta,
) -> tuple[date, date]:
    """Inclusive (start, end) dates to request for one city."""
    today = now.astimezone(timezone.utc).date()
    if watermark is None:
        start = start_date
    else:
        start = max((watermark.astimezone(timezone.utc) - lookback).date(), start_date)
    return min(start, today), today


def get_watermark(conn: psycopg.Connection, city_id: str) -> datetime | None:
    row = conn.execute(
        "select last_loaded_hour from ops.watermarks where city_id = %s", (city_id,)
    ).fetchone()
    return row[0] if row else None


def advance_watermark(conn: psycopg.Connection, city_id: str, hour: datetime) -> None:
    """Move the watermark forward to `hour`; never back (e.g. when backfilling old data)."""
    conn.execute(
        """
        insert into ops.watermarks (city_id, last_loaded_hour) values (%s, %s)
        on conflict (city_id) do update
            set last_loaded_hour = excluded.last_loaded_hour, updated_at = now()
            where excluded.last_loaded_hour > ops.watermarks.last_loaded_hour
        """,
        (city_id, hour),
    )
