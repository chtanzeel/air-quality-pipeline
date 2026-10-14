"""Run orchestration shared by the CLI and the Airflow DAGs.

A run is extract (API -> landing files) then load (landing files -> Postgres). The two steps
are separate functions so Airflow can run them as separate tasks; `run` chains them for the CLI.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg

from air_quality import runlog
from air_quality.cities import City
from air_quality.client import OpenMeteoClient
from air_quality.config import PipelineConfig
from air_quality.extract import extract_city
from air_quality.load import LoadResult, fetched_at_of, load_file
from air_quality.watermarks import get_watermark, request_window

log = logging.getLogger(__name__)


def new_run_id(mode: str) -> str:
    return f"{mode}__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}__{uuid.uuid4().hex[:8]}"


def extract_step(
    conn: psycopg.Connection,
    client: OpenMeteoClient,
    cities: list[City],
    cfg: PipelineConfig,
    run_id: str,
    start: date | None = None,
    end: date | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
) -> list[Path]:
    """Land API responses for each city. With no explicit start/end, use watermark - lookback."""
    paths: list[Path] = []
    for i, city in enumerate(cities):
        if start is not None and end is not None:
            w_start, w_end = start, end
        else:
            w_start, w_end = request_window(
                get_watermark(conn, city.city_id),
                now(),
                cfg.start_date,
                timedelta(hours=cfg.lookback_hours),
            )
        if i and cfg.request_sleep_seconds:
            sleep(cfg.request_sleep_seconds)
        log.info("Extracting %s for %s..%s", city.city_id, w_start, w_end)
        landed = extract_city(
            client,
            city,
            w_start,
            w_end,
            cfg.landing_dir,
            run_id,
            window_days=cfg.window_days,
            request_sleep_seconds=cfg.request_sleep_seconds,
            now=now,
            sleep=sleep,
        )
        runlog.record_extract(conn, run_id, w_start, w_end, len(landed))
        paths.extend(f.path for f in landed)
    return paths


def load_step(
    conn: psycopg.Connection, paths: list[Path], landing_dir: Path, run_id: str
) -> list[LoadResult]:
    """Load files oldest fetch first, so when files overlap the newest fetch wins
    (a replay of all of landing/ ends in the same state as the original runs)."""
    results = []
    for path in sorted(paths, key=lambda p: (fetched_at_of(p), str(p))):
        result = load_file(conn, path, landing_dir, run_id)
        runlog.record_load(conn, run_id, [result])
        results.append(result)
    return results


@dataclass
class RunSummary:
    run_id: str
    files: int
    fetched: int
    inserted: int
    updated: int
    unchanged: int
    dropped_future: int
    dropped_empty: int


def summarise(run_id: str, results: list[LoadResult]) -> RunSummary:
    return RunSummary(
        run_id=run_id,
        files=len(results),
        fetched=sum(r.fetched for r in results),
        inserted=sum(r.inserted for r in results),
        updated=sum(r.updated for r in results),
        unchanged=sum(r.unchanged for r in results),
        dropped_future=sum(r.dropped_future for r in results),
        dropped_empty=sum(r.dropped_empty for r in results),
    )


def run(
    conn: psycopg.Connection,
    client: OpenMeteoClient,
    cities: list[City],
    cfg: PipelineConfig,
    mode: str,
    start: date | None = None,
    end: date | None = None,
    run_id: str | None = None,
) -> RunSummary:
    """Extract then load, recording the run in ops.run_log whether it succeeds or fails."""
    run_id = run_id or new_run_id(mode)
    runlog.start_run(conn, run_id, mode, [c.city_id for c in cities])
    try:
        paths = extract_step(conn, client, cities, cfg, run_id, start, end)
        results = load_step(conn, paths, cfg.landing_dir, run_id)
    except Exception as exc:
        runlog.finish_run(conn, run_id, "failed", f"{type(exc).__name__}: {exc}")
        raise
    runlog.finish_run(conn, run_id, "success")
    return summarise(run_id, results)


def replay(
    conn: psycopg.Connection, paths: list[Path], landing_dir: Path, run_id: str | None = None
) -> RunSummary:
    """Re-load already landed files without calling the API."""
    run_id = run_id or new_run_id("replay")
    city_ids = sorted({p.parent.name for p in paths})
    runlog.start_run(conn, run_id, "replay", city_ids)
    try:
        results = load_step(conn, paths, landing_dir, run_id)
    except Exception as exc:
        runlog.finish_run(conn, run_id, "failed", f"{type(exc).__name__}: {exc}")
        raise
    runlog.finish_run(conn, run_id, "success")
    return summarise(run_id, results)
