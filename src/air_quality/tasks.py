"""Task functions called by the Airflow DAGs. Plain Python, so they're testable without Airflow.

Airflow's own run_id is used as the pipeline run_id, so ops.run_log rows, lineage `_batch_id`s
and Airflow runs line up one to one.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from air_quality import db, pipeline, runlog
from air_quality.cities import load_cities
from air_quality.client import OpenMeteoClient
from air_quality.config import PipelineConfig
from air_quality.extract import date_windows
from air_quality.schema import ensure_schema

log = logging.getLogger(__name__)


def start_run(run_id: str, mode: str, city_ids: list[str] | None = None) -> None:
    cfg = PipelineConfig.from_env()
    cities = load_cities(cfg.cities_file, city_ids)
    with db.connect() as conn:
        ensure_schema(conn)
        runlog.start_run(conn, run_id, mode, [c.city_id for c in cities])


def extract(run_id: str, city_ids: list[str] | None = None) -> list[str]:
    """Incremental extract for each city: watermark - lookback .. today."""
    cfg = PipelineConfig.from_env()
    cities = load_cities(cfg.cities_file, city_ids)
    with db.connect() as conn:
        paths = pipeline.extract_step(conn, OpenMeteoClient(), cities, cfg, run_id)
    return [str(p) for p in paths]


def load(run_id: str, paths: list[str]) -> dict[str, Any]:
    cfg = PipelineConfig.from_env()
    with db.connect() as conn:
        results = pipeline.load_step(conn, [Path(p) for p in paths], cfg.landing_dir, run_id)
    summary = vars(pipeline.summarise(run_id, results))
    log.info("Load summary: %s", summary)
    return summary


def plan_backfill(
    start: date, end: date, city_ids: list[str], window_days: int
) -> list[dict[str, str]]:
    """One chunk per city and date window: each becomes its own retryable Airflow task."""
    return [
        {"city_id": city_id, "start": w_start.isoformat(), "end": w_end.isoformat()}
        for city_id in city_ids
        for w_start, w_end in date_windows(start, end, window_days)
    ]


def ingest_chunk(run_id: str, chunk: dict[str, str]) -> dict[str, Any]:
    """Extract one city/window and load it straight away. Idempotent: safe to retry or re-run."""
    cfg = PipelineConfig.from_env()
    cities = load_cities(cfg.cities_file, [chunk["city_id"]])
    start, end = date.fromisoformat(chunk["start"]), date.fromisoformat(chunk["end"])
    with db.connect() as conn:
        paths = pipeline.extract_step(conn, OpenMeteoClient(), cities, cfg, run_id, start, end)
        results = pipeline.load_step(conn, paths, cfg.landing_dir, run_id)
    return {**chunk, **vars(pipeline.summarise(run_id, results))}


def finish(run_id: str) -> dict[str, Any]:
    """Mark the run successful and report data quality: rows, freshness and quarantine."""
    with db.connect() as conn:
        runlog.finish_run(conn, run_id, "success")
        cur = conn.execute("select * from ops.run_log where run_id = %s", (run_id,))
        names = [d.name for d in cur.description]
        run = dict(zip(names, cur.fetchone(), strict=True))
        per_city = conn.execute(
            """
            select city_id, count(*), max(measured_at)
            from raw.air_quality_hourly group by city_id order by city_id
            """
        ).fetchall()
        quarantined = None
        if conn.execute(
            "select to_regclass('quarantine.quarantine_air_quality_hourly')"
        ).fetchone()[0]:
            quarantined = conn.execute(
                "select count(*) from quarantine.quarantine_air_quality_hourly"
            ).fetchone()[0]

    now = datetime.now(timezone.utc)
    total = sum(n for _, n, _ in per_city)
    summary = {
        "run_id": run_id,
        "mode": run["mode"],
        "window": f"{run['window_start']}..{run['window_end']}",
        "files_written": run["files_written"],
        "rows_fetched": run["rows_fetched"],
        "rows_inserted": run["rows_inserted"],
        "rows_updated": run["rows_updated"],
        "rows_unchanged": run["rows_unchanged"],
        "duration_seconds": float(run["duration_seconds"] or 0),
        "total_rows": total,
        "rows_quarantined": quarantined,
        "quarantine_share": round(quarantined / total, 6)
        if quarantined is not None and total
        else None,
        "hours_behind_per_city": {
            city: round((now - latest).total_seconds() / 3600, 1) for city, _, latest in per_city
        },
    }
    log.info("Quality summary: %s", summary)
    return summary


def on_task_failure(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback: record the failure in ops.run_log. Never raises."""
    run_id = context.get("run_id")
    ti = context.get("task_instance")
    error = f"{getattr(ti, 'task_id', '?')}: {context.get('exception')!r}"
    try:
        with db.connect() as conn:
            runlog.finish_run(conn, run_id, "failed", error[:2000])
    except Exception:
        log.exception("Could not record failure of %s in ops.run_log", run_id)
