"""Load: flatten a landed response into hourly rows and upsert them into raw.air_quality_hourly.

Rules applied while flattening (see docs/probe_findings.md):
- Hours after the fetch time are forecasts, not yet valid: dropped. The cutoff is the file's own
  fetch time from its sidecar, not "now", so replaying an old file never loads its old forecasts.
- Hours where every measure is null (before coverage starts, or upstream gaps) are dropped.

The upsert only rewrites a row when a value actually changed, so re-loading a file reports every
row as unchanged and leaves lineage columns alone.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from air_quality.extract import sidecar_path
from air_quality.schema import MEASURES
from air_quality.watermarks import advance_watermark

log = logging.getLogger(__name__)

EXPECTED_UNITS = {m: "μg/m³" for m in MEASURES if m != "us_aqi"} | {
    "time": "iso8601",
    "us_aqi": "USAQI",
}
ROW_COLUMNS = ("city_id", "measured_at", *MEASURES, "grid_latitude", "grid_longitude")


class SchemaDriftError(ValueError):
    """The API response no longer has the shape or units this pipeline was built against."""


@dataclass
class Flattened:
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    fetched: int = 0
    dropped_future: int = 0
    dropped_empty: int = 0

    @property
    def max_hour(self) -> datetime | None:
        return max((r[1] for r in self.rows), default=None)


def floor_hour(ts: datetime) -> datetime:
    return ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def flatten(payload: dict[str, Any], city_id: str, fetched_at: datetime) -> Flattened:
    if not isinstance(payload, dict) or "hourly" not in payload:
        raise SchemaDriftError("Expected a single-location response object with an 'hourly' key")
    if payload.get("utc_offset_seconds") != 0:
        raise SchemaDriftError(
            f"Expected UTC times, got offset {payload.get('utc_offset_seconds')}"
        )
    units = payload.get("hourly_units", {})
    wrong = {k: units.get(k) for k, v in EXPECTED_UNITS.items() if units.get(k) != v}
    if wrong:
        raise SchemaDriftError(f"Unexpected or missing units: {wrong}")

    hourly = payload["hourly"]
    times = hourly["time"]
    columns = [hourly[m] for m in MEASURES]
    if any(len(c) != len(times) for c in columns):
        raise SchemaDriftError("Hourly arrays have different lengths")

    cutoff = floor_hour(fetched_at)
    grid_lat, grid_lon = float(payload["latitude"]), float(payload["longitude"])
    out = Flattened(fetched=len(times))
    for i, t in enumerate(times):
        measured_at = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        values = [c[i] for c in columns]
        if measured_at > cutoff:
            out.dropped_future += 1
        elif all(v is None for v in values):
            out.dropped_empty += 1
        else:
            out.rows.append((city_id, measured_at, *values, grid_lat, grid_lon))
    return out


@dataclass
class LoadResult:
    source_file: str
    city_id: str
    fetched: int
    dropped_future: int
    dropped_empty: int
    inserted: int
    updated: int
    unchanged: int
    max_hour: datetime | None


_VALUE_COLUMNS = (*MEASURES, "grid_latitude", "grid_longitude")
_UPSERT = f"""
insert into raw.air_quality_hourly as t ({", ".join(ROW_COLUMNS)}, _source_file, _batch_id)
select {", ".join(ROW_COLUMNS)}, _source_file, _batch_id from _stage
on conflict (city_id, measured_at) do update set
    {", ".join(f"{c} = excluded.{c}" for c in _VALUE_COLUMNS)},
    _source_file = excluded._source_file,
    _loaded_at = now(),
    _batch_id = excluded._batch_id
where ({", ".join(f"t.{c}" for c in _VALUE_COLUMNS)})
    is distinct from ({", ".join(f"excluded.{c}" for c in _VALUE_COLUMNS)})
returning (xmax = 0) as inserted
"""
# `xmax = 0` is true for freshly inserted rows and false for rows rewritten by DO UPDATE.
# Rows skipped by the WHERE clause return nothing: those are the unchanged ones.


def upsert_rows(
    conn: psycopg.Connection, rows: list[tuple[Any, ...]], source_file: str, batch_id: str
) -> tuple[int, int]:
    """Upsert rows inside the caller's transaction. Returns (inserted, updated)."""
    if not rows:
        return 0, 0
    conn.execute(
        "create temp table _stage (like raw.air_quality_hourly including defaults) on commit drop"
    )
    with conn.cursor().copy(
        f"copy _stage ({', '.join(ROW_COLUMNS)}, _source_file, _batch_id) from stdin"
    ) as copy:
        for row in rows:
            copy.write_row((*row, source_file, batch_id))
    flags = [r[0] for r in conn.execute(_UPSERT).fetchall()]
    inserted = sum(flags)
    return inserted, len(flags) - inserted


def read_landed(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = json.loads(path.read_bytes())
    sidecar = json.loads(sidecar_path(path).read_text(encoding="utf-8"))
    return payload, sidecar


def fetched_at_of(path: Path) -> datetime:
    sidecar = json.loads(sidecar_path(path).read_text(encoding="utf-8"))
    return datetime.fromisoformat(sidecar["fetched_at"].replace("Z", "+00:00"))


def load_file(conn: psycopg.Connection, path: Path, landing_dir: Path, batch_id: str) -> LoadResult:
    """Load one landed file and advance its city's watermark, atomically."""
    payload, sidecar = read_landed(path)
    city_id = sidecar["city_id"]
    fetched_at = fetched_at_of(path)
    flat = flatten(payload, city_id, fetched_at)
    # Relative to landing/, so lineage reads the same on the host and in containers.
    source_file = path.resolve().relative_to(landing_dir.resolve()).as_posix()

    with conn.transaction():
        inserted, updated = upsert_rows(conn, flat.rows, source_file, batch_id)
        if flat.max_hour is not None:
            advance_watermark(conn, city_id, flat.max_hour)

    result = LoadResult(
        source_file=source_file,
        city_id=city_id,
        fetched=flat.fetched,
        dropped_future=flat.dropped_future,
        dropped_empty=flat.dropped_empty,
        inserted=inserted,
        updated=updated,
        unchanged=len(flat.rows) - inserted - updated,
        max_hour=flat.max_hour,
    )
    log.info(
        "Loaded %s: %d rows (%d inserted, %d updated, %d unchanged), dropped %d future, %d empty",
        source_file,
        len(flat.rows),
        inserted,
        updated,
        result.unchanged,
        flat.dropped_future,
        flat.dropped_empty,
    )
    return result
