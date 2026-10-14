"""Extract: split a date range into windows, call the API, land each response unchanged.

Landing layout: landing/<city_id>/<start>_<end>_<fetched_at>.json, plus a `.params.json` sidecar
holding the request and fetch time. Files are never overwritten, so landing/ is a replayable
record of every response.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from air_quality.cities import City
from air_quality.client import OpenMeteoClient

log = logging.getLogger(__name__)


def date_windows(start: date, end: date, window_days: int) -> Iterator[tuple[date, date]]:
    """Inclusive [start, end] split into consecutive inclusive windows of at most window_days."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=window_days - 1), end)
        yield current, window_end
        current = window_end + timedelta(days=1)


def sidecar_path(landing_file: Path) -> Path:
    return landing_file.with_suffix(".params.json")


def landing_path(
    landing_dir: Path, city_id: str, start: date, end: date, fetched_at: datetime
) -> Path:
    stamp = fetched_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return landing_dir / city_id / f"{start.isoformat()}_{end.isoformat()}_{stamp}.json"


def _write_new(path: Path, data: bytes) -> None:
    """Write via a temp file and rename (no half-written files), never overwriting."""
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite landed file {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


@dataclass(frozen=True)
class LandedFile:
    city_id: str
    path: Path
    window_start: date
    window_end: date
    fetched_at: datetime


def land_response(
    landing_dir: Path,
    city: City,
    start: date,
    end: date,
    fetched_at: datetime,
    url: str,
    params: dict[str, str],
    status: int,
    body: bytes,
    run_id: str,
) -> LandedFile:
    path = landing_path(landing_dir, city.city_id, start, end, fetched_at)
    sidecar = {
        "url": url,
        "params": params,
        "http_status": status,
        "fetched_at": fetched_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "city_id": city.city_id,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "run_id": run_id,
    }
    # Sidecar first: a data file without its sidecar would be unloadable.
    _write_new(sidecar_path(path), (json.dumps(sidecar, indent=2) + "\n").encode("utf-8"))
    _write_new(path, body)
    return LandedFile(city.city_id, path, start, end, fetched_at)


def extract_city(
    client: OpenMeteoClient,
    city: City,
    start: date,
    end: date,
    landing_dir: Path,
    run_id: str,
    window_days: int = 31,
    request_sleep_seconds: float = 1.0,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
) -> list[LandedFile]:
    landed = []
    for i, (w_start, w_end) in enumerate(date_windows(start, end, window_days)):
        if i and request_sleep_seconds:
            sleep(request_sleep_seconds)
        response = client.fetch(city.latitude, city.longitude, w_start, w_end)
        fetched_at = now()
        landed.append(
            land_response(
                landing_dir,
                city,
                w_start,
                w_end,
                fetched_at,
                response.url,
                response.params,
                response.status,
                response.body,
                run_id,
            )
        )
        log.info("Landed %s %s..%s -> %s", city.city_id, w_start, w_end, landed[-1].path.name)
    return landed
