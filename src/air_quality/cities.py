"""City list. The dbt seed `dbt/seeds/cities.csv` is the single source of truth."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class City:
    city_id: str
    name: str
    latitude: float
    longitude: float


def load_cities(path: Path, only: list[str] | None = None) -> list[City]:
    with open(path, newline="", encoding="utf-8") as f:
        cities = [
            City(r["city_id"], r["city"], float(r["latitude"]), float(r["longitude"]))
            for r in csv.DictReader(f)
        ]
    if only:
        unknown = set(only) - {c.city_id for c in cities}
        if unknown:
            raise ValueError(f"Unknown city_id(s): {', '.join(sorted(unknown))}")
        cities = [c for c in cities if c.city_id in only]
    return cities
