"""Runtime configuration, read from environment variables.

Locally, values come from `.env` (loaded by the CLI). In containers, Docker Compose injects them.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name} (see .env.example)")
    return value


@dataclass(frozen=True)
class WarehouseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls, dbname: str | None = None) -> WarehouseConfig:
        return cls(
            host=os.environ.get("WAREHOUSE_HOST", "localhost"),
            port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
            dbname=dbname or _require("WAREHOUSE_DB"),
            user=_require("WAREHOUSE_USER"),
            password=_require("WAREHOUSE_PASSWORD"),
        )


@dataclass(frozen=True)
class PipelineConfig:
    # First hour with CAMS global data on Open-Meteo is 2022-08-04T00:00Z (docs/probe_findings.md).
    start_date: date = date(2022, 8, 4)
    lookback_hours: int = 48
    window_days: int = 31
    request_sleep_seconds: float = 1.0
    landing_dir: Path = REPO_ROOT / "landing"
    cities_file: Path = REPO_ROOT / "dbt" / "seeds" / "cities.csv"

    @classmethod
    def from_env(cls) -> PipelineConfig:
        default = cls()
        env = os.environ.get
        return cls(
            start_date=date.fromisoformat(env("AQ_START_DATE", default.start_date.isoformat())),
            lookback_hours=int(env("AQ_LOOKBACK_HOURS", default.lookback_hours)),
            window_days=int(env("AQ_WINDOW_DAYS", default.window_days)),
            request_sleep_seconds=float(
                env("AQ_REQUEST_SLEEP_SECONDS", default.request_sleep_seconds)
            ),
            landing_dir=Path(env("AQ_LANDING_DIR", default.landing_dir)),
            cities_file=Path(env("AQ_CITIES_FILE", default.cities_file)),
        )
