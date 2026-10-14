from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

from air_quality import db
from air_quality.config import WarehouseConfig
from air_quality.extract import sidecar_path
from air_quality.schema import ensure_schema

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_json(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def connect_or_skip(dbname: str | None = None) -> psycopg.Connection:
    """Connect, or skip the test when Postgres isn't running. CI sets AQ_REQUIRE_POSTGRES=1
    so a missing database fails the build instead of silently skipping."""
    load_dotenv()
    try:
        return db.connect(WarehouseConfig.from_env(dbname=dbname))
    except (psycopg.OperationalError, RuntimeError) as exc:
        if os.environ.get("AQ_REQUIRE_POSTGRES") == "1":
            raise
        pytest.skip(f"Postgres not available ({exc}); run: docker compose up -d postgres")


@pytest.fixture
def pg_conn():
    """A clean scratch database (<WAREHOUSE_DB>_test); never the real warehouse."""
    load_dotenv()
    test_db = os.environ.get("WAREHOUSE_TEST_DB") or f"{os.environ.get('WAREHOUSE_DB')}_test"
    conn = connect_or_skip(test_db)
    ensure_schema(conn)
    conn.execute("truncate raw.air_quality_hourly, ops.watermarks, ops.run_log")
    yield conn
    conn.close()


@pytest.fixture
def land(tmp_path):
    """Write a payload into a temporary landing dir with a pipeline-format sidecar."""
    landing_dir = tmp_path / "landing"

    def _land(payload, city_id: str, name: str, fetched_at: str, raw: bytes | None = None) -> Path:
        path = landing_dir / city_id / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw if raw is not None else json.dumps(payload).encode("utf-8"))
        datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))  # validate
        sidecar_path(path).write_text(
            json.dumps({"city_id": city_id, "fetched_at": fetched_at, "run_id": "test"}),
            encoding="utf-8",
        )
        return path

    _land.landing_dir = landing_dir
    return _land
