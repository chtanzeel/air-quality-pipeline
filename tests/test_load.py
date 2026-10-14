"""Loader against a real Postgres (the <WAREHOUSE_DB>_test scratch database).

Run `docker compose up -d postgres` first. Skipped if Postgres is down, unless
AQ_REQUIRE_POSTGRES=1 (as in CI).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from air_quality import pipeline
from air_quality.cities import City
from air_quality.client import ApiError, ApiResponse
from air_quality.config import PipelineConfig
from air_quality.load import load_file
from air_quality.watermarks import get_watermark
from tests.conftest import FIXTURES, fixture_json

pytestmark = pytest.mark.postgres
UTC = timezone.utc
SEP_A = "lhr_2026-09-01_2026-09-10.json"  # 240 hours
SEP_B = "lhr_2026-09-05_2026-09-10.json"  # 144 hours, all inside SEP_A
FETCHED = "2026-10-14T14:21:00Z"


def count(conn, where: str = "true") -> int:
    return conn.execute(f"select count(*) from raw.air_quality_hourly where {where}").fetchone()[0]


def land_fixture(land, name: str, fetched_at: str = FETCHED):
    return land(None, "lhr", name, fetched_at, raw=(FIXTURES / name).read_bytes())


def test_reloading_the_same_file_changes_nothing(pg_conn, land):
    path = land_fixture(land, SEP_A)
    first = load_file(pg_conn, path, land.landing_dir, "batch-1")
    assert (first.inserted, first.updated, first.unchanged) == (240, 0, 0)
    before = pg_conn.execute(
        "select count(*), max(_loaded_at), count(distinct _batch_id) from raw.air_quality_hourly"
    ).fetchone()

    second = load_file(pg_conn, path, land.landing_dir, "batch-2")
    assert (second.inserted, second.updated, second.unchanged) == (0, 0, 240)
    after = pg_conn.execute(
        "select count(*), max(_loaded_at), count(distinct _batch_id) from raw.air_quality_hourly"
    ).fetchone()
    assert after == before  # same rows, lineage untouched
    assert count(pg_conn, "_batch_id = 'batch-2'") == 0


def test_overlapping_window_inserts_nothing(pg_conn, land):
    load_file(pg_conn, land_fixture(land, SEP_A), land.landing_dir, "a")
    result = load_file(pg_conn, land_fixture(land, SEP_B), land.landing_dir, "b")
    assert (result.inserted, result.updated, result.unchanged) == (0, 0, 144)
    assert count(pg_conn) == 240


def test_revised_value_updates_only_that_row(pg_conn, land):
    load_file(pg_conn, land_fixture(land, SEP_B), land.landing_dir, "original")
    revised = fixture_json(SEP_B)
    revised["hourly"]["pm2_5"][10] += 5.0
    path = land(revised, "lhr", "revised.json", FETCHED)
    result = load_file(pg_conn, path, land.landing_dir, "revision")
    assert (result.inserted, result.updated, result.unchanged) == (0, 1, 143)
    assert count(pg_conn, "_batch_id = 'revision'") == 1
    assert count(pg_conn, "_source_file = 'lhr/revised.json'") == 1


def test_future_hours_use_the_files_fetch_time_not_now(pg_conn, land):
    payload = fixture_json("multi_city_2026-10-12_2026-10-16.json")[0]
    path = land(payload, "lhr", "multi.json", "2026-10-14T14:19:46Z")
    result = load_file(pg_conn, path, land.landing_dir, "b")
    assert result.inserted == 63 and result.dropped_future == 57
    assert count(pg_conn, "measured_at > '2026-10-14T14:00Z'") == 0
    assert get_watermark(pg_conn, "lhr") == datetime(2026, 10, 14, 14, tzinfo=UTC)


def test_watermark_never_moves_backwards(pg_conn, land):
    payload = fixture_json("multi_city_2026-10-12_2026-10-16.json")[0]
    load_file(
        pg_conn, land(payload, "lhr", "oct.json", "2026-10-14T14:19:46Z"), land.landing_dir, "x"
    )
    load_file(pg_conn, land_fixture(land, SEP_A), land.landing_dir, "y")  # older data
    assert get_watermark(pg_conn, "lhr") == datetime(2026, 10, 14, 14, tzinfo=UTC)


def test_empty_file_loads_nothing_and_sets_no_watermark(pg_conn, land):
    path = land_fixture(land, "lhr_2021-01-01_2021-01-02_before_coverage.json")
    result = load_file(pg_conn, path, land.landing_dir, "b")
    assert (result.inserted, result.dropped_empty) == (0, 48)
    assert get_watermark(pg_conn, "lhr") is None


class FixtureClient:
    """Serves the September fixture for every request; counts calls."""

    def __init__(self, fail_with: Exception | None = None):
        self.calls = []
        self.fail_with = fail_with

    def fetch(self, lat, lon, start, end):
        self.calls.append((start, end))
        if self.fail_with:
            raise self.fail_with
        body = (FIXTURES / SEP_A).read_bytes()
        return ApiResponse("https://example.test", {}, 200, body, json.loads(body))


LHR = City("lhr", "Lahore", 31.5204, 74.3587)


def config(tmp_path) -> PipelineConfig:
    return PipelineConfig(landing_dir=tmp_path / "landing", request_sleep_seconds=0)


def run_log(conn, run_id):
    cur = conn.execute("select * from ops.run_log where run_id = %s", (run_id,))
    names = [d.name for d in cur.description]
    return dict(zip(names, cur.fetchone(), strict=True))


def test_run_records_success_in_run_log_and_replay_matches(pg_conn, tmp_path):
    cfg = config(tmp_path)
    summary = pipeline.run(
        pg_conn, FixtureClient(), [LHR], cfg, "backfill", date(2026, 9, 1), date(2026, 9, 10)
    )
    row = run_log(pg_conn, summary.run_id)
    assert row["status"] == "success"
    assert (row["files_written"], row["rows_fetched"], row["rows_inserted"]) == (1, 240, 240)
    assert str(row["window_start"]) == "2026-09-01" and str(row["window_end"]) == "2026-09-10"
    assert row["duration_seconds"] is not None

    files = [p for p in cfg.landing_dir.rglob("*.json") if not p.name.endswith(".params.json")]
    replayed = pipeline.replay(pg_conn, files, cfg.landing_dir)
    assert (replayed.inserted, replayed.updated, replayed.unchanged) == (0, 0, 240)
    assert count(pg_conn) == 240


def test_failed_run_is_logged_with_reason(pg_conn, tmp_path):
    client = FixtureClient(fail_with=ApiError("HTTP 400 from Open-Meteo: Bad Request", 400))
    with pytest.raises(ApiError):
        pipeline.run(pg_conn, client, [LHR], config(tmp_path), "incremental", run_id="r-fail")
    row = run_log(pg_conn, "r-fail")
    assert row["status"] == "failed"
    assert "Bad Request" in row["error"]
    assert row["finished_at"] is not None


def test_incremental_run_requests_watermark_minus_lookback(pg_conn, tmp_path, land):
    payload = fixture_json("multi_city_2026-10-12_2026-10-16.json")[0]
    load_file(
        pg_conn, land(payload, "lhr", "oct.json", "2026-10-14T14:19:46Z"), land.landing_dir, "x"
    )
    client = FixtureClient()
    now = datetime(2026, 10, 15, 1, 0, tzinfo=UTC)
    pipeline.extract_step(pg_conn, client, [LHR], config(tmp_path), "r", now=lambda: now)
    # watermark 2026-10-14T14:00 - 48h = 2026-10-12T14:00 -> request 10-12 .. today (10-15)
    assert client.calls == [(date(2026, 10, 12), date(2026, 10, 15))]


def test_replay_applies_files_in_fetch_order_so_newest_fetch_wins(pg_conn, land):
    older = fixture_json(SEP_B)
    newer = fixture_json(SEP_B)
    newer["hourly"]["pm2_5"][0] = 999.0  # an upstream revision seen by the later fetch
    # Names sort the wrong way round on purpose: path order must not decide.
    new_path = land(newer, "lhr", "a_newer.json", "2026-10-14T12:00:00Z")
    old_path = land(older, "lhr", "b_older.json", "2026-10-13T12:00:00Z")
    pipeline.replay(pg_conn, [new_path, old_path], land.landing_dir)
    value = pg_conn.execute(
        "select pm2_5 from raw.air_quality_hourly where measured_at = '2026-09-05T00:00Z'"
    ).fetchone()[0]
    assert value == 999.0
