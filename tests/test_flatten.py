"""Flattening API payloads into hourly rows: future hours, empty hours, schema checks."""

from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest

from air_quality.load import ROW_COLUMNS, SchemaDriftError, flatten
from tests.conftest import fixture_json

UTC = timezone.utc
FETCHED = datetime(2026, 10, 14, 14, 19, 46, tzinfo=UTC)
col = ROW_COLUMNS.index


def lahore_from_multi_city():
    return fixture_json("multi_city_2026-10-12_2026-10-16.json")[0]


def test_drops_hours_after_fetch_time():
    flat = flatten(lahore_from_multi_city(), "lhr", FETCHED)
    assert flat.fetched == 120
    assert flat.dropped_future == 57
    assert len(flat.rows) == 63
    assert flat.max_hour == datetime(2026, 10, 14, 14, tzinfo=UTC)  # current hour kept


def test_rows_are_utc_and_carry_grid_cell():
    row = flatten(lahore_from_multi_city(), "lhr", FETCHED).rows[0]
    assert row[col("city_id")] == "lhr"
    assert row[col("measured_at")] == datetime(2026, 10, 12, 0, tzinfo=UTC)
    assert row[col("pm2_5")] == 42.5
    assert (row[col("grid_latitude")], row[col("grid_longitude")]) == (31.5, 74.40001)


def test_drops_all_null_hours_but_keeps_partly_null_ones():
    flat = flatten(fixture_json("lhr_2022-07-25_2022-08-05_data_start.json"), "lhr", FETCHED)
    assert flat.fetched == 288
    assert flat.dropped_empty == 240  # before 2022-08-04
    assert len(flat.rows) == 48
    first = flat.rows[0]
    assert first[col("measured_at")] == datetime(2022, 8, 4, tzinfo=UTC)
    assert first[col("pm2_5")] is not None
    assert first[col("us_aqi")] is None  # needs 24h of history: starts 2022-08-05


def test_before_coverage_yields_no_rows():
    flat = flatten(fixture_json("lhr_2021-01-01_2021-01-02_before_coverage.json"), "lhr", FETCHED)
    assert flat.rows == [] and flat.dropped_empty == 48 and flat.max_hour is None


def test_unit_change_is_schema_drift():
    payload = copy.deepcopy(lahore_from_multi_city())
    payload["hourly_units"]["carbon_monoxide"] = "ppm"
    with pytest.raises(SchemaDriftError, match="carbon_monoxide"):
        flatten(payload, "lhr", FETCHED)


def test_multi_location_list_is_rejected():
    with pytest.raises(SchemaDriftError):
        flatten(fixture_json("multi_city_2026-10-12_2026-10-16.json"), "lhr", FETCHED)


def test_non_utc_response_is_rejected():
    payload = copy.deepcopy(lahore_from_multi_city())
    payload["utc_offset_seconds"] = 18000
    with pytest.raises(SchemaDriftError):
        flatten(payload, "lhr", FETCHED)
