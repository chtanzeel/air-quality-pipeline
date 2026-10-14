"""Window chunking and landing files."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from air_quality.cities import City, load_cities
from air_quality.client import ApiResponse
from air_quality.config import PipelineConfig
from air_quality.extract import date_windows, extract_city, landing_path, sidecar_path

LHR = City("lhr", "Lahore", 31.5204, 74.3587)


def test_windows_cover_range_without_gaps_or_overlap():
    start, end = date(2022, 8, 4), date(2026, 10, 14)
    windows = list(date_windows(start, end, 31))
    assert windows[0][0] == start and windows[-1][1] == end
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:], strict=False):
        assert next_start == prev_end + timedelta(days=1)
    assert all((e - s).days + 1 <= 31 for s, e in windows)
    assert sum((e - s).days + 1 for s, e in windows) == (end - start).days + 1


def test_windows_exact_boundaries():
    assert list(date_windows(date(2026, 1, 1), date(2026, 3, 5), 31)) == [
        (date(2026, 1, 1), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 3, 3)),
        (date(2026, 3, 4), date(2026, 3, 5)),
    ]


def test_single_day_and_short_range():
    assert list(date_windows(date(2026, 5, 1), date(2026, 5, 1), 31)) == [
        (date(2026, 5, 1), date(2026, 5, 1))
    ]
    assert list(date_windows(date(2026, 5, 1), date(2026, 5, 3), 31)) == [
        (date(2026, 5, 1), date(2026, 5, 3))
    ]


@pytest.mark.parametrize(
    "start,end,days",
    [
        (date(2026, 5, 2), date(2026, 5, 1), 31),
        (date(2026, 5, 1), date(2026, 5, 2), 0),
    ],
)
def test_invalid_windows_raise(start, end, days):
    with pytest.raises(ValueError):
        list(date_windows(start, end, days))


def test_landing_path_is_windows_safe(tmp_path):
    fetched = datetime(2026, 10, 14, 14, 19, 46, 123456, tzinfo=timezone.utc)
    path = landing_path(tmp_path, "lhr", date(2026, 10, 1), date(2026, 10, 14), fetched)
    assert path == tmp_path / "lhr" / "2026-10-01_2026-10-14_20261014T141946123456Z.json"
    assert ":" not in path.name


class FakeClient:
    def __init__(self):
        self.calls = []

    def fetch(self, lat, lon, start, end):
        self.calls.append((start, end))
        body = json.dumps({"window": [start.isoformat(), end.isoformat()]}).encode()
        params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
        return ApiResponse("https://example.test/aq", params, 200, body, json.loads(body))


def test_extract_city_lands_each_window_unchanged_with_sidecar(tmp_path):
    client, sleeps = FakeClient(), []
    ticks = iter(datetime(2026, 10, 14, 12, 0, s, tzinfo=timezone.utc) for s in range(10))
    landed = extract_city(
        client,
        LHR,
        date(2026, 1, 1),
        date(2026, 3, 5),
        tmp_path,
        run_id="r1",
        window_days=31,
        request_sleep_seconds=1.5,
        now=lambda: next(ticks),
        sleep=sleeps.append,
    )
    assert len(landed) == 3 == len(client.calls)
    assert sleeps == [1.5, 1.5]  # between requests, not after the last
    first = landed[0]
    assert json.loads(first.path.read_bytes()) == {"window": ["2026-01-01", "2026-01-31"]}
    sidecar = json.loads(sidecar_path(first.path).read_text(encoding="utf-8"))
    assert sidecar["city_id"] == "lhr"
    assert sidecar["fetched_at"] == "2026-10-14T12:00:00Z"
    assert sidecar["params"] == {"start_date": "2026-01-01", "end_date": "2026-01-31"}
    assert sidecar["run_id"] == "r1"
    assert not list(tmp_path.rglob("*.tmp"))


def test_landed_files_are_never_overwritten(tmp_path):
    fixed = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)

    def extract(run_id):
        day = date(2026, 1, 1)
        extract_city(
            FakeClient(), LHR, day, day, tmp_path, run_id, now=lambda: fixed, sleep=lambda s: None
        )

    extract("r1")
    with pytest.raises(FileExistsError):
        extract("r2")


def test_cities_seed_has_the_five_cities():
    seed = PipelineConfig().cities_file
    assert [c.city_id for c in load_cities(seed)] == ["lhr", "khi", "isb", "pew", "fsd"]
    assert load_cities(seed, ["pew"])[0].name == "Peshawar"
    with pytest.raises(ValueError):
        load_cities(seed, ["xyz"])
