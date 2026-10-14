"""Retry behaviour of the API client, with a fake HTTP session (no network)."""

from __future__ import annotations

import json
from datetime import date

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from air_quality.client import ApiError, OpenMeteoClient, build_params
from tests.conftest import FIXTURES


def make_response(status: int, body: bytes = b"{}", headers: dict | None = None):
    r = requests.Response()
    r.status_code = status
    r._content = body
    r.headers = CaseInsensitiveDict(headers or {})
    r.url = "https://air-quality-api.open-meteo.com/v1/air-quality?test"
    return r


OK_BODY = (FIXTURES / "lhr_2026-09-05_2026-09-10.json").read_bytes()


class FakeSession:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def client_with(*outcomes):
    session = FakeSession(*outcomes)
    sleeps: list[float] = []
    return OpenMeteoClient(session=session, sleep=sleeps.append), session, sleeps


def fetch(client):
    return client.fetch(31.5204, 74.3587, date(2026, 9, 5), date(2026, 9, 10))


def test_429_then_200_retries_once_with_backoff():
    client, session, sleeps = client_with(make_response(429), make_response(200, OK_BODY))
    response = fetch(client)
    assert len(session.calls) == 2
    assert sleeps == [2.0]
    assert response.body == OK_BODY  # unchanged bytes, for landing
    assert response.payload["hourly"]["time"][0] == "2026-09-05T00:00"


def test_5xx_backoff_is_exponential():
    client, session, sleeps = client_with(
        make_response(503), make_response(500), make_response(502), make_response(200, OK_BODY)
    )
    fetch(client)
    assert sleeps == [2.0, 4.0, 8.0]


def test_retry_after_header_is_respected():
    client, _, sleeps = client_with(
        make_response(429, headers={"Retry-After": "30"}), make_response(200, OK_BODY)
    )
    fetch(client)
    assert sleeps == [30.0]


def test_network_error_is_retried():
    client, session, sleeps = client_with(
        requests.ConnectionError("reset"), make_response(200, OK_BODY)
    )
    fetch(client)
    assert len(session.calls) == 2


def test_400_is_not_retried_and_reason_is_surfaced():
    body = (FIXTURES / "error_400_invalid_variable.json").read_bytes()
    client, session, sleeps = client_with(make_response(400, body))
    with pytest.raises(ApiError) as excinfo:
        fetch(client)
    assert len(session.calls) == 1
    assert sleeps == []
    assert excinfo.value.status == 400
    assert excinfo.value.reason == json.loads(body)["reason"]
    assert "invalid String value pm25" in str(excinfo.value)


def test_gives_up_after_max_attempts():
    client, session, sleeps = client_with(*[make_response(429) for _ in range(5)])
    with pytest.raises(ApiError, match="Gave up after 5 attempts"):
        fetch(client)
    assert len(session.calls) == 5
    assert sleeps == [2.0, 4.0, 8.0, 16.0]


def test_backoff_is_capped():
    client = OpenMeteoClient(session=FakeSession(), max_backoff_seconds=60)
    assert client._backoff(10, None) == 60


def test_error_flag_in_200_body_raises():
    client, _, _ = client_with(make_response(200, b'{"error": true, "reason": "nope"}'))
    with pytest.raises(ApiError, match="nope"):
        fetch(client)


def test_request_params():
    params = build_params(31.5204, 74.3587, date(2026, 9, 5), date(2026, 9, 10))
    assert params["domains"] == "cams_global"
    assert params["timezone"] == "GMT"
    assert params["start_date"] == "2026-09-05" and params["end_date"] == "2026-09-10"
    assert params["hourly"].split(",") == [
        "pm2_5", "pm10", "nitrogen_dioxide", "sulphur_dioxide",
        "ozone", "carbon_monoxide", "dust", "us_aqi",
    ]  # fmt: skip
