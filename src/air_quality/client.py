"""Open-Meteo air quality API client: timeouts, and retries with exponential backoff.

Retries 429, 5xx and network errors. Never retries 400 (a bad request will not get better)
and surfaces the API's `reason` instead.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests

from air_quality.schema import MEASURES

log = logging.getLogger(__name__)

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None, reason: str | None = None):
        super().__init__(message)
        self.status = status
        self.reason = reason


@dataclass(frozen=True)
class ApiResponse:
    url: str
    params: dict[str, str]
    status: int
    body: bytes  # exactly as received; written unchanged to landing/
    payload: dict[str, Any]


def build_params(latitude: float, longitude: float, start: date, end: date) -> dict[str, str]:
    return {
        "latitude": f"{latitude:.4f}",
        "longitude": f"{longitude:.4f}",
        "hourly": ",".join(MEASURES),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "timezone": "GMT",
        "domains": "cams_global",
    }


def _error_reason(response: requests.Response) -> str:
    try:
        body = response.json()
        if isinstance(body, dict) and body.get("reason"):
            return str(body["reason"])
    except ValueError:
        pass
    return response.text[:500] or response.reason or "no reason given"


class OpenMeteoClient:
    def __init__(
        self,
        session: requests.Session | None = None,
        base_url: str = BASE_URL,
        timeout: tuple[float, float] = (10, 60),
        max_attempts: int = 5,
        backoff_base_seconds: float = 2.0,
        max_backoff_seconds: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.session = session or requests.Session()
        self.base_url = base_url
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_base_seconds = backoff_base_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.sleep = sleep

    def _backoff(self, attempt: int, response: requests.Response | None) -> float:
        delay = self.backoff_base_seconds * 2 ** (attempt - 1)
        retry_after = response.headers.get("Retry-After") if response is not None else None
        if retry_after and retry_after.isdigit():
            delay = max(delay, float(retry_after))
        return min(delay, self.max_backoff_seconds)

    def fetch(self, latitude: float, longitude: float, start: date, end: date) -> ApiResponse:
        params = build_params(latitude, longitude, start, end)
        last_problem = ""
        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_problem = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code == 200:
                    payload = response.json()
                    if isinstance(payload, dict) and payload.get("error"):
                        raise ApiError(
                            f"API error: {payload.get('reason')}", 200, payload.get("reason")
                        )
                    return ApiResponse(response.url, params, 200, response.content, payload)
                reason = _error_reason(response)
                if response.status_code not in RETRY_STATUSES:
                    raise ApiError(
                        f"HTTP {response.status_code} from Open-Meteo: {reason}",
                        response.status_code,
                        reason,
                    )
                last_problem = f"HTTP {response.status_code}: {reason}"

            if attempt < self.max_attempts:
                delay = self._backoff(attempt, response)
                log.warning(
                    "Attempt %d/%d failed (%s); retrying in %.0fs",
                    attempt,
                    self.max_attempts,
                    last_problem,
                    delay,
                )
                self.sleep(delay)

        raise ApiError(f"Gave up after {self.max_attempts} attempts; last error: {last_problem}")
