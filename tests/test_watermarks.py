"""Request window from watermark and lookback."""

from datetime import date, datetime, timedelta, timezone

from air_quality.watermarks import request_window

UTC = timezone.utc
START = date(2022, 8, 4)
LOOKBACK = timedelta(hours=48)
NOW = datetime(2026, 10, 14, 15, 30, tzinfo=UTC)
TODAY = date(2026, 10, 14)


def test_first_run_starts_at_configured_date():
    assert request_window(None, NOW, START, LOOKBACK) == (START, TODAY)


def test_lookback_is_subtracted_from_watermark():
    watermark = datetime(2026, 10, 13, 23, tzinfo=UTC)
    # 2026-10-13T23:00 - 48h = 2026-10-11T23:00, widened to the whole day.
    assert request_window(watermark, NOW, START, LOOKBACK) == (date(2026, 10, 11), TODAY)


def test_lookback_never_reaches_before_start_date():
    watermark = datetime(2022, 8, 5, 3, tzinfo=UTC)
    assert request_window(watermark, NOW, START, LOOKBACK)[0] == START


def test_end_is_today_in_utc_even_from_another_timezone():
    pkt = timezone(timedelta(hours=5))
    early_morning_pkt = datetime(2026, 10, 15, 2, 0, tzinfo=pkt)  # still 10-14 in UTC
    assert request_window(None, early_morning_pkt, START, LOOKBACK)[1] == TODAY


def test_custom_lookback():
    watermark = datetime(2026, 10, 13, 23, tzinfo=UTC)
    got = request_window(watermark, NOW, START, timedelta(hours=6))
    assert got == (date(2026, 10, 13), TODAY)


def test_watermark_ahead_of_today_still_requests_today():
    watermark = datetime(2026, 10, 20, tzinfo=UTC)
    assert request_window(watermark, NOW, START, LOOKBACK) == (TODAY, TODAY)
