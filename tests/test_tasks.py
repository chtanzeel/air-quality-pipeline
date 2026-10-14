"""Run-log bookkeeping in the Airflow task layer, against the Postgres scratch database."""

from __future__ import annotations

import os

import pytest

from air_quality import tasks

pytestmark = pytest.mark.postgres


@pytest.fixture
def scratch_db(pg_conn, monkeypatch):
    """Point the task layer (which opens its own connections) at the scratch database."""
    monkeypatch.setenv("WAREHOUSE_DB", pg_conn.info.dbname)
    return pg_conn


def status(conn, run_id):
    return conn.execute(
        "select status, error, finished_at is not null from ops.run_log where run_id = %s",
        (run_id,),
    ).fetchone()


def test_finish_marks_success_and_reports(scratch_db):
    tasks.start_run("manual__1", "incremental", ["lhr"])
    summary = tasks.finish("manual__1")
    assert status(scratch_db, "manual__1") == ("success", None, True)
    assert summary["mode"] == "incremental"
    assert summary["total_rows"] == 0


def test_failure_callback_records_error(scratch_db):
    class TI:
        task_id = "extract"

    tasks.start_run("manual__2", "incremental")
    tasks.on_task_failure(
        {"run_id": "manual__2", "task_instance": TI(), "exception": RuntimeError("API down")}
    )
    state, error, finished = status(scratch_db, "manual__2")
    assert (state, finished) == ("failed", True)
    assert "extract" in error and "API down" in error


def test_failure_callback_never_raises(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_PORT", "1")  # nothing listens here
    monkeypatch.setitem(os.environ, "WAREHOUSE_HOST", "127.0.0.1")
    tasks.on_task_failure({"run_id": "x", "exception": RuntimeError("boom")})
