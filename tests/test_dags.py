"""DAG integrity: every DAG file imports cleanly (Airflow rejects cycles at import), and the
DAGs have the shape, retries and timeouts the pipeline relies on.

Needs Airflow, which doesn't run natively on Windows. Locally:
    docker compose run --rm airflow-cli pytest tests/test_dags.py
Skipped when Airflow isn't installed, unless AQ_REQUIRE_AIRFLOW=1 (as in CI).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

if os.environ.get("AQ_REQUIRE_AIRFLOW") != "1":
    pytest.importorskip("airflow", reason="Airflow not installed (runs in Docker and CI)")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

# In the Airflow containers the DAGs are mounted at /opt/airflow/dags (compose sets AQ_DAGS_DIR).
DAGS_DIR = Path(os.environ.get("AQ_DAGS_DIR", Path(__file__).resolve().parents[1] / "dags"))


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    # Guard against a silent pass: an empty or missing folder has no import errors either.
    assert sorted(p.name for p in DAGS_DIR.glob("*.py")), f"no DAG files in {DAGS_DIR}"
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_present(dagbag):
    assert set(dagbag.dag_ids) == {"air_quality_daily", "air_quality_backfill"}


def test_daily_dag_shape(dagbag):
    dag = dagbag.dags["air_quality_daily"]
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    order = {t.task_id: {d.task_id for d in t.downstream_list} for t in dag.tasks}
    assert order == {
        "extract": {"load"},
        "load": {"dbt_build"},
        "dbt_build": {"quality_summary"},
        "quality_summary": set(),
    }


def test_backfill_dag_is_manual_and_parameterised(dagbag):
    dag = dagbag.dags["air_quality_backfill"]
    assert dag.schedule is None or str(dag.timetable.summary) in ("None", "")
    assert {"start_date", "end_date", "cities"} <= set(dag.params.keys())
    order = {t.task_id: {d.task_id for d in t.downstream_list} for t in dag.tasks}
    assert order["plan"] == {"ingest_chunk"}
    assert order["ingest_chunk"] == {"dbt_build"}
    assert order["dbt_build"] == {"quality_summary"}


@pytest.mark.parametrize("dag_id", ["air_quality_daily", "air_quality_backfill"])
def test_every_task_has_retries_and_timeout(dagbag, dag_id):
    for t in dagbag.dags[dag_id].tasks:
        assert t.retries >= 2, t.task_id
        assert t.execution_timeout is not None, t.task_id
        assert t.on_failure_callback, t.task_id
