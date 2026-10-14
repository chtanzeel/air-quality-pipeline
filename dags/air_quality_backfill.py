"""
### air_quality_backfill

Loads an explicit date range, by default all history from 2022-08-04 (the first hour of CAMS
global data on Open-Meteo) to today.

`plan` splits the range into one chunk per city and 31-day window. Each chunk is a separate
mapped task that extracts one window and loads it straight away, so a failure only retries
that chunk. Chunks are idempotent: landed files are never overwritten, and the upsert only
writes rows that are new or changed, so re-running any range is safe. At most three chunks
run at once, to keep requests to the free API modest. Watermarks only move forward, so
backfilling old data never rewinds the daily DAG.

Trigger with params, e.g. `{"start_date": "2024-10-01", "end_date": "2025-01-31"}`.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

from air_quality import tasks as aq
from air_quality.cities import load_cities
from air_quality.config import PipelineConfig

DBT = (
    '"$AQ_DBT_BIN" {command} --project-dir "$AQ_DBT_DIR" --profiles-dir "$AQ_DBT_DIR" '
    "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
)
ALL_CITIES = ["lhr", "khi", "isb", "pew", "fsd"]

default_args = {
    "owner": "air-quality",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": aq.on_task_failure,
}


@dag(
    dag_id="air_quality_backfill",
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    doc_md=__doc__,
    tags=["air-quality", "backfill"],
    params={
        "start_date": Param("2022-08-04", type="string", format="date"),
        "end_date": Param(
            None, type=["null", "string"], format="date", description="Default: today (UTC)"
        ),
        "cities": Param(ALL_CITIES, type="array", description="city_ids to backfill"),
    },
)
def air_quality_backfill():
    @task(execution_timeout=timedelta(minutes=5))
    def plan(params: dict | None = None, run_id: str | None = None) -> list[dict]:
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"]) if params.get("end_date") else None
        end = end or datetime.now(timezone.utc).date()
        cities = [
            c.city_id for c in load_cities(PipelineConfig.from_env().cities_file, params["cities"])
        ]
        aq.start_run(run_id, "backfill", cities)
        return aq.plan_backfill(start, end, cities, PipelineConfig.from_env().window_days)

    @task(max_active_tis_per_dag=3, execution_timeout=timedelta(minutes=15))
    def ingest_chunk(chunk: dict, run_id: str | None = None) -> dict:
        return aq.ingest_chunk(run_id, chunk)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT.format(command="build"),
        execution_timeout=timedelta(minutes=30),
    )

    @task(execution_timeout=timedelta(minutes=5))
    def quality_summary(run_id: str | None = None) -> dict:
        return aq.finish(run_id)

    ingest_chunk.expand(chunk=plan()) >> dbt_build >> quality_summary()


air_quality_backfill()
