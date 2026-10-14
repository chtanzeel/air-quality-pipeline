"""
### air_quality_daily

Incremental daily load of CAMS air quality model data for five Pakistani cities.

`extract` asks the Open-Meteo API, for each city, for *watermark - lookback* (48 h by default)
up to today and lands the raw responses in `landing/`. `load` upserts them into
`raw.air_quality_hourly` (re-fetched hours are only rewritten if upstream revised them) and
advances the watermarks. `dbt_build` checks source freshness, then builds and tests the models.
`quality_summary` closes the run in `ops.run_log` and reports row counts, freshness and the
quarantine share.

Runs at 01:30 UTC (06:30 in Pakistan). `catchup=False`: each run works out its own window from
the watermark, so missed days are picked up by the next run. For history, use
`air_quality_backfill`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from air_quality import tasks as aq

DBT = (
    '"$AQ_DBT_BIN" {command} --project-dir "$AQ_DBT_DIR" --profiles-dir "$AQ_DBT_DIR" '
    "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
)

default_args = {
    "owner": "air-quality",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": aq.on_task_failure,
}


@dag(
    dag_id="air_quality_daily",
    schedule="30 1 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args=default_args,
    doc_md=__doc__,
    tags=["air-quality", "incremental"],
)
def air_quality_daily():
    @task(execution_timeout=timedelta(minutes=60))
    def extract(run_id: str | None = None) -> list[str]:
        aq.start_run(run_id, "incremental")
        return aq.extract(run_id)

    @task(execution_timeout=timedelta(minutes=30))
    def load(paths: list[str], run_id: str | None = None) -> dict:
        return aq.load(run_id, paths)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT.format(command='source freshness')} && {DBT.format(command='build')}",
        execution_timeout=timedelta(minutes=30),
    )

    @task(execution_timeout=timedelta(minutes=5))
    def quality_summary(run_id: str | None = None) -> dict:
        return aq.finish(run_id)

    load(extract()) >> dbt_build >> quality_summary()


air_quality_daily()
