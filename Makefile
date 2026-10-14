# Convenience targets for Linux/macOS/CI. On Windows without make, run the commands directly.
.PHONY: install up up-db down reset check-db lint format test test-dags dbt backfill

install:
	python -m pip install -e ".[dev,dbt]"

# Everything: Postgres, Airflow init, API server (http://localhost:8080), scheduler, DAG processor.
up:
	docker compose up -d --build

up-db:
	docker compose up -d --wait postgres

down:
	docker compose down

# Deletes the Postgres volume: all warehouse and Airflow data.
reset:
	docker compose down -v

check-db:
	aq check-db

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .
	ruff check --fix .

test:
	pytest

# DAG integrity tests run inside the Airflow image (Airflow doesn't run natively on Windows).
test-dags:
	docker compose run --rm airflow-cli pytest -q tests/test_dags.py

dbt:
	aq dbt build

# Full history for all cities through Airflow (idempotent; safe to re-run).
backfill:
	docker compose exec airflow-scheduler airflow dags trigger air_quality_backfill
