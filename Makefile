# Convenience targets for Linux/macOS/CI. On Windows without make, run the commands directly.
.PHONY: install up down reset check-db lint format test

install:
	python -m pip install -e ".[dev]"

up:
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
