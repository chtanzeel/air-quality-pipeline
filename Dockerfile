FROM apache/airflow:3.3.2

# dbt lives in its own virtualenv: its pinned dependencies conflict with Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.12.6" "dbt-postgres==1.11.0"

USER root
RUN mkdir -p /opt/aq && chown airflow:0 /opt/aq
USER airflow

# The pipeline package, installed editable so the mounted ./src is what runs.
# apache-airflow is pinned in the same command so pip can never upgrade it.
COPY --chown=airflow:0 pyproject.toml README.md /opt/aq/
COPY --chown=airflow:0 src /opt/aq/src
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -e "/opt/aq" "pytest>=8,<10"
