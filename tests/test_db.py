"""Smoke test: the warehouse is reachable and sql/init created the expected schemas."""

import os

import pytest
from dotenv import load_dotenv

from air_quality import db

pytestmark = pytest.mark.postgres


def test_warehouse_has_raw_and_ops_schemas():
    load_dotenv()
    with db.connect() as conn:
        rows = conn.execute(
            "select schema_name from information_schema.schemata "
            "where schema_name in ('raw', 'ops') order by 1"
        ).fetchall()
    assert [r[0] for r in rows] == ["ops", "raw"]


def test_warehouse_role_cannot_reach_airflow_database():
    load_dotenv()
    with db.connect() as conn:
        allowed = conn.execute(
            "select has_database_privilege(current_user, %s, 'CONNECT')",
            (os.environ.get("AIRFLOW_DB", "airflow"),),
        ).fetchone()[0]
    assert allowed is False
