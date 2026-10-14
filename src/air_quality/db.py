"""Warehouse connections."""

from __future__ import annotations

import psycopg

from air_quality.config import WarehouseConfig


def connect(config: WarehouseConfig | None = None) -> psycopg.Connection:
    config = config or WarehouseConfig.from_env()
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        connect_timeout=10,
        application_name="air_quality",
    )
