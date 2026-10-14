"""Runtime configuration, read from environment variables.

Locally, values come from `.env` (loaded by the CLI). In containers, Docker Compose injects them.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name} (see .env.example)")
    return value


@dataclass(frozen=True)
class WarehouseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> WarehouseConfig:
        return cls(
            host=os.environ.get("WAREHOUSE_HOST", "localhost"),
            port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
            dbname=_require("WAREHOUSE_DB"),
            user=_require("WAREHOUSE_USER"),
            password=_require("WAREHOUSE_PASSWORD"),
        )
