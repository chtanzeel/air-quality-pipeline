"""Command-line entry point: `aq <command>` (or `python -m air_quality.cli <command>`)."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from air_quality import db, pipeline
from air_quality.cities import load_cities
from air_quality.client import OpenMeteoClient
from air_quality.config import REPO_ROOT, PipelineConfig
from air_quality.schema import ensure_schema

REQUIRED_SCHEMAS = ("raw", "ops")


def check_db() -> int:
    """Connect to the warehouse and confirm the schemas from sql/init exist."""
    with db.connect() as conn:
        server, database, user = conn.execute(
            "select current_setting('server_version'), current_database(), current_user"
        ).fetchone()
        schemas = {
            row[0]
            for row in conn.execute(
                "select schema_name from information_schema.schemata where schema_name = any(%s)",
                (list(REQUIRED_SCHEMAS),),
            )
        }
    print(f"Connected: Postgres {server}, database={database}, user={user}")
    missing = [s for s in REQUIRED_SCHEMAS if s not in schemas]
    if missing:
        print(f"Missing schemas: {', '.join(missing)}", file=sys.stderr)
        return 1
    print(f"Schemas present: {', '.join(REQUIRED_SCHEMAS)}")
    return 0


def status() -> int:
    with db.connect() as conn:
        ensure_schema(conn)
        print("Rows per city:")
        for row in conn.execute(
            """
            select r.city_id, count(*), min(measured_at), max(measured_at), w.last_loaded_hour
            from raw.air_quality_hourly r left join ops.watermarks w using (city_id)
            group by r.city_id, w.last_loaded_hour order by r.city_id
            """
        ):
            print("  {:<4} rows={:<7} {} .. {}  watermark={}".format(*row))
        print("Last runs:")
        for row in conn.execute(
            """
            select run_id, status, window_start, window_end, files_written, rows_fetched,
                   rows_inserted, rows_updated, rows_unchanged, duration_seconds, error
            from ops.run_log order by started_at desc limit 5
            """
        ):
            print(
                "  {} {} window={}..{} files={} fetched={} ins={} upd={} same={} {}s {}".format(
                    *row[:10], row[10] or ""
                )
            )
    return 0


def run_dbt(args: list[str]) -> int:
    """Run dbt with this repo's project and profiles dirs, using the env loaded from .env."""
    from dbt.cli.main import dbtRunner  # optional dependency: pip install -e ".[dbt]"

    dbt_dir = str(REPO_ROOT / "dbt")
    extra = [] if "--project-dir" in args else ["--project-dir", dbt_dir]
    extra += [] if "--profiles-dir" in args else ["--profiles-dir", dbt_dir]
    result = dbtRunner().invoke([*args, *extra])
    return 0 if result.success else 1


def _expand(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for p in map(Path, paths):
        if p.is_dir():
            out.extend(f for f in p.rglob("*.json") if not f.name.endswith(".params.json"))
        else:
            out.append(p)
    return sorted(out)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(prog="aq", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-db", help="connect to the warehouse and verify schemas")
    sub.add_parser("init-db", help="create pipeline tables (idempotent)")
    sub.add_parser("status", help="show row counts, watermarks and recent runs")
    p_run = sub.add_parser("run", help="incremental run: watermark - lookback .. today")
    p_bf = sub.add_parser("backfill", help="load an explicit date range (inclusive)")
    p_bf.add_argument("--start", type=date.fromisoformat, required=True)
    p_bf.add_argument("--end", type=date.fromisoformat, required=True)
    for p in (p_run, p_bf):
        p.add_argument("--cities", help="comma-separated city_ids (default: all)")
    p_rp = sub.add_parser("replay", help="re-load landed files or directories, no API calls")
    p_rp.add_argument("paths", nargs="+")
    p_dbt = sub.add_parser("dbt", help="run dbt with the repo's project/profiles and .env")
    p_dbt.add_argument("dbt_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    if args.command == "check-db":
        return check_db()
    if args.command == "status":
        return status()
    if args.command == "dbt":
        return run_dbt(args.dbt_args)

    cfg = PipelineConfig.from_env()
    with db.connect() as conn:
        ensure_schema(conn)
        if args.command == "init-db":
            print("Schema ready.")
            return 0
        if args.command == "replay":
            summary = pipeline.replay(conn, _expand(args.paths), cfg.landing_dir)
        else:
            only = args.cities.split(",") if args.cities else None
            cities = load_cities(cfg.cities_file, only)
            if args.command == "run":
                summary = pipeline.run(conn, OpenMeteoClient(), cities, cfg, "incremental")
            else:
                summary = pipeline.run(
                    conn, OpenMeteoClient(), cities, cfg, "backfill", args.start, args.end
                )
    print(asdict(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
