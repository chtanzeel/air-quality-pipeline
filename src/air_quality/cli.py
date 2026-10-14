"""Command-line entry point: `aq <command>` (or `python -m air_quality.cli <command>`)."""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

from air_quality import db

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


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="aq", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-db", help="connect to the warehouse and verify schemas")
    args = parser.parse_args(argv)

    if args.command == "check-db":
        return check_db()
    return 2


if __name__ == "__main__":
    sys.exit(main())
