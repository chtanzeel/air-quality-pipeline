"""Write docs/portfolio_stats.json from the warehouse and the dbt project. Every value is read
from a real run; nothing is typed in by hand except the pytest count, which comes from a run of
the full suite and is passed in.

    python scripts/portfolio_stats.py --pytest-passed 53
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone

from dotenv import load_dotenv

from air_quality import db
from air_quality.config import REPO_ROOT

OUT = REPO_ROOT / "docs" / "portfolio_stats.json"


def dbt_counts() -> dict[str, int]:
    from dbt.cli.main import dbtRunner

    dbt_dir = str(REPO_ROOT / "dbt")
    result = dbtRunner().invoke(
        [
            "ls",
            "--project-dir",
            dbt_dir,
            "--profiles-dir",
            dbt_dir,
            "--output",
            "json",
            "--output-keys",
            "resource_type",
            "--quiet",
        ]  # fmt: skip
    )
    counts = Counter(json.loads(line)["resource_type"] for line in result.result)
    return {
        "models": counts["model"],
        "seeds": counts["seed"],
        "data_tests": counts["test"],
        "unit_tests": counts["unit_test"],
        "sources": counts["source"],
    }


def warehouse_stats() -> dict:
    with db.connect() as conn:
        cities = [
            {"city_id": c, "city": n, "province": p, "latitude": lat, "longitude": lon}
            for c, n, p, lat, lon in conn.execute(
                "select city_id, city, province, latitude, longitude from marts.dim_city "
                "order by city_id"
            )
        ]
        first, last, total = conn.execute(
            "select min(measured_at), max(measured_at), count(*) from raw.air_quality_hourly"
        ).fetchone()
        quarantined = conn.execute(
            "select count(*) from quarantine.quarantine_air_quality_hourly"
        ).fetchone()[0]
        reasons = dict(
            conn.execute(
                """
                select regexp_replace(quarantine_reason, ' [0-9]{4}-[0-9]{2}-[0-9]{2}-.*$', ''),
                       count(*)
                from quarantine.quarantine_air_quality_hourly group by 1 order by 2 desc
                """
            ).fetchall()
        )
        # The full-history backfill: the backfill run with the earliest window start.
        bf = conn.execute(
            """
            select run_id, window_start, window_end, files_written, rows_inserted,
                   duration_seconds
            from ops.run_log
            where mode = 'backfill' and status = 'success'
            order by window_start, (window_end - window_start) desc limit 1
            """
        ).fetchone()
        monthly = conn.execute(
            """
            select to_char(date_trunc('month', day), 'YYYY-MM') as month, city_id,
                   round(avg(mean_pm2_5_ug_m3), 1)
            from marts.fct_air_quality_daily
            where is_complete
            group by 1, 2 order by 1, 2
            """
        ).fetchall()

    series: dict[str, dict] = {}
    for month, city, value in monthly:
        series.setdefault(month, {"month": month})[city] = float(value)

    return {
        "cities": cities,
        "date_range_utc": {"first_hour": first.isoformat(), "last_hour": last.isoformat()},
        "total_rows": total,
        "rows_quarantined": quarantined,
        "quarantine_share": round(quarantined / total, 5),
        "quarantine_reasons": reasons,
        "backfill": {
            "run_id": bf[0],
            "window": f"{bf[1]}..{bf[2]}",
            "files": bf[3],
            "rows_inserted": bf[4],
            "runtime_seconds": float(bf[5]),
            "runtime_note": "ops.run_log duration: plan task start to quality summary, "
            "including dbt build",
        },
        "monthly_mean_pm2_5_ug_m3": list(series.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pytest-passed", type=int, required=True)
    args = parser.parse_args()
    load_dotenv()

    stats = {
        "project": "air-quality-pipeline",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "data_note": "CAMS global atmospheric composition forecasts (model data, ~45 km grid, "
        "3-hourly native steps) via Open-Meteo; not ground-station measurements.",
        **warehouse_stats(),
        "dbt": dbt_counts(),
        "pytest_passed": args.pytest_passed,
        "stack": {"airflow": "3.3.2", "dbt_core": "1.12.6", "postgres": "17.11"},
    }
    OUT.write_text(json.dumps(stats, indent=2, default=str) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
