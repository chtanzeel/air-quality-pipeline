"""Render docs/pm25_smog_seasons_{light,dark}.png from marts.fct_air_quality_daily.

    python scripts/make_chart.py      (needs the warehouse running and `pip install -e ".[docs]"`)

One panel per smog season (1 Oct - 31 Jan, local time), sharing the y axis: the 7-day mean
of daily mean PM2.5 for each city. Light and dark versions, for README <picture> switching.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from air_quality import db  # noqa: E402

DOCS = Path(__file__).resolve().parents[1] / "docs"

# Fixed city -> categorical slot (reference palette order, never cycled).
CITIES = [("lhr", "Lahore"), ("fsd", "Faisalabad"), ("isb", "Islamabad"),
          ("pew", "Peshawar"), ("khi", "Karachi")]  # fmt: skip
THEMES = {
    "light": {
        "surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e", "grid": "#e4e3df",
        "series": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"],
    },
    "dark": {
        "surface": "#1a1a19", "text": "#f0efec", "muted": "#c3c2b7", "grid": "#33332f",
        "series": ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181"],
    },
}  # fmt: skip
WHO_24H = 15.0  # WHO 2021 24-hour PM2.5 guideline, µg/m³


def fetch() -> dict[str, dict[str, list[tuple[date, float]]]]:
    """season -> city -> [(day, 7-day mean)], using complete days only."""
    sql = """
        select smog_season, city_id, day,
               avg(mean_pm2_5_ug_m3) over (
                   partition by city_id, smog_season order by day
                   rows between 6 preceding and current row) as pm25_7d
        from marts.fct_air_quality_daily
        where smog_season is not null and is_complete
        order by smog_season, city_id, day
    """
    out: dict[str, dict[str, list[tuple[date, float]]]] = {}
    with db.connect() as conn:
        for season, city, day, value in conn.execute(sql):
            out.setdefault(season, {}).setdefault(city, []).append((day, float(value)))
    return out


def season_axis_date(day: date, season: str) -> date:
    """Map a day onto a common Oct-Jan axis (year 2000/2001) so panels line up."""
    start_year = int(season[:4])
    return date(2000 + (day.year - start_year), day.month, day.day)


def label_line_ends(ax, ends, min_gap: float, color: str) -> None:
    """Direct labels at line ends, nudged apart vertically so they never overlap."""
    placed: list[float] = []
    for y, name, x in sorted(ends):
        y_label = max(y, placed[-1] + min_gap) if placed else y
        placed.append(y_label)
        ax.annotate(name, (x, y_label), xytext=(5, 0), textcoords="offset points", va="center",
                    fontsize=9, color=color, annotation_clip=False)  # fmt: skip


def render(data, mode: str, path: Path) -> None:
    t = THEMES[mode]
    seasons = sorted(data)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, axes = plt.subplots(1, len(seasons), figsize=(16, 4.6), sharey=True, dpi=110)
    fig.patch.set_facecolor(t["surface"])
    ymax = max(v for s in data.values() for c in s.values() for _, v in c) * 1.08

    for i, (ax, season) in enumerate(zip(axes, seasons, strict=True)):
        ax.set_facecolor(t["surface"])
        last_full = i == len(seasons) - 2  # direct labels on the latest complete season
        line_ends = []
        for (city_id, name), color in zip(CITIES, t["series"], strict=True):
            points = data[season].get(city_id, [])
            if not points:
                continue
            xs = [season_axis_date(d, season) for d, _ in points]
            ys = [v for _, v in points]
            ax.plot(xs, ys, color=color, linewidth=2, solid_capstyle="round", label=name)
            line_ends.append((ys[-1], name, xs[-1]))
        if last_full:
            label_line_ends(ax, line_ends, min_gap=ymax * 0.055, color=t["text"])
        ax.axhline(WHO_24H, color=t["muted"], linewidth=1, linestyle=(0, (4, 3)))
        partial = max(d for c in data[season].values() for d, _ in c) < date(
            int(season[:4]) + 1, 1, 31
        ) - timedelta(days=1)
        ax.set_title(season + (" (partial)" if partial else ""), color=t["text"], loc="left",
                     fontsize=11, fontweight="bold")  # fmt: skip
        ax.set_xlim(date(2000, 10, 1), date(2001, 1, 31))
        ax.set_ylim(0, ymax)
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.grid(axis="y", color=t["grid"], linewidth=0.8)
        ax.set_axisbelow(True)
        ax.tick_params(colors=t["muted"], length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.spines["bottom"].set_visible(True)
        ax.spines["bottom"].set_color(t["grid"])
    axes[0].set_ylabel("PM2.5, 7-day mean (µg/m³)", color=t["muted"])
    axes[0].annotate(
        "WHO 24-h guideline (15)", (date(2000, 10, 3), WHO_24H), xytext=(0, 4),
        textcoords="offset points", fontsize=8, color=t["muted"],
    )  # fmt: skip

    fig.suptitle(
        "Smog seasons in five Pakistani cities: daily PM2.5, 7-day mean",
        x=0.01, y=0.99, ha="left", fontsize=14, fontweight="bold", color=t["text"],
    )  # fmt: skip
    fig.text(
        0.01, 0.905,
        "CAMS global model data (~45 km grid) via Open-Meteo, not ground measurements. "
        "Complete local (PKT) days only. Season = 1 Oct to 31 Jan.",
        fontsize=9.5, color=t["muted"], ha="left",
    )  # fmt: skip
    handles, labels = axes[-2].get_legend_handles_labels()
    legend = fig.legend(handles, labels, loc="upper right", ncol=5, frameon=False,
                        bbox_to_anchor=(0.995, 0.995), fontsize=9.5)  # fmt: skip
    for text in legend.get_texts():
        text.set_color(t["text"])
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.subplots_adjust(wspace=0.32)
    fig.savefig(path, facecolor=t["surface"])
    plt.close(fig)
    print(f"wrote {path}")


def main() -> None:
    load_dotenv()
    data = fetch()
    for mode in THEMES:
        render(data, mode, DOCS / f"pm25_smog_seasons_{mode}.png")


if __name__ == "__main__":
    main()
