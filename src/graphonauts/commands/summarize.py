"""Re-compute summary statistics from existing time benchmark run files.

Scans time benchmark directories for run_*.json files and re-aggregates them
into summary.json. Useful after interrupted benchmark runs or when you want
to recompute statistics with updated code.
"""

import click

from graphonauts.base.results import TimeSummary
from graphonauts.commands import BENCHMARKS_PATH, DB_MODULES, QUERY_CATEGORIES
from utils import printer


def _summarize_time_results(db_name: str, category: str | None = None, query: int | None = None) -> list[TimeSummary]:
    """Re-aggregate time benchmark run files into summary.json for every timestamped run."""
    base = BENCHMARKS_PATH / "time" / db_name
    if not base.exists():
        print(f"No time results found for {db_name} at {base}")
        return []

    summaries: list[TimeSummary] = []

    dirs = sorted(d for d in base.rglob("*") if d.is_dir() and any(d.glob("run_*.json")))

    if category:
        category_dir = base / category
        if query is not None:
            query_prefix = category_dir / str(query)
            dirs = [d for d in dirs if query_prefix in d.parents or d.parent == query_prefix]
        else:
            dirs = [d for d in dirs if category_dir in d.parents or d.parent == category_dir]

    for d in dirs:
        summary = TimeSummary.from_directory(d)
        summary.save(d)
        summaries.append(summary)

        printer.header(f"Query {summary.query_category} {summary.query_variant} ({d.name})")
        printer.info(summary.description)
        printer.separator()
        printer.summary_stats(
            mean_s=summary.mean_s,
            median_s=summary.median_s,
            stdev_s=summary.stdev_s,
            min_s=summary.min_s,
            max_s=summary.max_s,
            p5_s=summary.percentile_5_s,
            p95_s=summary.percentile_95_s,
        )
        printer.blank()

    if not summaries:
        print("No run files found to summarize.")

    return summaries


@click.command()
@click.argument("db", type=click.Choice(list(DB_MODULES.keys()), case_sensitive=False))
@click.option(
    "--category",
    type=click.Choice(QUERY_CATEGORIES, case_sensitive=False),
    default=None,
    help="Query category (e.g., join, selection)",
)
@click.option("--query", type=int, default=None, help="Specific variant number within the category")
def summarize(db: str, category: str | None, query: int | None) -> None:
    """Re-compute summary.json from existing time benchmark runs."""
    _summarize_time_results(db, category, query)
