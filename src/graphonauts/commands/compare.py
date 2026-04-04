"""Cross-database comparison of benchmark results for the Graphonauts framework.

This module implements the comparison methodology used to evaluate multiple graph
database systems against one another using previously collected benchmark data. It
performs no benchmarking itself; instead, it discovers the most recent result files
from the ``benchmarks/`` directory tree and assembles side-by-side comparison tables.

**Result discovery strategy.** Benchmark results are stored in a hierarchical directory
structure: ``benchmarks/{phase}/{db}/{category}/{variant}/{timestamp}/``. For each
database and query combination, the discovery functions sort timestamp directories
lexicographically and select the latest one (i.e., the most recent benchmark run).
This allows results to accumulate over time without manual cleanup, while always
presenting the freshest data in comparisons.

**Comparison tables.** Three categories of comparison are produced:

1. **Load benchmark** --- compares total data loading time and memory consumption
   (baseline, peak, and delta) across databases using the latest ``LoadResult`` per
   database.

2. **Memory benchmark** --- compares cache-adjusted peak memory delta for each query
   across databases using the latest ``MemoryResult`` per database and query.

3. **Time benchmark** --- compares a user-selected statistical metric (mean, median,
   min, max, 5th or 95th percentile) from the ``TimeSummary`` for each query across
   databases. The default metric is the median, which is robust to outliers caused by
   garbage collection pauses or background OS activity.

Tables are rendered to stdout with fixed-width columns and grouped by query category
(Selection, Aggregation, Join, Set, Modification) for readability.
"""

from typing import Any

import click

from graphonauts.base.results import LoadResult, MemoryResult, TimeSummary
from graphonauts.commands import BENCHMARKS_PATH, DB_MODULES, QUERY_CATEGORIES
from utils import printer

TIME_METRIC_ATTRS: dict[str, str] = {
    "mean": "mean_s",
    "median": "median_s",
    "min": "min_s",
    "max": "max_s",
    "p5": "percentile_5_s",
    "p95": "percentile_95_s",
}


# ---------------------------------------------------------------------------
# Result discovery (find latest timestamp directory per database/query)
# ---------------------------------------------------------------------------


def _find_latest_load_result(db_name: str) -> LoadResult | None:
    """Locate the most recent load benchmark result for a given database.

    Searches ``benchmarks/load/{db_name}/`` for timestamp-named subdirectories,
    selects the lexicographically latest one, and deserializes the ``LoadResult``
    from its JSON file.

    Args:
        db_name: The canonical name of the database (e.g., ``"neo4j"``, ``"memgraph"``).

    Returns:
        The deserialized ``LoadResult`` if found, or ``None`` if no result exists.
    """
    load_dir = BENCHMARKS_PATH / "load" / db_name
    if not load_dir.exists():
        return None
    timestamp_dirs = sorted((d for d in load_dir.iterdir() if d.is_dir()), key=lambda d: d.name)
    if not timestamp_dirs:
        return None
    json_file = timestamp_dirs[-1] / f"{db_name}.json"
    if not json_file.exists():
        return None
    return LoadResult.load(json_file)


def _find_latest_memory_results(db_name: str) -> dict[tuple[str, int], MemoryResult]:
    """Locate the most recent memory benchmark result for each query of a given database.

    Traverses the ``benchmarks/memory/{db_name}/{category}/{variant}/`` hierarchy,
    and for each query selects the lexicographically latest timestamp directory.

    Args:
        db_name: The canonical name of the database (e.g., ``"neo4j"``, ``"memgraph"``).

    Returns:
        A dictionary mapping ``(category, variant)`` tuples to their latest
        ``MemoryResult``. Queries for which no result exists are omitted.
    """
    memory_dir = BENCHMARKS_PATH / "memory" / db_name
    if not memory_dir.exists():
        return {}
    results: dict[tuple[str, int], MemoryResult] = {}
    for category_name in QUERY_CATEGORIES:
        category_dir = memory_dir / category_name
        if not category_dir.exists():
            continue
        for variant_dir in sorted(category_dir.iterdir()):
            if not variant_dir.is_dir():
                continue
            try:
                variant = int(variant_dir.name)
            except ValueError:
                continue
            timestamp_dirs = sorted((d for d in variant_dir.iterdir() if d.is_dir()), key=lambda d: d.name)
            if not timestamp_dirs:
                continue
            json_file = timestamp_dirs[-1] / "memory.json"
            if json_file.exists():
                results[(category_name, variant)] = MemoryResult.load(json_file)
    return results


def _find_latest_time_summaries(db_name: str) -> dict[tuple[str, int], TimeSummary]:
    """Locate the most recent time benchmark summary for each query of a given database.

    Traverses the ``benchmarks/time/{db_name}/{category}/{variant}/`` hierarchy,
    and for each query selects the lexicographically latest timestamp directory.

    Args:
        db_name: The canonical name of the database (e.g., ``"neo4j"``, ``"memgraph"``).

    Returns:
        A dictionary mapping ``(category, variant)`` tuples to their latest
        ``TimeSummary``. Queries for which no summary exists are omitted.
    """
    time_dir = BENCHMARKS_PATH / "time" / db_name
    if not time_dir.exists():
        return {}
    results: dict[tuple[str, int], TimeSummary] = {}
    for category_name in QUERY_CATEGORIES:
        category_dir = time_dir / category_name
        if not category_dir.exists():
            continue
        for variant_dir in sorted(category_dir.iterdir()):
            if not variant_dir.is_dir():
                continue
            try:
                variant = int(variant_dir.name)
            except ValueError:
                continue
            timestamp_dirs = sorted((d for d in variant_dir.iterdir() if d.is_dir()), key=lambda d: d.name)
            if not timestamp_dirs:
                continue
            summary_file = timestamp_dirs[-1] / "summary.json"
            if summary_file.exists():
                results[(category_name, variant)] = TimeSummary.load(summary_file)
    return results


# ---------------------------------------------------------------------------
# Comparison table rendering
# ---------------------------------------------------------------------------


def _print_query_comparison_table(
    title: str,
    db_names: list[str],
    all_results: dict[str, dict[tuple[str, int], Any]],
    value_getter: Any,
    desc_getter: Any,
    fmt: str,
) -> None:
    """Render a fixed-width comparison table for a set of query-level benchmark results.

    This is a generic table renderer used by both the memory and time comparison
    functions. It collects the union of all query keys present across databases,
    groups them by query category, and prints one row per query with database-specific
    values in separate columns.

    Args:
        title: The table heading displayed above the output (e.g., ``"Memory Benchmark (MB)"``).
        db_names: Ordered list of database names to include as columns.
        all_results: A mapping from database name to a dictionary of ``(category, variant)``
            keys and their corresponding result objects.
        value_getter: A callable that extracts the numeric value to display from a single
            result object.
        desc_getter: A callable that extracts a human-readable description string from a
            single result object.
        fmt: A Python format specifier for the numeric value (e.g., ``".2f"``, ``".4f"``).
    """
    if not any(all_results.values()):
        print(f"No {title.lower()} benchmark results found.\n")
        return

    all_keys: set[tuple[str, int]] = set()
    for result_dict in all_results.values():
        all_keys.update(result_dict.keys())

    printer.subheader(title)

    query_w = 18
    desc_w = 40
    col_w = max(15, max(len(db) for db in db_names) + 4)

    header_line = f"{'Query':<{query_w}}{'Description':<{desc_w}}" + "".join(f"{db:>{col_w}}" for db in db_names)
    print(header_line)
    print("-" * len(header_line))

    current_category: str | None = None
    for key in sorted(all_keys):
        qcat, qvar = key
        if qcat != current_category:
            current_category = qcat
            category_label = qcat.capitalize()
            print(f"\n  {category_label}")

        desc = ""
        for db in db_names:
            r = all_results[db].get(key)
            if r is not None:
                desc = desc_getter(r)
                break
        if len(desc) > desc_w - 2:
            desc = desc[: desc_w - 5] + "..."

        query_label = f"{qcat} {qvar}"
        row = f"{query_label:<{query_w}}{desc:<{desc_w}}"
        for db in db_names:
            r = all_results[db].get(key)
            if r is None:
                row += f"{'N/A':>{col_w}}"
            else:
                row += f"{value_getter(r):>{col_w}{fmt}}"
        print(row)
    printer.blank()


def _print_load_comparison(db_names: list[str]) -> None:
    results: dict[str, LoadResult | None] = {db: _find_latest_load_result(db) for db in db_names}

    if not any(results.values()):
        print("\nNo load benchmark results found.\n")
        return

    printer.subheader("Load Benchmark")

    metric_w = 20
    col_w = max(15, max(len(db) for db in db_names) + 4)

    header_line = f"{'Metric':<{metric_w}}" + "".join(f"{db:>{col_w}}" for db in db_names)
    print(header_line)
    print("-" * len(header_line))

    for label, attr in [
        ("Time (s)", "total_time_s"),
        ("Memory (MB)", "memory_usage_mb"),
        ("Baseline (MB)", "memory_baseline_mb"),
        ("Peak (MB)", "memory_peak_mb"),
    ]:
        row = f"{label:<{metric_w}}"
        for db in db_names:
            r = results[db]
            if r is None:
                row += f"{'N/A':>{col_w}}"
            else:
                row += f"{getattr(r, attr):>{col_w}.2f}"
        print(row)
    printer.blank()


def _print_memory_comparison(db_names: list[str]) -> None:
    all_memory: dict[str, dict[tuple[str, int], MemoryResult]] = {
        db: _find_latest_memory_results(db) for db in db_names
    }
    _print_query_comparison_table(
        title="Memory Benchmark (MB)",
        db_names=db_names,
        all_results=all_memory,
        value_getter=lambda r: r.memory_usage_mb,
        desc_getter=lambda r: r.description,
        fmt=".2f",
    )


def _print_time_comparison(db_names: list[str], time_metric: str = "median") -> None:
    all_time: dict[str, dict[tuple[str, int], TimeSummary]] = {db: _find_latest_time_summaries(db) for db in db_names}
    attr = TIME_METRIC_ATTRS[time_metric]
    _print_query_comparison_table(
        title=f"Time Benchmark ({time_metric}, seconds)",
        db_names=db_names,
        all_results=all_time,
        value_getter=lambda r: getattr(r, attr),
        desc_getter=lambda r: r.description,
        fmt=".4f",
    )


@click.command()
@click.argument("db", nargs=-1, type=click.Choice(list(DB_MODULES.keys()), case_sensitive=False))
@click.option(
    "--metric",
    type=click.Choice(["mean", "median", "min", "max", "p5", "p95"], case_sensitive=False),
    default="median",
    help="Time metric for comparison (default: median)",
)
def compare(db: tuple[str, ...], metric: str) -> None:
    """Compare benchmark results across databases."""
    db_names = sorted(db) if db else sorted(DB_MODULES.keys())
    if len(db_names) < 2:
        raise click.UsageError("compare requires at least 2 databases. Omit DB argument to compare all.")

    printer.banner(f"Comparing databases: {', '.join(db_names)}")

    _print_load_comparison(db_names)
    _print_memory_comparison(db_names)
    _print_time_comparison(db_names, metric)
