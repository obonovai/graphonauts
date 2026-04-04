"""Unified terminal output styling for the Graphonauts benchmarking framework.

This module provides a centralised set of formatting functions that ensure consistent,
readable terminal output across all framework commands (benchmark, compare, load, verify)
and database-specific loaders. By routing all user-facing output through these functions,
the framework maintains a uniform visual language --- headers, separators, progress
indicators, key-value pairs, tabular data, and statistical summaries --- regardless of
which command or database module is active.

All functions write directly to ``stdout`` using Python's built-in ``print()``. No
logging framework is used for user-facing output; the ``logging`` module is reserved
for debug-level diagnostics in background threads (e.g., the memory monitoring thread).

Design rationale: a module-level function library (rather than a class) was chosen
because output formatting is stateless and does not benefit from encapsulation. The
``WIDTH``, ``SEP_CHAR``, and ``BANNER_CHAR`` module constants provide the only
configurable parameters.

Usage::

    from utils.printer import header, separator, task_start, task_done, table
"""

from typing import Any

# Standard separator width used across all output
WIDTH = 70
SEP_CHAR = "-"
BANNER_CHAR = "="


def header(text: str) -> None:
    """Print a command header line, e.g., ``'>> Loading TPC-H data into neo4j'``.

    Args:
        text: The header text to display after the ``>>`` prefix.
    """
    print(f">> {text}")


def subheader(text: str) -> None:
    """Print a section title, e.g., ``'--- Memory Benchmark (MB) ---'``.

    Args:
        text: The section title text, displayed between ``---`` delimiters.
    """
    print(f"\n--- {text} ---\n")


def separator() -> None:
    """Print a horizontal divider line."""
    print(SEP_CHAR * WIDTH)


def banner(text: str) -> None:
    """Print a prominent banner for major operations.

    The banner consists of a full-width line of ``=`` characters, the text indented
    by three spaces, and another full-width ``=`` line. Used to visually demarcate
    the start of significant operations such as a full benchmark suite run.

    Args:
        text: The banner message text.
    """
    print(BANNER_CHAR * WIDTH)
    print(f"   {text}")
    print(BANNER_CHAR * WIDTH)


def info(text: str) -> None:
    """Print an indented informational line.

    Args:
        text: The informational message to display, indented by three spaces.
    """
    print(f"   {text}")


def warning(text: str) -> None:
    """Print a warning message with a prominent prefix.

    Used to alert the user to configuration choices that may produce
    scientifically invalid or unexpected results, such as running memory
    benchmarks without container restart.

    Args:
        text: The warning message to display after the ``[!]`` prefix.
    """
    print(f"  [!] {text}")


def key_value(key: str, value: Any, fmt: str = "") -> None:
    """Print an indented key-value pair, e.g., ``'   time:   1.23s'``.

    Args:
        key: The label for the value, right-padded to 8 characters with a trailing colon.
        value: The value to display. Formatted using ``fmt`` if provided, otherwise
            converted to string via ``str()``.
        fmt: An optional Python format specifier (e.g., ``".2f"``) applied to ``value``.
    """
    formatted = f"{value:{fmt}}" if fmt else str(value)
    print(f"   {key + ':':<8} {formatted}")


def task_start(label: str) -> None:
    """Print a task-in-progress indicator: ``'[ ] label...'``.

    Uses a carriage return and ``flush=True`` to enable in-place overwriting by
    a subsequent ``task_done()`` call on the same line.

    Args:
        label: The task description shown after the ``[ ]`` prefix.
    """
    print(f"\r[ ] {label}...", end="", flush=True)


def task_done(label: str, detail: str | None = None) -> None:
    """Overwrite the current line with a completed task indicator.

    Replaces the in-progress indicator left by ``task_start()`` with a checkmark
    and optional detail suffix (e.g., elapsed time or row count).

    Args:
        label: The task description (should match the corresponding ``task_start()`` call).
        detail: An optional string appended in parentheses after the label.
    """
    suffix = f" ({detail})" if detail else ""
    print(f"\r[✓] {label}{suffix}" + " " * 30)


def task_progress(label: str, current: int, total: int) -> None:
    """Overwrite the current line with batch progress: ``'[ ] label... batch X/Y'``.

    Used during chunked data loading to indicate how many batches have been
    processed out of the total.

    Args:
        label: The task description.
        current: The current batch number (1-indexed).
        total: The total number of batches.
    """
    print(f"\r[ ] {label}... batch {current}/{total}", end="", flush=True)


def run_progress(run_number: int, total_runs: int, time_s: float) -> None:
    """Print a single benchmark run result line.

    Args:
        run_number: The current iteration number (1-indexed).
        total_runs: The total number of planned iterations.
        time_s: The measured execution time for this run, in seconds.
    """
    print(f"   run {run_number:3d}/{total_runs}: {time_s:.4f}s")


def summary_stats(
    mean_s: float,
    median_s: float,
    stdev_s: float,
    min_s: float,
    max_s: float,
    p5_s: float,
    p95_s: float,
) -> None:
    """Print formatted benchmark summary statistics for a completed time benchmark.

    Displays two lines of descriptive statistics: the first with central tendency
    and dispersion (mean, median, standard deviation), and the second with range
    and tail percentiles (min, max, 5th percentile, 95th percentile).

    Args:
        mean_s: Arithmetic mean of execution times, in seconds.
        median_s: Median (50th percentile) of execution times, in seconds.
        stdev_s: Sample standard deviation of execution times, in seconds.
        min_s: Minimum observed execution time, in seconds.
        max_s: Maximum observed execution time, in seconds.
        p5_s: 5th percentile of execution times, in seconds.
        p95_s: 95th percentile of execution times, in seconds.
    """
    print(f"   mean: {mean_s:.4f}s | median: {median_s:.4f}s | stdev: {stdev_s:.4f}s")
    print(f"   min:  {min_s:.4f}s | max:    {max_s:.4f}s | p5: {p5_s:.4f}s | p95: {p95_s:.4f}s")


def table(
    headers: list[str],
    rows: list[list[str]],
    col_widths: list[int] | None = None,
    alignments: list[str] | None = None,
) -> None:
    """Print a formatted table with headers, separator, and aligned rows.

    Args:
        headers: Column header strings.
        rows: List of row data (each row is a list of strings).
        col_widths: Optional per-column widths. If None, auto-computed from data.
        alignments: Optional per-column alignment ('<' for left, '>' for right).
            Defaults to left-aligned for all columns.
    """
    num_cols = len(headers)

    if col_widths is None:
        col_widths = [len(h) + 2 for h in headers]
        for row in rows:
            for i, cell in enumerate(row[:num_cols]):
                col_widths[i] = max(col_widths[i], len(cell) + 2)

    if alignments is None:
        alignments = ["<"] * num_cols

    header_line = ""
    for i, h in enumerate(headers):
        header_line += f"{h:{alignments[i]}{col_widths[i]}}"
    print(header_line)
    print(SEP_CHAR * len(header_line))

    for row in rows:
        line = ""
        for i, cell in enumerate(row[:num_cols]):
            line += f"{cell:{alignments[i]}{col_widths[i]}}"
        print(line)


def blank() -> None:
    """Print an empty line."""
    print()
