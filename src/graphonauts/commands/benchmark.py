"""Orchestration of separated memory and time benchmark phases for graph database queries.

This module implements the core benchmarking methodology of the Graphonauts framework,
in which memory consumption measurement and execution time measurement are carried out
as strictly separated phases. The rationale for this separation is twofold:

1. **Memory benchmarks** restart the Docker container before each query execution to
   establish a clean memory baseline. The query is then executed exactly once while a
   background thread continuously samples container memory via the Docker stats
   streaming API. Restarting the container eliminates confounding factors such as JIT
   compilation artefacts, query plan caches, and residual heap allocations from prior
   operations. A single execution suffices because the quantity of interest --- peak
   memory delta above baseline --- is deterministic for a given dataset and query.

2. **Time benchmarks** restart the Docker container once per query to establish a
   known cache state, then execute the query N times (default 50) against the same
   container instance without further restarts. The first iteration captures cold-cache
   latency (immediately after restart, with empty page caches and no compiled query
   plans), while subsequent iterations capture warm-cache, steady-state performance.
   Individual run results are persisted immediately so that partial data is preserved
   if the process is interrupted. After all iterations, a statistical summary (mean,
   median, standard deviation, min, max, 5th and 95th percentiles) is computed and
   saved alongside the individual results.

Both phases share the same container restart infrastructure (``ContainerManager``),
which handles the Docker SDK restart call, readiness polling via container logs, and
settle periods. The measurement tools (``MemoryMonitor`` for memory sampling,
``TimeMonitor`` for wall-clock timing) are pure measurement components with no
lifecycle management responsibility.

If the two phases were combined --- for instance, if time measurements were taken while
the container was being restarted for memory isolation --- the JIT warmup period would
inflate early time measurements, and accumulated cache state from repeated executions
would inflate memory readings. The separated design avoids both confounding effects.

Results are persisted to ``benchmarks/{memory,time}/{db}/{category}/{variant}/{timestamp}/``
with JSON files for structured data and PNG plots for memory timelines.
"""

import asyncio
import time

import click

from graphonauts.base.client import BaseClient
from graphonauts.base.config import DatabaseConfig
from graphonauts.base.query import Query
from graphonauts.base.results import MemoryResult, TimeResult, TimeSummary
from graphonauts.commands import BENCHMARKS_PATH, DB_MODULES, QUERY_CATEGORIES, TIME_RUNS
from graphonauts.commands._common import benchmark_session, filter_queries, timestamp
from utils import ContainerManager, MemoryMonitor, TimeMonitor, printer


async def run_memory_benchmark(
    client: BaseClient,
    query: Query,
    config: DatabaseConfig,
    *,
    no_save: bool = False,
    no_restart: bool = False,
) -> MemoryResult:
    """Measure peak memory consumption for a single query execution against a clean baseline.

    This function implements the memory benchmarking protocol, which isolates the memory
    footprint of a single query by restarting the database container before execution.
    The sequence of operations is as follows:

    1. Execute ``query.setup()`` if defined (e.g., create an index). This runs before
       the container restart; for databases such as Neo4j, indexes are persisted to disk
       and survive restarts, so they remain available after the restart.
    2. The ``ContainerManager`` restarts the Docker container and waits for health check
       indicators in the container logs, ensuring the database is fully initialised.
    3. The database client reconnects to the restarted container.
    4. The ``MemoryMonitor`` context manager records a baseline memory sample and spawns
       a background thread to continuously sample container memory via the Docker stats
       streaming API.
    5. The query is executed exactly once while memory is being sampled.
    6. Upon exiting the ``MemoryMonitor`` context, monitoring stops and a final memory
       sample is recorded.
    7. Execute ``query.teardown()`` if defined (e.g., drop the index).
    8. The ``MemoryResult`` (baseline, peak, and delta in megabytes) is serialized to
       JSON, and the memory timeline is plotted and saved as a PNG image.

    Args:
        client: An instance conforming to the ``BaseClient`` protocol, used to execute
            the query against the target database.
        query: The ``Query`` dataclass containing the query string, optional parameters,
            optional setup/teardown callables, and descriptive metadata.
        config: The ``DatabaseConfig`` for the target database, providing the container
            name, health check indicators, and health check timeout.
        no_save: If ``True``, skip persisting results to disk. The benchmark
            still executes and prints results to the console, but no JSON files
            or PNG plots are written.
        no_restart: If ``True``, skip the container restart and client reconnect
            steps. The query executes against the already-running container with
            its current cache state.

    Returns:
        A ``MemoryResult`` dataclass containing the database name, query metadata,
        timestamp, and memory measurements (baseline, peak, and delta in megabytes).
    """
    printer.header(f"Memory Benchmark | Query {query.category} {query.variant}")
    printer.info(query.description)
    printer.separator()

    if query.setup:
        await query.setup(client)

    if not no_restart:
        container_manager = ContainerManager(
            container_name=config.container_name,
            health_check_indicators=config.health_check_indicators,
            health_check_timeout=config.health_check_timeout,
        )
        container_manager.restart_and_wait()
        await client.areconnect()

    with MemoryMonitor(container_name=config.container_name) as memory_monitor:
        await client.aexecute(query.query, query.params)

    if query.teardown:
        await query.teardown(client)

    result = MemoryResult(
        database=config.name,
        query_category=query.category,
        query_variant=query.variant,
        description=query.description,
        timestamp=time.time(),
        memory_baseline_mb=round(memory_monitor.baseline_mb, 6),
        memory_peak_mb=round(memory_monitor.peak_mb, 6),
        memory_usage_mb=round(memory_monitor.memory_usage_mb, 6),
    )

    if not no_save:
        result_dir = BENCHMARKS_PATH / "memory" / config.name / query.category / str(query.variant) / timestamp()
        result.save(result_dir)
        memory_monitor.plot(save_path=str(result_dir / "memory.png"), show=False)
        printer.key_value("saved", str(result_dir))

    printer.key_value(
        "memory",
        f"{memory_monitor.memory_usage_mb:.2f} MB "
        f"(baseline: {memory_monitor.baseline_mb:.2f} MB, peak: {memory_monitor.peak_mb:.2f} MB)",
    )
    printer.blank()

    return result


async def run_time_benchmark(
    client: BaseClient,
    query: Query,
    config: DatabaseConfig,
    num_runs: int = TIME_RUNS,
    *,
    no_save: bool = False,
    no_restart: bool = False,
) -> list[TimeResult]:
    """Measure query execution time over multiple iterations for statistical analysis.

    This function implements the time benchmarking protocol. The container is restarted
    once per query to establish a known cache state, and the query is then executed
    repeatedly against the same running container. The first iteration captures
    cold-cache latency (immediately after restart, with empty page caches and no
    compiled query plans), while subsequent iterations capture warm-cache, steady-state
    performance representative of a production workload.

    The query is executed via ``aexecute()``, which instructs the server to produce all
    result records but discards them on the client side (``result.consume()``). This
    isolates the measured time to server-side processing --- query planning, graph
    traversal, and result production --- and excludes client-side record deserialisation
    overhead, which would otherwise dominate for queries returning large result sets and
    mask server-side performance differences (e.g., index vs. sequential scan).

    Each individual run is timed using ``time.perf_counter()`` (a monotonic clock with
    sub-microsecond resolution, unaffected by system clock adjustments) and persisted
    immediately as ``run_NNN.json``. This incremental persistence strategy ensures that
    partial results are available even if the benchmark process is interrupted.

    After all iterations, a ``TimeSummary`` is computed with descriptive statistics
    (mean, median, standard deviation, minimum, maximum, 5th and 95th percentiles) and
    saved as ``summary.json`` alongside the individual run files.

    Setup and teardown callables (e.g., index creation and removal) are executed once,
    wrapping the entire iteration loop, rather than once per iteration. Setup runs
    before the container restart so that indexes (which are persisted to disk) survive
    the restart and are available during query execution.

    Args:
        client: An instance conforming to the ``BaseClient`` protocol, used to execute
            the query against the target database.
        query: The ``Query`` dataclass containing the query string, optional parameters,
            optional setup/teardown callables, and descriptive metadata.
        config: The ``DatabaseConfig`` for the target database, providing the database
            name and container configuration for restart and health checking.
        num_runs: The number of iterations to execute. Defaults to the framework-wide
            ``TIME_RUNS`` constant (typically 50). Higher values yield more robust
            statistical estimates but increase total benchmark duration.
        no_save: If ``True``, skip persisting results to disk. The benchmark
            still executes and prints results to the console, but no JSON files
            are written.
        no_restart: If ``True``, skip the container restart and client reconnect
            steps. The query executes against the already-running container with
            its current cache state.

    Returns:
        A list of ``TimeResult`` dataclasses, one per iteration, each containing the
        database name, query metadata, run number, timestamp, and measured execution
        time in seconds.
    """
    printer.header(f"Time Benchmark | Query {query.category} {query.variant} | {num_runs} runs")
    printer.info(query.description)
    printer.separator()

    result_dir = (
        BENCHMARKS_PATH / "time" / config.name / query.category / str(query.variant) / timestamp()
        if not no_save
        else None
    )
    results: list[TimeResult] = []

    if query.setup:
        await query.setup(client)

    if not no_restart:
        container_manager = ContainerManager(
            container_name=config.container_name,
            health_check_indicators=config.health_check_indicators,
            health_check_timeout=config.health_check_timeout,
        )
        container_manager.restart_and_wait()
        await client.areconnect()

    for i in range(1, num_runs + 1):
        with TimeMonitor() as tm:
            await client.aexecute(query.query, query.params)

        result = TimeResult(
            database=config.name,
            query_category=query.category,
            query_variant=query.variant,
            description=query.description,
            run_number=i,
            timestamp=time.time(),
            execution_time_s=round(tm.elapsed_time_s, 6),
        )
        if result_dir is not None:
            result.save(result_dir)
        results.append(result)
        printer.run_progress(i, num_runs, tm.elapsed_time_s)

    if query.teardown:
        await query.teardown(client)

    summary = TimeSummary.from_results(results)
    if result_dir is not None:
        summary.save(result_dir)

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
    if result_dir is not None:
        printer.key_value("saved", str(result_dir))

    return results


async def _run_benchmarks(
    db_name: str,
    memory: bool,
    time_bench: bool,
    run_all: bool,
    category: str | None,
    query: int | None,
    num_runs: int,
    no_save: bool,
    no_restart: bool,
) -> None:
    async with benchmark_session(db_name) as (config, client, queries):
        keys = filter_queries(queries, category, query, db_name)

        # Default (no flags) and --all both run memory + time
        run_both = run_all or (not memory and not time_bench)

        if no_restart and (run_both or memory):
            printer.warning("Memory benchmarks without container restart produce unreliable baselines.")
            printer.blank()

        if run_both:
            printer.banner(f"Benchmarking {db_name} | {len(keys)} queries | {num_runs} runs each")
            for i, key in enumerate(keys, 1):
                printer.blank()
                printer.info(f"[{i}/{len(keys)}] Query {key[0]} {key[1]}")
                await run_memory_benchmark(client, queries[key], config, no_save=no_save, no_restart=no_restart)
                await run_time_benchmark(client, queries[key], config, num_runs, no_save=no_save, no_restart=no_restart)
        elif memory:
            for key in keys:
                await run_memory_benchmark(client, queries[key], config, no_save=no_save, no_restart=no_restart)
        elif time_bench:
            for key in keys:
                await run_time_benchmark(client, queries[key], config, num_runs, no_save=no_save, no_restart=no_restart)


@click.command()
@click.argument("db", type=click.Choice(list(DB_MODULES.keys()), case_sensitive=False))
@click.option("--memory", is_flag=True, help="Run memory benchmarks")
@click.option("--time", "time_bench", is_flag=True, help="Run time benchmarks")
@click.option("--all", "run_all", is_flag=True, help="Run both memory and time benchmarks")
@click.option(
    "--category",
    type=click.Choice(QUERY_CATEGORIES, case_sensitive=False),
    default=None,
    help="Query category (e.g., join, selection)",
)
@click.option("--query", type=int, default=None, help="Specific variant number within the category")
@click.option("--runs", type=int, default=TIME_RUNS, help=f"Number of time benchmark runs (default {TIME_RUNS})")
@click.option("--no-save", is_flag=True, default=False, help="Skip saving results to disk (console output only)")
@click.option("--no-restart", is_flag=True, default=False, help="Skip container restart and cache clearing")
def benchmark(
    db: str,
    memory: bool,
    time_bench: bool,
    run_all: bool,
    category: str | None,
    query: int | None,
    runs: int,
    no_save: bool,
    no_restart: bool,
) -> None:
    """Run memory and/or time benchmarks.

    By default (no flags), runs both memory and time benchmarks with 1 run.
    Use --runs 50 for full statistical benchmarks.
    """
    asyncio.run(_run_benchmarks(db, memory, time_bench, run_all, category, query, runs, no_save, no_restart))
