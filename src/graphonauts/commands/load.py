"""TPC-H data loading into a graph database with concurrent timing and memory monitoring.

This module orchestrates the ingestion of TPC-H Scale Factor 1 data into the target
graph database. During loading, both wall-clock time and container memory consumption
are recorded simultaneously. Unlike the query memory benchmark, the container is
**not** restarted before loading --- the measurement captures the incremental memory
cost of populating an already-running (but empty) database instance.

The loading process delegates to the database-specific ``Loader`` implementation, which
handles schema creation, entity dependency ordering, chunked reading of pipe-delimited
TPC-H ``.tbl`` files, and batch transaction management. Progress is reported to stdout
via the ``printer`` utility.

Results --- including total loading time, baseline memory, peak memory, and memory
delta --- are serialized as a ``LoadResult`` JSON file and accompanied by a memory
timeline PNG plot, both saved to ``benchmarks/load/{db}/{timestamp}/``.
"""

import asyncio
import time

import click

from graphonauts.base.results import LoadResult
from graphonauts.commands import BENCHMARKS_PATH, DB_MODULES
from graphonauts.commands._common import get_client, get_loader, load_db_module, timestamp
from utils import MemoryMonitor, TimeMonitor, printer


async def _load_data(db_name: str) -> None:
    """Perform the complete data loading workflow for the specified database.

    Resolves the database module, instantiates the client and loader, executes the
    loader's ``aload()`` coroutine under concurrent time and memory monitoring,
    and persists the resulting ``LoadResult`` and memory timeline plot.

    Args:
        db_name: The canonical name of the target database as registered in ``DB_MODULES``
            (e.g., ``"neo4j"``, ``"memgraph"``).
    """
    mod = load_db_module(db_name)
    config = mod.config
    client = get_client(db_name)
    loader = get_loader(db_name, client)

    printer.header(f"Loading TPC-H data into {config.name}")
    printer.separator()

    with MemoryMonitor(container_name=config.container_name) as memory_monitor:
        with TimeMonitor() as tm:
            await loader.aload()

    result_path = BENCHMARKS_PATH / "load" / config.name / timestamp()

    result = LoadResult(
        database=config.name,
        timestamp=time.time(),
        total_time_s=round(tm.elapsed_time_s, 6),
        memory_baseline_mb=round(memory_monitor.baseline_mb, 6),
        memory_peak_mb=round(memory_monitor.peak_mb, 6),
        memory_usage_mb=round(memory_monitor.memory_usage_mb, 6),
    )
    result.save(result_path)
    memory_monitor.plot(save_path=str(result_path / "memory.png"), show=False)

    printer.separator()
    printer.key_value("time", f"{tm.elapsed_time_s:.2f}s")
    printer.key_value(
        "memory",
        f"{memory_monitor.memory_usage_mb:.2f} MB "
        f"(baseline: {memory_monitor.baseline_mb:.2f} MB, peak: {memory_monitor.peak_mb:.2f} MB)",
    )
    printer.key_value("saved", str(result_path))


@click.command()
@click.argument("db", type=click.Choice(list(DB_MODULES.keys()), case_sensitive=False))
def load(db: str) -> None:
    """Load TPC-H data into the database."""
    asyncio.run(_load_data(db))
