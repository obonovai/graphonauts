"""Run queries and save fetched records for cross-database comparison.

Results are saved to benchmarks/results/{db}/{category}/{variant}/results.json.
No timestamp directory -- this is reference data, the latest run is canonical.
"""

import asyncio
import json
from pathlib import Path

import click

from graphonauts.base.client import BaseClient
from graphonauts.base.config import DatabaseConfig
from graphonauts.base.query import Query
from graphonauts.commands import BENCHMARKS_PATH, DB_MODULES, QUERY_CATEGORIES
from graphonauts.commands._common import benchmark_session, filter_queries
from utils import printer


async def _save_query_results(
    client: BaseClient,
    query: Query,
    config: DatabaseConfig,
) -> Path:
    """Run a query once and save the fetched records."""
    printer.header(f"Saving Results | Query {query.category} {query.variant}")
    printer.info(query.description)
    printer.separator()

    if query.setup:
        await query.setup(client)

    records = await client.afetch(query.query, query.params)

    if query.teardown:
        await query.teardown(client)

    result_dir = BENCHMARKS_PATH / "results" / config.name / query.category / str(query.variant)
    result_dir.mkdir(parents=True, exist_ok=True)
    result_file = result_dir / "results.json"

    with open(result_file, "w") as f:
        json.dump(
            {
                "database": config.name,
                "query_category": query.category,
                "query_variant": query.variant,
                "description": query.description,
                "total": len(records),
                "records": records,
            },
            f,
            indent=2,
            default=str,
        )

    printer.key_value("records", len(records))
    printer.key_value("saved", str(result_file))

    return result_file


async def _save_results(db_name: str, category: str | None, query: int | None) -> None:
    async with benchmark_session(db_name) as (config, client, queries):
        keys = filter_queries(queries, category, query, db_name)
        for key in keys:
            await _save_query_results(client, queries[key], config)


@click.command()
@click.argument("db", type=click.Choice(list(DB_MODULES.keys()), case_sensitive=False))
@click.option(
    "--category",
    type=click.Choice(QUERY_CATEGORIES, case_sensitive=False),
    default=None,
    help="Query category (e.g., join, selection)",
)
@click.option("--query", type=int, default=None, help="Specific variant number within the category")
def save(db: str, category: str | None, query: int | None) -> None:
    """Run queries and save fetched records for cross-database comparison."""
    asyncio.run(_save_results(db, category, query))
