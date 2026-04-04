"""Verification of TPC-H data integrity after loading into a graph database.

This module validates that the TPC-H dataset at Scale Factor 1 has been loaded
correctly and completely into the target graph database. It executes a series of
count queries --- one for each node label and relationship type defined in the
TPC-H schema --- and compares the returned counts against reference values that
are known a priori from the TPC-H specification.

The verification step is essential for ensuring that benchmark measurements are
conducted on a correctly populated database. Any discrepancy in entity counts
would invalidate subsequent query benchmark results, as execution time and memory
consumption are sensitive to dataset cardinality.

Expected counts are maintained in ``graphonauts.base.verify.EXPECTED_COUNTS`` and
are independent of the target database engine.
"""

import asyncio
import importlib

import click

from graphonauts.base.client import BaseClient
from graphonauts.base.verify import EXPECTED_COUNTS, VerifyQuery
from graphonauts.commands import DB_MODULES
from graphonauts.commands._common import get_client
from utils import printer


async def _run_verification(client: BaseClient, queries: list[VerifyQuery]) -> bool:
    """Execute all verification queries and report results in a formatted summary table.

    For each ``VerifyQuery`` in the provided list, the corresponding count query is
    executed against the database via the client. The returned count is compared to
    the expected value from ``EXPECTED_COUNTS``. Results are accumulated and printed
    as a table with columns for entity name, expected count, actual count, and
    pass/fail status.

    Args:
        client: An instance conforming to the ``BaseClient`` protocol, already
            connected to the target database.
        queries: A list of ``VerifyQuery`` instances, each specifying an entity name
            and the corresponding count query string for the target database engine.

    Returns:
        ``True`` if every query's actual count matches the expected count, ``False``
        if any discrepancy is detected. A summary line is printed indicating the
        total number of checks and overall pass/fail status.
    """
    passed = 0
    failed = 0
    results: list[tuple[str, int, int, bool]] = []

    for vq in queries:
        records = await client.afetch(vq.query)
        actual = records[0]["count"] if records else 0
        expected = EXPECTED_COUNTS[vq.entity]
        ok = actual == expected
        results.append((vq.entity, expected, actual, ok))
        if ok:
            passed += 1
        else:
            failed += 1

    printer.header("Verifying TPC-H data")
    printer.table(
        headers=["Entity", "Expected", "Actual", "Status"],
        rows=[
            [entity, f"{expected:>12,}", f"{actual:>12,}", "PASS" if ok else "FAIL"]
            for entity, expected, actual, ok in results
        ],
        col_widths=[20, 14, 14, 8],
        alignments=["<", ">", ">", "<"],
    )

    total = passed + failed
    if failed == 0:
        printer.info(f"Result: {passed}/{total} checks passed. ALL OK")
    else:
        printer.info(f"Result: {passed}/{total} checks passed. FAILED")

    return failed == 0


async def _verify_data(db_name: str) -> None:
    verify_mod = importlib.import_module(f"{DB_MODULES[db_name]}.verify")
    client = get_client(db_name)
    client.connect()

    try:
        success = await _run_verification(client, verify_mod.VERIFY_QUERIES)
        if not success:
            raise click.ClickException("Verification failed!")
    finally:
        await client.aclose()


@click.command()
@click.argument("db", type=click.Choice(list(DB_MODULES.keys()), case_sensitive=False))
def verify(db: str) -> None:
    """Verify TPC-H data was loaded correctly."""
    asyncio.run(_verify_data(db))
