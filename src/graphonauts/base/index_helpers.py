"""Factory functions for creating index setup and teardown callables.

Several benchmark queries are designed to evaluate the performance impact of
database indexes. These queries require an index to be created before the
benchmark measurement begins and removed afterward to prevent index state
from leaking into subsequent measurements. This module provides factory
functions that generate the requisite asynchronous setup and teardown
callables from DDL (Data Definition Language) strings.

The factory pattern eliminates repetitive boilerplate in each database
module's ``queries.py``. Without these helpers, every indexed query would
need to define its own pair of ``async def setup`` / ``async def teardown``
functions with identical structure, differing only in the DDL string.

Implementation note:
    Databases that create indexes asynchronously (e.g., Neo4j) should provide
    an ``await_command`` to ``make_index_setup`` that blocks until the index
    is fully populated. Databases that create indexes synchronously (e.g.,
    Memgraph) can omit the parameter; a 3-second fallback sleep is used as
    a best-effort delay.

    For teardown, a 1-second ``asyncio.sleep`` is inserted after each DDL
    execution to allow the database engine to complete index removal.

Usage example::

    from graphonauts.base.index_helpers import make_index_setup, make_index_teardown

    # Neo4j: asynchronous index creation with explicit await
    QUERIES = {
        (1, 3): Query(
            ...,
            setup=make_index_setup(
                "CREATE INDEX idx FOR (o:Order) ON (o.orderdate)",
                await_command="CALL db.awaitIndexes(300)",
            ),
            teardown=make_index_teardown("DROP INDEX idx IF EXISTS"),
        ),
    }

    # Memgraph: synchronous index creation (no await_command needed)
    QUERIES = {
        (1, 3): Query(
            ...,
            setup=make_index_setup("CREATE INDEX ON :Order(orderdate)"),
            teardown=make_index_teardown("DROP INDEX ON :Order(orderdate)"),
        ),
    }
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from graphonauts.base.client import BaseClient


def make_index_setup(ddl: str, await_command: str | None = None) -> Callable[[BaseClient], Coroutine[Any, Any, None]]:
    """Create an asynchronous index setup callable from a DDL string.

    The returned callable, when invoked with a database client, executes the
    provided DDL statement and then either runs a database-specific await
    command to block until the index is fully populated, or falls back to a
    fixed sleep if no await command is provided.

    Some databases (e.g., Neo4j) process ``CREATE INDEX`` asynchronously --
    the DDL returns immediately while index construction continues in the
    background. For these databases, an ``await_command`` such as
    ``"CALL db.awaitIndexes(300)"`` should be provided to guarantee the index
    is online before the benchmark begins. Databases that build indexes
    synchronously (e.g., Memgraph) can omit the parameter.

    Args:
        ddl: A database-specific DDL statement that creates the index
            (e.g., ``CREATE INDEX ... FOR (n:Label) ON (n.property)``).
        await_command: Optional command to execute after the DDL that blocks
            until the index is fully built. When provided, replaces the
            fallback sleep. When ``None``, a 3-second sleep is used as a
            best-effort delay for databases without an explicit await
            mechanism.

    Returns:
        An asynchronous callable with the signature
        ``async (BaseClient) -> None`` suitable for use as the ``setup``
        field of a ``Query`` dataclass.
    """

    async def setup(client: BaseClient) -> None:
        await client.aexecute(ddl)
        if await_command:
            await client.aexecute(await_command)
        else:
            await asyncio.sleep(3)

    return setup


def make_index_teardown(ddl: str) -> Callable[[BaseClient], Coroutine[Any, Any, None]]:
    """Create an asynchronous index teardown callable from a DDL string.

    The returned callable, when invoked with a database client, executes the
    provided DDL statement and then pauses for 1 second to allow the database
    engine to complete asynchronous index removal and cleanup.

    Args:
        ddl: A database-specific DDL statement that drops the index
            (e.g., ``DROP INDEX idx_name IF EXISTS``).

    Returns:
        An asynchronous callable with the signature
        ``async (BaseClient) -> None`` suitable for use as the ``teardown``
        field of a ``Query`` dataclass.
    """

    async def teardown(client: BaseClient) -> None:
        await client.aexecute(ddl)
        await asyncio.sleep(1)

    return teardown
