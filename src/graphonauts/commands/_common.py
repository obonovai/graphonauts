"""Shared helpers for CLI command implementations.

This module centralises the utility functions that are common to multiple CLI
commands (benchmark, compare, load, verify). It provides:

- **Dynamic module loading**: importing database-specific modules by name at runtime,
  enabling the protocol-based extensibility pattern used throughout the framework.
- **Client and loader discovery**: introspecting database modules to locate and
  instantiate concrete ``Client`` and ``Loader`` classes without hard-coding their
  names.
- **Query filtering**: selecting queries by category and/or variant number from
  user-supplied ``--category`` and ``--query`` CLI options.
- **Benchmark session context manager**: an async context manager that handles the
  client connection lifecycle (connect on entry, close on exit) and yields the
  configuration, client, and query dictionary as a single tuple.
- **Path utilities**: timestamp generation for result directory names.
"""

import importlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import click

from graphonauts.base.config import DatabaseConfig
from graphonauts.base.query import Query
from graphonauts.commands import DB_MODULES


def load_db_module(db_name: str) -> Any:
    """Dynamically import a database module by its registered short name.

    Uses ``importlib.import_module`` to load the top-level package for the
    specified database (e.g., ``graphonauts.neo4j_db`` for ``"neo4j"``). The
    mapping from short names to fully-qualified module paths is maintained in
    the ``DB_MODULES`` dictionary.

    Args:
        db_name: The canonical short name of the database (e.g., ``"neo4j"``,
            ``"memgraph"``).

    Returns:
        The imported module object, which is expected to expose a ``config``
        attribute of type ``DatabaseConfig``.

    Raises:
        click.BadParameter: If ``db_name`` is not present in ``DB_MODULES``.
    """
    if db_name not in DB_MODULES:
        raise click.BadParameter(f"Unknown database: {db_name}. Available: {', '.join(DB_MODULES)}")
    return importlib.import_module(DB_MODULES[db_name])


def get_client(db_name: str) -> Any:
    """Discover and instantiate the ``Client`` class from a database module.

    Introspects the ``{module}.client`` submodule and locates a class whose name
    ends with ``"Client"`` (excluding ``BaseClient``). This convention-based
    discovery avoids hard-coding class names and supports the framework's
    protocol-based extensibility.

    Args:
        db_name: The canonical short name of the database (e.g., ``"neo4j"``).

    Returns:
        A newly constructed instance of the discovered ``Client`` class.

    Raises:
        click.ClickException: If no suitable ``Client`` class is found.
    """
    client_mod = importlib.import_module(f"{DB_MODULES[db_name]}.client")
    for name in dir(client_mod):
        obj = getattr(client_mod, name)
        if isinstance(obj, type) and name.endswith("Client") and name != "BaseClient":
            return obj()
    raise click.ClickException(f"No Client class found in {DB_MODULES[db_name]}.client")


def get_queries(db_name: str) -> dict[tuple[str, int], Query]:
    """Load the ``QUERIES`` dictionary from a database module's queries submodule.

    Each database module is expected to provide a ``queries`` submodule that
    exports a ``QUERIES`` dictionary mapping ``(category, variant)`` tuples to
    ``Query`` dataclass instances.

    Args:
        db_name: The canonical short name of the database (e.g., ``"neo4j"``).

    Returns:
        A dictionary mapping ``(category, variant)`` tuples to ``Query``
        dataclass instances containing the query string, parameters, description,
        and optional setup/teardown callables.
    """
    queries_mod = importlib.import_module(f"{DB_MODULES[db_name]}.queries")
    return queries_mod.QUERIES  # type: ignore[no-any-return]


def get_loader(db_name: str, client: Any) -> Any:
    """Discover and instantiate the ``Loader`` class from a database module.

    Introspects the ``{module}.loader`` submodule and locates a class whose name
    ends with ``"Loader"`` (excluding ``BaseLoader``). The loader is instantiated
    with the provided client as its sole constructor argument.

    Args:
        db_name: The canonical short name of the database (e.g., ``"neo4j"``).
        client: A connected client instance to pass to the loader constructor.

    Returns:
        A newly constructed instance of the discovered ``Loader`` class.

    Raises:
        click.ClickException: If no suitable ``Loader`` class is found.
    """
    loader_mod = importlib.import_module(f"{DB_MODULES[db_name]}.loader")
    for name in dir(loader_mod):
        obj = getattr(loader_mod, name)
        if isinstance(obj, type) and name.endswith("Loader") and name != "BaseLoader":
            return obj(client)
    raise click.ClickException(f"No Loader class found in {DB_MODULES[db_name]}.loader")


def filter_queries(
    queries: dict[tuple[str, int], Query],
    category: str | None,
    query: int | None,
    db_name: str,
) -> list[tuple[str, int]]:
    """Filter query keys by category and/or variant number.

    Supports three modes:

    - Neither ``category`` nor ``query`` given: returns all keys sorted.
    - Only ``category`` given: returns all keys matching that category.
    - Both given: returns the single matching key ``(category, query)``.

    Args:
        queries: The full dictionary of available queries for the target database.
        category: Optional query category name (e.g., ``"join"``, ``"selection"``).
        query: Optional variant number within the category.
        db_name: The database name, used in error messages.

    Returns:
        A sorted list of ``(category, variant)`` keys.

    Raises:
        click.ClickException: If ``--query`` is given without ``--category``,
            or if the specified query is not found.
    """
    if query is not None and category is None:
        raise click.ClickException("--query requires --category to be specified")

    if category is None:
        return sorted(queries)

    if query is not None:
        key = (category, query)
        if key not in queries:
            raise click.ClickException(f"Query {category} {query} not found for {db_name}")
        return [key]

    keys = sorted(k for k in queries if k[0] == category)
    if not keys:
        raise click.ClickException(f"No queries found for category '{category}' in {db_name}")
    return keys


@asynccontextmanager
async def benchmark_session(db_name: str) -> AsyncIterator[tuple[DatabaseConfig, Any, dict[tuple[str, int], Query]]]:
    """Async context manager that manages the database client lifecycle for benchmarking.

    On entry, the database module is dynamically loaded, the client and query
    dictionary are instantiated, and the client's synchronous ``connect()`` method
    is invoked. On exit (including on exception), the client's ``aclose()`` coroutine
    is awaited to release the connection.

    Args:
        db_name: The canonical short name of the database (e.g., ``"neo4j"``).

    Yields:
        A three-element tuple of ``(config, client, queries)`` where ``config`` is the
        ``DatabaseConfig``, ``client`` is the connected client instance, and ``queries``
        is the dictionary of available ``Query`` objects keyed by ``(category, variant)``.

    Usage::

        async with benchmark_session("neo4j") as (config, client, queries):
            await run_memory_benchmark(client, queries[("selection", 1)], config)
    """
    mod = load_db_module(db_name)
    config: DatabaseConfig = mod.config
    queries = get_queries(db_name)
    client = get_client(db_name)
    client.connect()
    try:
        yield config, client, queries
    finally:
        await client.aclose()


def timestamp() -> str:
    """Generate a human-readable timestamp string for use in result directory names.

    The format ``YYYY-MM-DD_HH-MM-SS`` is chosen to be both lexicographically
    sortable (enabling the "latest directory" discovery strategy) and filesystem-safe
    (no colons or spaces).

    Returns:
        A string representing the current local time in ``YYYY-MM-DD_HH-MM-SS`` format.
    """
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
