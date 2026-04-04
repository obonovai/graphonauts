"""Dataclass representing a single benchmark query within the TPC-H evaluation suite.

Each database module (e.g., ``neo4j_db``, ``memgraph_db``) defines a ``QUERIES``
dictionary mapping ``(category, variant)`` tuples to ``Query`` instances that
contain the database-specific query string and associated metadata. Categories
are string identifiers (``"selection"``, ``"aggregation"``, ``"join"``, ``"set"``,
``"modification"``), each containing multiple variants that test different aspects
of the category.

The ``Query`` dataclass also supports optional ``setup`` and ``teardown``
callables, which are used primarily for index management. Queries that evaluate
indexed versus non-indexed performance provide setup functions that create the
relevant index and teardown functions that remove it. This mechanism ensures
that index state does not leak between benchmark runs.
"""

from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Query:
    """Encapsulates a single benchmark query with its metadata and lifecycle hooks.

    The ``Query`` dataclass serves as the fundamental unit of work in the
    benchmarking framework. It pairs a database-specific query string with
    the metadata needed for result organisation, progress reporting, and
    optional index management.

    The 19 benchmark queries are organised into a two-dimensional taxonomy:

    - **Selection**: Queries that filter nodes or relationships by property
      values, testing predicate push-down and index utilisation.
    - **Aggregation**: Queries involving ``COUNT``, ``SUM``, ``AVG``, and
      ``GROUP BY`` operations on graph data.
    - **Join**: Queries that traverse multiple relationship types, evaluating
      the database's join and path-traversal performance.
    - **Set**: Queries using ``UNION``, ``INTERSECT``, or equivalent
      set-algebraic constructs.
    - **Modification**: Queries testing sorting (indexed/non-indexed) and
      ``DISTINCT`` result sets.

    Attributes:
        category: String identifier for the query category (``"selection"``,
            ``"aggregation"``, ``"join"``, ``"set"``, ``"modification"``),
            following the taxonomy described above.
        variant: Integer identifier for the specific variant within its category
            (e.g., category="selection", variant=1 through variant=4 for four
            selection query variants).
        description: A concise, human-readable description of the query's purpose
            and the aspect of database performance it evaluates. Used in benchmark
            result reports and progress output.
        query: The database-specific query string (e.g., Cypher for Neo4j,
            openCypher for Memgraph). This string may contain parameterised
            placeholders whose values are supplied via the ``params`` attribute.
        params: A dictionary of parameter name-value pairs for parameterised
            query execution. Defaults to an empty dictionary. Parameterised
            queries prevent injection and may enable query plan caching.
        setup: An optional asynchronous callable invoked before the benchmark
            measurement begins. It receives the database client as its sole
            argument. Typically used to create indexes required by the query.
            In memory benchmarks, setup runs before the container restart so
            that indexes persist through the restart cycle.
        teardown: An optional asynchronous callable invoked after the benchmark
            measurement completes. It receives the database client as its sole
            argument. Typically used to drop indexes created during setup,
            restoring the database to its pre-benchmark state.
    """

    category: str
    variant: int
    description: str
    query: str
    params: dict[str, Any] = field(default_factory=dict)
    setup: Callable[..., Coroutine[Any, Any, None]] | None = None
    teardown: Callable[..., Coroutine[Any, Any, None]] | None = None
