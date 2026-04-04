"""ArangoDB database module for the Graphonauts benchmarking framework.

This package provides the ArangoDB-specific implementation of the database abstraction
layer defined by the ``BaseClient`` and ``BaseLoader`` protocols. ArangoDB is a
multi-model database that supports document, key-value, and graph data models through
a unified query language (AQL -- ArangoDB Query Language).

Unlike Neo4j and Memgraph which use the Bolt protocol, ArangoDB communicates via HTTP
REST API on port 8529. The ``python-arango`` driver is synchronous, so all I/O calls
are wrapped with ``asyncio.to_thread()`` to satisfy the framework's async interface.

The module exposes a ``config`` object of type ``DatabaseConfig`` containing the
container name, health check indicators, and timeout settings specific to the ArangoDB
Docker deployment used in benchmarking.

Submodules:
    - ``client``: Asynchronous ArangoDB client wrapping the synchronous ``python-arango``
      driver via ``asyncio.to_thread()``.
    - ``loader``: TPC-H data loading logic using ArangoDB's batch document/edge insert API.
    - ``queries``: Benchmark query definitions in AQL syntax.
    - ``verify``: Data integrity verification queries in AQL.
"""

from graphonauts.base.config import DatabaseConfig

config = DatabaseConfig(
    name="arangodb",
    container_name="arangodb-graphonaut",
    health_check_indicators=[
        "is ready for business",
    ],
    health_check_timeout=120.0,
)
