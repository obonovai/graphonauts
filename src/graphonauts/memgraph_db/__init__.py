"""Memgraph database module for the Graphonauts benchmarking framework.

This package provides the Memgraph-specific implementation of the database abstraction
layer defined by the ``BaseClient`` and ``BaseLoader`` protocols. Memgraph is an
in-memory graph database compatible with the openCypher query language and the Bolt
protocol, making it structurally similar to Neo4j but with a fundamentally different
storage architecture (memory-first rather than disk-first).

The module exposes a ``config`` object of type ``DatabaseConfig`` containing the
container name, health check indicators, and timeout settings specific to the Memgraph
Docker deployment used in benchmarking.

Submodules:
    - ``client``: Asynchronous Memgraph client using the Neo4j Python driver (Bolt
      protocol compatibility).
    - ``loader``: TPC-H data loading logic for Memgraph's Cypher dialect.
    - ``queries``: Benchmark query definitions in openCypher syntax.
    - ``verify``: Data integrity verification queries.
"""

from graphonauts.base.config import DatabaseConfig

config = DatabaseConfig(
    name="memgraph",
    container_name="memgraph-graphonaut",
    health_check_indicators=[
        "Bolt server is fully armed and operational",
        "Bolt listening on",
        "Memgraph successfully started!",
    ],
    health_check_timeout=300.0,
)
