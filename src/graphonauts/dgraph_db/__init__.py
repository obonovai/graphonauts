"""Dgraph database module for the Graphonauts benchmarking framework.

This package provides the Dgraph-specific implementation of the database abstraction
layer defined by the ``BaseClient`` and ``BaseLoader`` protocols. Dgraph is a native
graph database that uses gRPC for client communication and DQL (Dgraph Query Language)
for queries.

Unlike Neo4j and Memgraph which use the Bolt protocol, and ArangoDB which uses HTTP,
Dgraph communicates via gRPC on port 9080. The ``pydgraph`` driver is synchronous,
so all I/O calls are wrapped with ``asyncio.to_thread()`` to satisfy the framework's
async interface.

Dgraph requires two services: Dgraph Zero (cluster coordinator) and Dgraph Alpha
(data server). The ``container_name`` refers to the Alpha service, where data is stored
and queries are executed.

The module exposes a ``config`` object of type ``DatabaseConfig`` containing the
container name, health check indicators, and timeout settings specific to the Dgraph
Docker deployment used in benchmarking.

Submodules:
    - ``client``: Asynchronous Dgraph client wrapping the synchronous ``pydgraph``
      driver via ``asyncio.to_thread()``.
    - ``loader``: TPC-H data loading logic using Dgraph JSON mutations.
    - ``verify``: Data integrity verification queries in DQL.
"""

from graphonauts.base.config import DatabaseConfig

config = DatabaseConfig(
    name="dgraph",
    container_name="dgraph-alpha-graphonaut",
    health_check_indicators=[
        "Server is ready: OK",
    ],
    health_check_timeout=900,
)
