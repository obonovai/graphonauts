"""Configuration dataclass for Docker-based database container management.

This module defines the ``DatabaseConfig`` dataclass, which encapsulates
the operational parameters needed to manage a graph database running inside
a Docker container. The benchmarking framework relies on Docker for
reproducible, isolated database deployments, and this configuration controls
how the framework interacts with the containerised database instance.

Container management is central to the benchmark methodology:

- **Memory benchmarks** restart the container before each query measurement
  to establish a clean memory baseline, ensuring that memory consumption
  attributed to a query is not confounded by residual state from prior
  queries.
- **Health checking** after container restart ensures that the database has
  fully initialised and is accepting connections before benchmark execution
  begins, preventing premature query attempts that would produce errors or
  skewed timing measurements.
"""

from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Operational configuration for a Docker-containerised graph database.

    Each supported database module provides an instance of this class that
    the benchmark runner uses to identify, monitor, and manage the database
    container throughout the benchmark lifecycle.

    Attributes:
        name: A short, unique string identifier for the database, used as
            the top-level directory name in the benchmark results hierarchy
            (e.g., ``"neo4j"``, ``"memgraph"``). Also used in CLI output
            and comparison tables.
        container_name: The Docker container name as specified in the
            corresponding ``docker-compose.yml`` file. Used by the
            ``MemoryMonitor`` to collect memory statistics via the Docker
            API and by the runner to perform container restarts.
        health_check_indicators: A list of log message substrings that, when
            all are found in the container's log output, indicate that the
            database engine has completed its startup sequence and is ready
            to accept client connections. Multiple indicators support databases
            whose readiness is signalled by a sequence of log messages rather
            than a single entry.
        health_check_timeout: The maximum duration, in seconds, to wait for
            all health check indicators to appear in the container logs after
            a restart. If this timeout is exceeded, the benchmark raises an
            error rather than proceeding with a potentially unresponsive
            database. Defaults to 60.0 seconds, which is sufficient for most
            graph databases loading persisted data from disk.
    """

    name: str
    container_name: str
    health_check_indicators: list[str]
    health_check_timeout: float = 60.0
