"""CLI command implementations for the Graphonauts benchmarking framework.

Each command is a separate module with a Click command function that gets registered
in the main CLI group. Shared constants and helpers live in this package's __init__
and _common module.
"""

from pathlib import Path

# All benchmark results are stored under this centralized directory
BENCHMARKS_PATH = Path(__file__).parent.parent.parent.parent / "benchmarks"

# Default number of time benchmark iterations per query.
# Set to 1 for safe, quick runs. Use --runs 50 for full statistical benchmarks.
TIME_RUNS = 1

# Canonical query type names (singular) used as internal identifiers and CLI input
QUERY_CATEGORIES: list[str] = ["selection", "aggregation", "join", "set", "modification"]

# Registry of supported databases. Each value is the Python module path.
# To add a new database, create the module and add an entry here.
DB_MODULES: dict[str, str] = {
    "neo4j": "graphonauts.neo4j_db",
    "memgraph": "graphonauts.memgraph_db",
    "arangodb": "graphonauts.arangodb_db",
}
