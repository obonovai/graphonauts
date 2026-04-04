# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (including Neo4j driver)
poetry install

# Start/stop Neo4j container
poe neo4j-up
poe neo4j-down

# Format code (ruff imports + formatting)
poe format-code

# Type checking (strict mode)
poetry run mypy src/

# Run tests
poetry run pytest

# Run the CLI (subcommand-based)
python -m graphonauts load neo4j
python -m graphonauts verify neo4j
python -m graphonauts benchmark neo4j
python -m graphonauts benchmark neo4j --memory --category join --query 2
python -m graphonauts benchmark neo4j --time --runs 50
python -m graphonauts benchmark neo4j --category selection --runs 50
python -m graphonauts compare neo4j memgraph
python -m graphonauts compare neo4j memgraph --metric mean
python -m graphonauts save neo4j
python -m graphonauts summarize neo4j
```

## Architecture

Benchmarking framework for graph databases using TPC-H queries. Measures query execution time and memory consumption as **separate phases** -- memory measurement restarts the container for a clean baseline (single run), time measurement runs 50 iterations without restarts.

### Core design: Protocol-based extensibility

Database implementations use Python `Protocol` (structural typing) -- no inheritance required. A new database module just needs to provide classes matching `BaseClient` and `BaseLoader` signatures, plus a `QUERIES` dict, `VERIFY_QUERIES` list, and `DatabaseConfig`.

### Module layout

- **`src/graphonauts/base/`** -- Database abstractions: Protocols (`BaseClient`, `BaseLoader`), `Query` dataclass, `DatabaseConfig`, result types (`LoadResult`, `MemoryResult`, `TimeResult`, `TimeSummary`), TPC-H schema definitions (`tpch.py`), index factory (`index_helpers.py`), verification types (`verify.py`).
- **`src/graphonauts/commands/`** -- CLI command implementations: one file per command (`load.py`, `verify.py`, `benchmark.py`, `compare.py`, `save.py`, `summarize.py`). Shared helpers in `_common.py`. Constants (`BENCHMARKS_PATH`, `QUERY_CATEGORIES`, `DB_MODULES`) in `__init__.py`.
- **`src/graphonauts/cli.py`** -- Thin Click group entry point. Registers subcommands from `commands/`.
- **`src/graphonauts/neo4j_db/`** -- Neo4j implementation: async client (Bolt protocol), TPC-H loader, 19 Cypher queries with index setup/teardown, Docker Compose config.
- **`src/graphonauts/memgraph_db/`** -- Memgraph implementation: async client (Bolt protocol), TPC-H loader (smaller chunks), 19 Memgraph-syntax queries, Docker Compose config.
- **`src/utils/`** -- `TimeMonitor` (perf_counter), `MemoryMonitor` (Docker stats API), `printer` (unified output styling).

### What's shared vs per-database

Shared (database-agnostic):
- TPC-H CSV schemas and chunked reader (`base/tpch.py`) -- column names, dtypes, file paths
- Progress and output formatting (`utils/printer.py`)
- Index setup/teardown factory (`base/index_helpers.py`)
- Benchmark orchestration (`commands/benchmark.py`, `commands/load.py`)
- Result types and comparison logic

Per-database:
- Client (connection protocol/driver)
- Loader (query language for data ingestion)
- Queries (19 benchmark queries in database-specific syntax)
- Verification queries (count queries in database-specific syntax)
- Docker Compose configuration

### Adding a new database

1. Create `src/graphonauts/{db_name}_db/` package
2. Implement: `client.py` (class matching `BaseClient`), `loader.py` (class matching `BaseLoader`, uses `base/tpch.py` for CSV reading), `queries.py` (`QUERIES` dict), `verify.py` (`VERIFY_QUERIES` list)
3. Add `__init__.py` with `DatabaseConfig` instance as `config`
4. Register in `commands/__init__.py` `DB_MODULES` dict

### Query structure

19 queries keyed by `(category, variant)` tuples where category is a string (`"selection"`, `"aggregation"`, `"join"`, `"set"`, `"modification"`). Each `Query` has optional `setup`/`teardown` async callables for index management (created via `base/index_helpers.py` factory). Setup runs before container restart in memory benchmarks -- indexes persist through database restarts.

### TPC-H data

Scale Factor 1 data in `tpch-osx/dbgen/*.tbl` (pipe-delimited). Entity specs defined in `base/tpch.py`. Loading order follows entity dependencies. Chunk sizes are per-database (Neo4j: 10k rows with concurrent transactions; Memgraph: 500-5k rows in single transactions).

## Code conventions

- Python 3.12+, async/await throughout for I/O
- mypy strict mode, ruff line-length 120
- Async methods prefixed with `a` (e.g., `afetch`, `aload`)
- `connect()` is synchronous (driver init only); all actual I/O is async
- Type annotations on all functions; `# type: ignore` only for untyped third-party libs (pandas, docker)
- All terminal output goes through `utils/printer.py` for consistent styling
- Academic-style documentation: formal docstrings with parameter descriptions, return types, design rationale, and methodology explanations (this is a diploma thesis project)
