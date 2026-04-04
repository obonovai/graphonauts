"""Graphonauts -- benchmarking framework for graph databases using TPC-H queries."""

from pathlib import Path

# Path to the TPC-H .tbl data files (pipe-delimited CSVs)
TPCH_PATH = Path(__file__).parent.parent.parent / "tpch-osx" / "dbgen"
