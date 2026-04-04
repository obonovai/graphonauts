"""TPC-H Scale Factor 1 entity specifications and CSV reading utilities.

The TPC-H (Transaction Processing Performance Council -- Ad Hoc) benchmark
is an industry-standard decision support benchmark that models a wholesale
supplier's business operations. The dataset at Scale Factor 1 comprises
8 entity types and approximately 7.5 million nodes with 21 million
relationships when mapped to a property graph model.

This module serves as the single source of truth for TPC-H data schema
definitions within the framework. It fulfils two responsibilities:

1. **Schema specification**: Each of the 8 TPC-H entity types is described
   by an ``EntitySpec`` dataclass instance that enumerates column names,
   data types, and date parsing requirements. These specifications are
   database-agnostic -- they describe the source CSV files produced by
   the ``dbgen`` tool, not the target graph schema.

2. **Data reading utilities**: Two reader functions (``read_entity`` for
   small tables, ``read_entity_chunks`` for large tables) provide pandas-
   based CSV parsing that all database loader implementations share. The
   chunked reader is essential for the largest entities (orders at 1.5M
   rows, line items at 6M rows) where loading the entire file into memory
   would be impractical.

The TPC-H ``dbgen`` tool produces pipe-delimited ``.tbl`` files with a
trailing pipe on each line, resulting in an extra empty column that must
be accounted for in the column specification (the ``_extra`` sentinel).

The entity specifications are intentionally identical across all target
databases -- only the loading queries and transaction chunk sizes differ
between implementations.

TPC-H Scale Factor 1 totals:
    ~7.5 million nodes and ~21 million relationships.
"""

import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pandas as pd  # type: ignore

from graphonauts import TPCH_PATH


@dataclass(frozen=True)
class EntitySpec:
    """Schema specification for a single TPC-H entity's pipe-delimited CSV file.

    Each ``EntitySpec`` instance fully describes the structure of one ``.tbl``
    file produced by the TPC-H ``dbgen`` tool, providing all the metadata
    required by ``pandas.read_csv`` to correctly parse the file. The
    ``frozen=True`` decorator ensures immutability, as these specifications
    are module-level constants shared across the entire application.

    The distinction between ``columns`` and ``usecols`` addresses a TPC-H
    data format quirk: each line ends with a pipe delimiter, which pandas
    interprets as an additional empty column. The ``columns`` list includes
    a ``_extra`` sentinel for this trailing column, while ``usecols`` excludes
    it to produce clean DataFrames.

    Attributes:
        name: Human-readable entity name used in progress messages and as a
            key in ``ENTITY_SPECS`` and ``ENTITY_ROW_COUNTS`` dictionaries
            (e.g., ``'regions'``, ``'line_items'``).
        filename: Name of the pipe-delimited ``.tbl`` file in the TPC-H data
            directory (e.g., ``'region.tbl'``, ``'lineitem.tbl'``).
        columns: Complete list of column names in the CSV, including the
            trailing ``'_extra'`` column that results from TPC-H's trailing
            pipe delimiter. This list must match the number of fields per
            line in the source file.
        usecols: Subset of ``columns`` to actually load into the DataFrame,
            excluding the ``'_extra'`` sentinel. Only these columns are
            passed to the database loader.
        dtypes: Explicit pandas dtype mapping for non-date columns. Providing
            explicit types avoids pandas' dtype inference, which can
            misinterpret numeric strings or produce inconsistent types across
            chunks in chunked reading mode.
        parse_dates: Column names to parse as ``date`` objects. Dates are
            stored as native date values in the graph database to enable
            correct temporal comparison operators in benchmark queries.
            Defaults to an empty list for entities without date columns.
    """

    name: str
    filename: str
    columns: list[str]
    usecols: list[str]
    dtypes: dict[str, Any]
    parse_dates: list[str] = field(default_factory=list)


# --- TPC-H Scale Factor 1 Entity Specifications ---

REGION = EntitySpec(
    name="regions",
    filename="region.tbl",
    columns=["regionkey", "name", "comment", "_extra"],
    usecols=["regionkey", "name", "comment"],
    dtypes={"regionkey": int, "name": str, "comment": str},
)

NATION = EntitySpec(
    name="nations",
    filename="nation.tbl",
    columns=["nationkey", "name", "regionkey", "comment", "_extra"],
    usecols=["nationkey", "name", "regionkey", "comment"],
    dtypes={"nationkey": int, "name": str, "regionkey": int, "comment": str},
)

SUPPLIER = EntitySpec(
    name="suppliers",
    filename="supplier.tbl",
    columns=["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment", "_extra"],
    usecols=["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"],
    dtypes={
        "suppkey": int,
        "name": str,
        "address": str,
        "nationkey": int,
        "phone": str,
        "acctbal": float,
        "comment": str,
    },
)

CUSTOMER = EntitySpec(
    name="customers",
    filename="customer.tbl",
    columns=["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "_extra"],
    usecols=["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"],
    dtypes={
        "custkey": int,
        "name": str,
        "address": str,
        "nationkey": int,
        "phone": str,
        "acctbal": float,
        "mktsegment": str,
        "comment": str,
    },
)

PART = EntitySpec(
    name="parts",
    filename="part.tbl",
    columns=["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment", "_extra"],
    usecols=["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment"],
    dtypes={
        "partkey": int,
        "name": str,
        "mfgr": str,
        "brand": str,
        "type": str,
        "size": int,
        "container": str,
        "retailprice": float,
        "comment": str,
    },
)

PARTSUPP = EntitySpec(
    name="part_suppliers",
    filename="partsupp.tbl",
    columns=["partkey", "suppkey", "availqty", "supplycost", "comment", "_extra"],
    usecols=["partkey", "suppkey", "availqty", "supplycost", "comment"],
    dtypes={"partkey": int, "suppkey": int, "availqty": int, "supplycost": float, "comment": str},
)

ORDER = EntitySpec(
    name="orders",
    filename="orders.tbl",
    columns=[
        "orderkey",
        "custkey",
        "orderstatus",
        "totalprice",
        "orderdate",
        "orderpriority",
        "clerk",
        "shippriority",
        "comment",
        "_extra",
    ],
    usecols=[
        "orderkey",
        "custkey",
        "orderstatus",
        "totalprice",
        "orderdate",
        "orderpriority",
        "clerk",
        "shippriority",
        "comment",
    ],
    dtypes={
        "orderkey": int,
        "custkey": int,
        "orderstatus": str,
        "totalprice": float,
        "orderpriority": str,
        "clerk": str,
        "shippriority": int,
        "comment": str,
    },
    parse_dates=["orderdate"],
)

LINEITEM = EntitySpec(
    name="line_items",
    filename="lineitem.tbl",
    columns=[
        "orderkey",
        "partkey",
        "suppkey",
        "linenumber",
        "quantity",
        "extendedprice",
        "discount",
        "tax",
        "returnflag",
        "linestatus",
        "shipdate",
        "commitdate",
        "receiptdate",
        "shipinstruct",
        "shipmode",
        "comment",
        "_extra",
    ],
    usecols=[
        "orderkey",
        "partkey",
        "suppkey",
        "linenumber",
        "quantity",
        "extendedprice",
        "discount",
        "tax",
        "returnflag",
        "linestatus",
        "shipdate",
        "commitdate",
        "receiptdate",
        "shipinstruct",
        "shipmode",
        "comment",
    ],
    dtypes={
        "orderkey": int,
        "partkey": int,
        "suppkey": int,
        "linenumber": int,
        "quantity": float,
        "extendedprice": float,
        "discount": float,
        "tax": float,
        "returnflag": str,
        "linestatus": str,
        "shipinstruct": str,
        "shipmode": str,
        "comment": str,
    },
    parse_dates=["shipdate", "commitdate", "receiptdate"],
)

# All entity specifications, ordered by loading dependency. Entities appearing
# earlier in the dictionary must be loaded before those appearing later, as
# later entities create relationships referencing earlier ones (e.g., Nation
# nodes reference Region nodes via BELONGS_TO relationships).
ENTITY_SPECS: dict[str, EntitySpec] = {
    "regions": REGION,
    "nations": NATION,
    "suppliers": SUPPLIER,
    "customers": CUSTOMER,
    "parts": PART,
    "part_suppliers": PARTSUPP,
    "orders": ORDER,
    "line_items": LINEITEM,
}

# Known row counts for TPC-H Scale Factor 1, used for progress bar calculation
# during data loading. These counts are deterministic for a given scale factor
# and are derived from the TPC-H specification.
ENTITY_ROW_COUNTS: dict[str, int] = {
    "regions": 5,
    "nations": 25,
    "suppliers": 10_000,
    "customers": 150_000,
    "parts": 200_000,
    "part_suppliers": 800_000,
    "orders": 1_500_000,
    "line_items": 6_001_215,
}

# Shared chunk sizes for chunked CSV reading during data loading. All database
# loaders use these values to ensure identical batch counts across databases,
# enabling fair comparison of loading performance. Values are constrained by
# Memgraph, which processes the entire UNWIND in a single in-memory transaction.
CHUNK_SIZE = 5_000
CHUNK_SIZE_PARTSUPP = 2_000
CHUNK_SIZE_ORDERS = 2_000
CHUNK_SIZE_LINEITEM = 1_000


def read_entity(spec: EntitySpec) -> list[dict[str, Any]]:
    """Read an entire TPC-H entity file into a list of row dicts.

    Suitable for small tables (regions, nations, suppliers) that fit in memory
    as a single batch.

    Args:
        spec: Entity schema specification.

    Returns:
        List of dicts, one per row, with column names as keys.
    """
    df = pd.read_csv(
        TPCH_PATH / spec.filename,
        delimiter="|",
        header=None,
        names=spec.columns,
        usecols=spec.usecols,
        dtype=spec.dtypes,
        parse_dates=spec.parse_dates if spec.parse_dates else False,
    )
    for col in spec.parse_dates:
        df[col] = df[col].dt.date
    return df.to_dict("records")  # type: ignore[no-any-return]


def read_entity_chunks(spec: EntitySpec, chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    """Read a TPC-H entity file in chunks, yielding batches of row dicts.

    Suitable for large tables (customers, parts, orders, lineitems) that must be
    loaded in batches to manage memory and transaction sizes.

    Args:
        spec: Entity schema specification.
        chunk_size: Number of CSV rows per pandas chunk.

    Yields:
        Lists of dicts, each list containing up to chunk_size rows.
    """
    chunks = pd.read_csv(
        TPCH_PATH / spec.filename,
        delimiter="|",
        header=None,
        names=spec.columns,
        usecols=spec.usecols,
        dtype=spec.dtypes,
        parse_dates=spec.parse_dates if spec.parse_dates else False,
        chunksize=chunk_size,
    )
    for chunk in chunks:
        for col in spec.parse_dates:
            chunk[col] = chunk[col].dt.date
        yield chunk.to_dict("records")


def total_batches(entity_name: str, chunk_size: int) -> int:
    """Calculate total number of batches for progress display.

    Args:
        entity_name: Key into ENTITY_ROW_COUNTS (e.g., 'customers').
        chunk_size: Rows per batch.

    Returns:
        Number of batches (rounded up).
    """
    return math.ceil(ENTITY_ROW_COUNTS[entity_name] / chunk_size)
