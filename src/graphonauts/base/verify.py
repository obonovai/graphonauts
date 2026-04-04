"""Data verification types and expected counts for TPC-H Scale Factor 1.

After loading TPC-H data into a graph database, it is essential to verify that
the ingestion was complete and correct before proceeding with benchmark
measurements. This module provides the reference data and query abstraction
for that verification step.

The verification methodology is straightforward: for each node label and
relationship type in the graph schema, a count query is executed and the
result is compared against the known expected count for TPC-H Scale Factor 1.
Discrepancies indicate one of several potential issues:

- **Under-count**: Partial load due to transaction failures, connectivity
  interruptions, or premature termination of the loading process.
- **Over-count**: Duplicate records introduced by re-running the loader
  without first clearing the database, or by incorrect ``MERGE`` vs
  ``CREATE`` semantics in the loading queries.
- **Missing entity type**: Complete absence of a node label or relationship
  type, typically caused by a skipped loading step or a schema mismatch.

The ``EXPECTED_COUNTS`` dictionary and the ``VerifyQuery`` dataclass are
consumed by each database module's ``verify.py``, which provides
database-specific count query syntax (e.g., Cypher ``MATCH ... RETURN count(*)``).
"""

from dataclasses import dataclass

# Expected counts for TPC-H Scale Factor 1.
# These values are deterministic for a given scale factor and are derived
# from the TPC-H specification and confirmed against dbgen output.
EXPECTED_COUNTS: dict[str, int] = {
    # Nodes
    "Region": 5,
    "Nation": 25,
    "Supplier": 10_000,
    "Customer": 150_000,
    "Part": 200_000,
    "Order": 1_500_000,
    "LineItem": 6_001_215,
    # Relationships
    "BELONGS_TO": 25,
    "LOCATED_IN": 160_000,
    "SUPPLIES": 800_000,
    "PLACED": 1_500_000,
    "CONTAINS": 6_001_215,
    "OF_PART": 6_001_215,
    "SUPPLIED_BY": 6_001_215,
}


@dataclass
class VerifyQuery:
    """A count query for verifying that a specific entity type was loaded correctly.

    Each database module defines a list of ``VerifyQuery`` instances, one per
    node label and relationship type. The ``entity`` field serves as the lookup
    key into ``EXPECTED_COUNTS``, while the ``query`` field contains the
    database-specific count statement.

    Attributes:
        entity: The name of the node label or relationship type being verified,
            corresponding to a key in the ``EXPECTED_COUNTS`` dictionary.
        query: A database-specific query string that returns a single integer
            count (e.g., ``MATCH (n:Region) RETURN count(n)`` for Neo4j).
    """

    entity: str
    query: str
