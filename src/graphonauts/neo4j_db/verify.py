"""Neo4j verification queries for checking TPC-H data integrity after loading.

Each query counts a specific node label or relationship type and the result
is compared against the expected TPC-H Scale Factor 1 values.
"""

from graphonauts.base.verify import VerifyQuery

VERIFY_QUERIES: list[VerifyQuery] = [
    # Nodes
    VerifyQuery("Region", "MATCH (n:Region) RETURN count(n) AS count"),
    VerifyQuery("Nation", "MATCH (n:Nation) RETURN count(n) AS count"),
    VerifyQuery("Supplier", "MATCH (n:Supplier) RETURN count(n) AS count"),
    VerifyQuery("Customer", "MATCH (n:Customer) RETURN count(n) AS count"),
    VerifyQuery("Part", "MATCH (n:Part) RETURN count(n) AS count"),
    VerifyQuery("Order", "MATCH (n:Order) RETURN count(n) AS count"),
    VerifyQuery("LineItem", "MATCH (n:LineItem) RETURN count(n) AS count"),
    # Relationships
    VerifyQuery("BELONGS_TO", "MATCH ()-[r:BELONGS_TO]->() RETURN count(r) AS count"),
    VerifyQuery("LOCATED_IN", "MATCH ()-[r:LOCATED_IN]->() RETURN count(r) AS count"),
    VerifyQuery("SUPPLIES", "MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS count"),
    VerifyQuery("PLACED", "MATCH ()-[r:PLACED]->() RETURN count(r) AS count"),
    VerifyQuery("CONTAINS", "MATCH ()-[r:CONTAINS]->() RETURN count(r) AS count"),
    VerifyQuery("OF_PART", "MATCH ()-[r:OF_PART]->() RETURN count(r) AS count"),
    VerifyQuery("SUPPLIED_BY", "MATCH ()-[r:SUPPLIED_BY]->() RETURN count(r) AS count"),
]
