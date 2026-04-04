"""ArangoDB verification queries for checking TPC-H data integrity after loading.

Each query counts documents in a specific collection (document or edge) and the
result is compared against the expected TPC-H Scale Factor 1 values.

AQL ``COLLECT WITH COUNT`` is used for efficient counting. Results are returned
as ``{count: N}`` to match the format expected by the verification command.
"""

from graphonauts.base.verify import VerifyQuery

VERIFY_QUERIES: list[VerifyQuery] = [
    # Nodes (document collections)
    VerifyQuery("Region", "FOR doc IN Region COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("Nation", "FOR doc IN Nation COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("Supplier", "FOR doc IN Supplier COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("Customer", "FOR doc IN Customer COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("Part", "FOR doc IN Part COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("Order", "FOR doc IN `Order` COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("LineItem", "FOR doc IN LineItem COLLECT WITH COUNT INTO c RETURN {count: c}"),
    # Relationships (edge collections)
    VerifyQuery("BELONGS_TO", "FOR doc IN belongs_to COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("LOCATED_IN", "FOR doc IN located_in COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("SUPPLIES", "FOR doc IN supplies COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("PLACED", "FOR doc IN placed COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("CONTAINS", "FOR doc IN `contains` COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("OF_PART", "FOR doc IN of_part COLLECT WITH COUNT INTO c RETURN {count: c}"),
    VerifyQuery("SUPPLIED_BY", "FOR doc IN supplied_by COLLECT WITH COUNT INTO c RETURN {count: c}"),
]
