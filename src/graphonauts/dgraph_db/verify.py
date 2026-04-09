"""Dgraph verification queries for checking TPC-H data integrity after loading.

Each query counts a specific node type or edge predicate and the result
is compared against the expected TPC-H Scale Factor 1 values.

Node counts use the ``type()`` function in DQL. Single-valued edge counts use
``has()`` to count nodes possessing the predicate. Multi-valued edge counts
(``[uid]`` predicates) use variable blocks with ``sum(val(...))`` to aggregate
per-node edge counts into a total.
"""

from graphonauts.base.verify import VerifyQuery

VERIFY_QUERIES: list[VerifyQuery] = [
    # Nodes
    VerifyQuery("Region", "{ result(func: type(Region)) { count: count(uid) } }"),
    VerifyQuery("Nation", "{ result(func: type(Nation)) { count: count(uid) } }"),
    VerifyQuery("Supplier", "{ result(func: type(Supplier)) { count: count(uid) } }"),
    VerifyQuery("Customer", "{ result(func: type(Customer)) { count: count(uid) } }"),
    VerifyQuery("Part", "{ result(func: type(Part)) { count: count(uid) } }"),
    VerifyQuery("Order", "{ result(func: type(Order)) { count: count(uid) } }"),
    VerifyQuery("LineItem", "{ result(func: type(LineItem)) { count: count(uid) } }"),
    # Relationships (single-valued edges: count of nodes with the predicate = count of edges)
    VerifyQuery("BELONGS_TO", "{ result(func: has(belongs_to)) { count: count(uid) } }"),
    VerifyQuery("LOCATED_IN", "{ result(func: has(located_in)) { count: count(uid) } }"),
    # Relationships (multi-valued edges: aggregate per-node edge counts)
    VerifyQuery(
        "SUPPLIES",
        "{ var(func: type(Supplier)) { c as count(supplies) } result() { count: sum(val(c)) } }",
    ),
    VerifyQuery(
        "PLACED",
        "{ var(func: type(Customer)) { c as count(placed) } result() { count: sum(val(c)) } }",
    ),
    VerifyQuery(
        "CONTAINS",
        "{ var(func: type(Order)) { c as count(contains) } result() { count: sum(val(c)) } }",
    ),
    # Relationships (single-valued edges on LineItem)
    VerifyQuery("OF_PART", "{ result(func: has(of_part)) { count: count(uid) } }"),
    VerifyQuery("SUPPLIED_BY", "{ result(func: has(supplied_by)) { count: count(uid) } }"),
]
