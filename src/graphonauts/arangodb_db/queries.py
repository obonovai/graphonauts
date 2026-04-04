"""ArangoDB AQL query definitions for TPC-H benchmarking.

19 queries across 5 categories:
    Selection (4 variants): point lookups and range queries
    Aggregation (2 variants): COUNT and MAX
    Join (7 variants): multi-hop graph traversals, shortest path, neighborhood search
    Set (3 variants): UNION, INTERSECTION, difference
    Modification (3 variants): sorting (indexed/non-indexed), DISTINCT

Key AQL vs Cypher differences:
    - Pattern matching: FOR doc IN Collection (not MATCH (n:Label))
    - Graph traversal: FOR v, e, p IN depth..depth OUTBOUND/INBOUND/ANY start edge_collection
    - Named graph: GRAPH "tpch" for open-ended traversals
    - Bind variables: @param (not $param)
    - Dates stored as ISO strings: string comparison works for date ranges
    - Index management: via python-arango HTTP API (not DDL statements)
    - COLLECT instead of WITH/GROUP BY, native UNION/INTERSECTION functions
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from graphonauts.base.client import BaseClient
from graphonauts.base.query import Query


def _make_index_setup(collection: str, fields: list[str]) -> Callable[[BaseClient], Coroutine[Any, Any, None]]:
    """Create an async index setup callable using the python-arango HTTP API.

    ArangoDB indexes cannot be created via AQL, so this function uses the
    ``python-arango`` driver's ``add_persistent_index`` method directly,
    bypassing the ``make_index_setup`` helper from ``base/index_helpers.py``
    which relies on ``client.aexecute(ddl)``.

    Args:
        collection: Name of the collection to index.
        fields: List of field names to include in the persistent index.

    Returns:
        An async callable with signature ``async (BaseClient) -> None``.
    """

    async def setup(client: BaseClient) -> None:
        db = client.db  # type: ignore[attr-defined]
        await asyncio.to_thread(db.collection(collection).add_persistent_index, fields=fields)
        await asyncio.sleep(1)

    return setup


def _make_index_teardown(collection: str, fields: list[str]) -> Callable[[BaseClient], Coroutine[Any, Any, None]]:
    """Create an async index teardown callable using the python-arango HTTP API.

    Locates the persistent index by matching its field list, then deletes it
    by ID. Sleeps 1 second after deletion for consistency with the framework's
    teardown pattern.

    Args:
        collection: Name of the collection containing the index.
        fields: Field list that uniquely identifies the target index.

    Returns:
        An async callable with signature ``async (BaseClient) -> None``.
    """

    async def teardown(client: BaseClient) -> None:
        db = client.db  # type: ignore[attr-defined]

        def _drop() -> None:
            col = db.collection(collection)
            for idx in col.indexes():
                if idx["type"] == "persistent" and sorted(idx["fields"]) == sorted(fields):
                    col.delete_index(idx["id"])
                    break

        await asyncio.to_thread(_drop)
        await asyncio.sleep(1)

    return teardown


QUERIES: dict[tuple[str, int], Query] = {
    # --- Selection queries ---
    ("selection", 1): Query(
        category="selection",
        variant=1,
        description="Non-Indexed Columns: Select supplier named 'Supplier#000000666'",
        query="""
            FOR s IN Supplier
                FILTER s.name == @name
                RETURN {suppkey: s.suppkey, name: s.name, address: s.address, phone: s.phone}
        """,
        params={"name": "Supplier#000000666"},
    ),
    ("selection", 2): Query(
        category="selection",
        variant=2,
        description="Non-Indexed Columns — Range Query: Select line items shipped between 1995-03-01 and 1995-03-31",
        query="""
            FOR li IN LineItem
                FILTER li.shipdate >= @from AND li.shipdate <= @to
                RETURN {linenumber: li.linenumber, shipdate: li.shipdate, extendedprice: li.extendedprice}
        """,
        params={"from": "1995-03-01", "to": "1995-03-31"},
    ),
    ("selection", 3): Query(
        category="selection",
        variant=3,
        description="Indexed Columns: Select supplier with the ID 1337",
        query="""
            FOR s IN Supplier
                FILTER s.suppkey == @suppkey
                RETURN {suppkey: s.suppkey, name: s.name, address: s.address, phone: s.phone}
        """,
        params={"suppkey": 1337},
        setup=_make_index_setup("Supplier", ["suppkey"]),
        teardown=_make_index_teardown("Supplier", ["suppkey"]),
    ),
    ("selection", 4): Query(
        category="selection",
        variant=4,
        description="Indexed Columns — Range Query: Select line items shipped between 1995-03-01 and 1995-03-31",
        query="""
            FOR li IN LineItem
                FILTER li.shipdate >= @from AND li.shipdate <= @to
                RETURN {linenumber: li.linenumber, shipdate: li.shipdate, extendedprice: li.extendedprice}
        """,
        params={"from": "1995-03-01", "to": "1995-03-31"},
        setup=_make_index_setup("LineItem", ["shipdate"]),
        teardown=_make_index_teardown("LineItem", ["shipdate"]),
    ),
    # --- Aggregation queries ---
    ("aggregation", 1): Query(
        category="aggregation",
        variant=1,
        description="COUNT: Count the number of products per brand",
        query="""
            FOR p IN Part
                COLLECT brand = p.brand WITH COUNT INTO product_count
                SORT product_count DESC
                RETURN {brand, product_count}
        """,
    ),
    ("aggregation", 2): Query(
        category="aggregation",
        variant=2,
        description="MAX: Find the most expensive product per brand",
        query="""
            FOR p IN Part
                COLLECT brand = p.brand AGGREGATE max_price = MAX(p.retailprice)
                SORT max_price DESC
                RETURN {brand, max_price}
        """,
    ),
    # --- Join / traversal queries ---
    ("join", 1): Query(
        category="join",
        variant=1,
        description="Non-Indexed Columns: Join supplier and customer through nations on non-indexed comment keywords",
        query="""
            FOR s IN Supplier
                FILTER CONTAINS(s.comment, "special")
                FOR nation IN 1..1 OUTBOUND s located_in
                    FOR c IN 1..1 INBOUND nation located_in
                        FILTER IS_SAME_COLLECTION("Customer", c)
                        FILTER CONTAINS(c.comment, "special")
                        RETURN {supplier_name: s.name, customer_name: c.name, customer_comment: c.comment}
        """,
    ),
    ("join", 2): Query(
        category="join",
        variant=2,
        description="Indexed Columns: Join all products with their orders",
        query="""
            FOR p IN Part
                FOR li IN 1..1 INBOUND p of_part
                    FOR o IN 1..1 INBOUND li `contains`
                        RETURN {part_name: p.name, order_date: o.orderdate, order_totalprice: o.totalprice}
        """,
    ),
    ("join", 3): Query(
        category="join",
        variant=3,
        description="Complex Join 1: Retrieve all order details",
        query="""
            FOR c IN Customer
                FOR n IN 1..1 OUTBOUND c located_in
                    FOR o IN 1..1 OUTBOUND c placed
                        FOR li IN 1..1 OUTBOUND o `contains`
                            FOR p IN 1..1 OUTBOUND li of_part
                                FOR s IN 1..1 OUTBOUND li supplied_by
                                    RETURN {
                                        custkey: c.custkey, customer_name: c.name, customer_nation: n.name,
                                        orderkey: o.orderkey, orderdate: o.orderdate, totalprice: o.totalprice,
                                        linenumber: li.linenumber, quantity: li.quantity, extendedprice: li.extendedprice,
                                        partkey: p.partkey, part_name: p.name, brand: p.brand,
                                        suppkey: s.suppkey, supplier_name: s.name
                                    }
        """,
    ),
    ("join", 4): Query(
        category="join",
        variant=4,
        description="Complex Join 2: Retrieve all customers having more than 1 order",
        query="""
            FOR c IN Customer
                LET order_count = LENGTH(FOR o IN 1..1 OUTBOUND c placed RETURN 1)
                FILTER order_count > 1
                FOR n IN 1..1 OUTBOUND c located_in
                    RETURN {
                        custkey: c.custkey, customer_name: c.name, mktsegment: c.mktsegment,
                        nation_name: n.name, order_count
                    }
        """,
    ),
    ("join", 5): Query(
        category="join",
        variant=5,
        description="Neighborhood Search: Find all direct and indirect relationships between customers up to a depth of 3",
        query="""
            LET c1 = DOCUMENT("Customer/1")
            FOR c2, e, p IN 1..3 ANY c1 GRAPH "tpch"
                FILTER IS_SAME_COLLECTION("Customer", c2)
                FILTER c2._id != c1._id
                RETURN {
                    customer1_key: c1.custkey, customer1_name: c1.name,
                    customer2_key: c2.custkey, customer2_name: c2.name,
                    path_length: LENGTH(p.edges),
                    node_types: (FOR v IN p.vertices RETURN PARSE_IDENTIFIER(v._id).collection)
                }
        """,
        setup=_make_index_setup("Customer", ["custkey"]),
        teardown=_make_index_teardown("Customer", ["custkey"]),
    ),
    ("join", 6): Query(
        category="join",
        variant=6,
        description="Shortest Path: Find the shortest path between two customers",
        query="""
            LET c1 = DOCUMENT("Customer/1")
            LET c2 = DOCUMENT("Customer/2")
            LET path = (
                FOR v, e IN ANY SHORTEST_PATH c1 TO c2 GRAPH "tpch"
                    RETURN {vertex: v, edge: e}
            )
            RETURN {
                customer1_name: c1.name,
                customer2_name: c2.name,
                path_length: LENGTH(path) - 1,
                node_types: (FOR item IN path RETURN PARSE_IDENTIFIER(item.vertex._id).collection),
                relationship_types: (
                    FOR item IN path
                        FILTER item.edge != null
                        RETURN PARSE_IDENTIFIER(item.edge._id).collection
                )
            }
        """,
        setup=_make_index_setup("Customer", ["custkey"]),
        teardown=_make_index_teardown("Customer", ["custkey"]),
    ),
    ("join", 7): Query(
        category="join",
        variant=7,
        description="Optional Traversal: Get a list of all suppliers and their part count (0 if they supply no parts)",
        query="""
            FOR s IN Supplier
                LET parts_supplied_count = LENGTH(FOR p IN 1..1 OUTBOUND s supplies RETURN 1)
                RETURN {suppkey: s.suppkey, supplier_name: s.name, address: s.address, parts_supplied_count}
        """,
    ),
    # --- Set operation queries ---
    ("set", 1): Query(
        category="set",
        variant=1,
        description="Union: Get a list of contacts (phone) for both suppliers and customers",
        query="""
            LET suppliers = (
                FOR s IN Supplier
                    RETURN {contact_phone: s.phone, contact_type: "Supplier", contact_name: s.name}
            )
            LET customers = (
                FOR c IN Customer
                    RETURN {contact_phone: c.phone, contact_type: "Customer", contact_name: c.name}
            )
            FOR contact IN UNION(suppliers, customers)
                RETURN contact
        """,
    ),
    ("set", 2): Query(
        category="set",
        variant=2,
        description="Intersection: Find common nations between suppliers and customers",
        query="""
            LET supplier_nations = (
                FOR s IN Supplier
                    FOR n IN 1..1 OUTBOUND s located_in
                        RETURN DISTINCT n.name
            )
            LET customer_nations = (
                FOR c IN Customer
                    FOR n IN 1..1 OUTBOUND c located_in
                        RETURN DISTINCT n.name
            )
            RETURN {common_nations: INTERSECTION(supplier_nations, customer_nations)}
        """,
    ),
    ("set", 3): Query(
        category="set",
        variant=3,
        description="Difference: Find customers who have not made any orders",
        query="""
            FOR c IN Customer
                LET has_orders = LENGTH(FOR o IN 1..1 OUTBOUND c placed LIMIT 1 RETURN 1)
                FILTER has_orders == 0
                SORT c.custkey
                RETURN {custkey: c.custkey, customer_name: c.name, mktsegment: c.mktsegment, acctbal: c.acctbal}
        """,
    ),
    # --- Modification / ordering queries ---
    ("modification", 1): Query(
        category="modification",
        variant=1,
        description="Non-Indexed Columns Sorting: Sort products by brand",
        query="""
            FOR p IN Part
                SORT p.brand, p.name
                RETURN {partkey: p.partkey, part_name: p.name, brand: p.brand, retailprice: p.retailprice}
        """,
    ),
    ("modification", 2): Query(
        category="modification",
        variant=2,
        description="Indexed Columns Sorting: Sort products by brand",
        query="""
            FOR p IN Part
                SORT p.brand, p.name
                RETURN {partkey: p.partkey, part_name: p.name, brand: p.brand, retailprice: p.retailprice}
        """,
        setup=_make_index_setup("Part", ["brand"]),
        teardown=_make_index_teardown("Part", ["brand"]),
    ),
    ("modification", 3): Query(
        category="modification",
        variant=3,
        description="Distinct: Find unique combinations of product brands and the countries of the suppliers selling those products",
        query="""
            FOR p IN Part
                FOR s IN 1..1 INBOUND p supplies
                    FOR n IN 1..1 OUTBOUND s located_in
                        COLLECT product_brand = p.brand, supplier_nation = n.name
                        SORT product_brand, supplier_nation
                        RETURN {product_brand, supplier_nation}
        """,
    ),
}
