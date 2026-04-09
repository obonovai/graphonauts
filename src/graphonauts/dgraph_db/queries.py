"""Dgraph DQL query definitions for TPC-H benchmarking.

19 queries across 5 categories:
    Selection (4 variants): point lookups and range queries
    Aggregation (2 variants): COUNT and MAX
    Join (7 variants): multi-hop traversals, shortest path, neighborhood search
    Set (3 variants): UNION, intersection, difference
    Modification (3 variants): sorting (indexed/non-indexed), DISTINCT

Key DQL vs Cypher differences:
    - Root function: func: type(X), eq(pred, val), has(pred) (not MATCH)
    - Filtering: @filter(eq(...) AND ge(...)) (not WHERE)
    - Edge traversal: nested blocks following predicates (not -[:REL]->)
    - Reverse edges: ~predicate (requires @reverse in schema)
    - Aggregation: @groupby(predicate) { count(uid) } (not GROUP BY)
    - Sorting: orderasc/orderdesc parameter (not ORDER BY)
    - Indexes required for ALL value-based filtering (eq, ge, le, allofterms)
    - Index management via schema alter, not DDL statements
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from graphonauts.base.client import BaseClient
from graphonauts.base.query import Query


def _make_index_setup(schema_with_index: str) -> Callable[[BaseClient], Coroutine[Any, Any, None]]:
    """Create an async index setup callable using Dgraph schema alter.

    Dgraph indexes are defined as part of the schema. Adding an index requires
    re-declaring the predicate with an ``@index`` directive via ``client.aalter()``.
    This bypasses ``make_index_setup`` from ``base/index_helpers.py`` which relies
    on ``client.aexecute(ddl)``.

    Args:
        schema_with_index: Schema line with ``@index`` directive,
            e.g. ``"name: string @index(hash) ."``.

    Returns:
        An async callable with signature ``async (BaseClient) -> None``.
    """

    async def setup(client: BaseClient) -> None:
        await client.aalter(schema_with_index)  # type: ignore[attr-defined]
        await asyncio.sleep(3)

    return setup


def _make_index_teardown(schema_without_index: str) -> Callable[[BaseClient], Coroutine[Any, Any, None]]:
    """Create an async index teardown callable using Dgraph schema alter.

    Removes an index by re-declaring the predicate without the ``@index`` directive.

    Args:
        schema_without_index: Original schema line without ``@index``,
            e.g. ``"name: string ."``.

    Returns:
        An async callable with signature ``async (BaseClient) -> None``.
    """

    async def teardown(client: BaseClient) -> None:
        await client.aalter(schema_without_index)  # type: ignore[attr-defined]
        await asyncio.sleep(1)

    return teardown


# All 19 benchmark queries keyed by (category, variant).
QUERIES: dict[tuple[str, int], Query] = {
    # --- Selection queries ---
    ("selection", 1): Query(
        category="selection",
        variant=1,
        description="Non-Indexed Columns: Select supplier named 'Supplier#000000666'",
        query="""
            query result($name: string) {
                result(func: eq(name, $name)) @filter(type(Supplier)) {
                    suppkey
                    name
                    address
                    phone
                }
            }
        """,
        params={"name": "Supplier#000000666"},
        setup=_make_index_setup("name: string @index(hash) ."),
        teardown=_make_index_teardown("name: string ."),
    ),
    ("selection", 2): Query(
        category="selection",
        variant=2,
        description="Non-Indexed Columns — Range Query: Select line items shipped between 1995-03-01 and 1995-03-31",
        query="""
            query result($from: string, $to: string) {
                result(func: type(LineItem)) @filter(ge(shipdate, $from) AND le(shipdate, $to)) {
                    linenumber
                    shipdate
                    extendedprice
                }
            }
        """,
        params={"from": "1995-03-01T00:00:00Z", "to": "1995-03-31T00:00:00Z"},
        setup=_make_index_setup("shipdate: dateTime @index(day) ."),
        teardown=_make_index_teardown("shipdate: dateTime ."),
    ),
    ("selection", 3): Query(
        category="selection",
        variant=3,
        description="Indexed Columns: Select supplier with the ID 1337",
        query="""
            query result($suppkey: int) {
                result(func: eq(suppkey, $suppkey)) @filter(type(Supplier)) {
                    suppkey
                    name
                    address
                    phone
                }
            }
        """,
        params={"suppkey": "1337"},
        setup=_make_index_setup("suppkey: int @index(int) ."),
        teardown=_make_index_teardown("suppkey: int ."),
    ),
    ("selection", 4): Query(
        category="selection",
        variant=4,
        description="Indexed Columns — Range Query: Select line items shipped between 1995-03-01 and 1995-03-31",
        query="""
            query result($from: string, $to: string) {
                result(func: type(LineItem)) @filter(ge(shipdate, $from) AND le(shipdate, $to)) {
                    linenumber
                    shipdate
                    extendedprice
                }
            }
        """,
        params={"from": "1995-03-01T00:00:00Z", "to": "1995-03-31T00:00:00Z"},
        setup=_make_index_setup("shipdate: dateTime @index(day) ."),
        teardown=_make_index_teardown("shipdate: dateTime ."),
    ),
    # --- Aggregation queries ---
    ("aggregation", 1): Query(
        category="aggregation",
        variant=1,
        description="COUNT: Count the number of products per brand",
        query="""
            {
                result(func: type(Part)) @groupby(brand) {
                    count(uid)
                }
            }
        """,
    ),
    ("aggregation", 2): Query(
        category="aggregation",
        variant=2,
        description="MAX: Find the most expensive product per brand",
        query="""
            {
                result(func: type(Part)) @groupby(brand) {
                    max(retailprice)
                }
            }
        """,
    ),
    # --- Join / traversal queries ---
    ("join", 1): Query(
        category="join",
        variant=1,
        description="Non-Indexed Columns: Join supplier and customer through nations on non-indexed comment keywords",
        query="""
            {
                result(func: type(Supplier)) @filter(allofterms(comment, "special")) {
                    supplier_name: name
                    located_in {
                        ~located_in @filter(type(Customer) AND allofterms(comment, "special")) {
                            customer_name: name
                            customer_comment: comment
                        }
                    }
                }
            }
        """,
        setup=_make_index_setup("comment: string @index(term) ."),
        teardown=_make_index_teardown("comment: string ."),
    ),
    ("join", 2): Query(
        category="join",
        variant=2,
        description="Indexed Columns: Join all products with their orders",
        query="""
            {
                result(func: type(Order)) {
                    order_date: orderdate
                    order_totalprice: totalprice
                    contains {
                        of_part {
                            part_name: name
                        }
                    }
                }
            }
        """,
    ),
    ("join", 3): Query(
        category="join",
        variant=3,
        description="Complex Join 1: Retrieve all order details (limited to 10000 customers — DQL nested response format OOMs on full dataset)",
        query="""
            {
                result(func: type(Customer), first: 10000) {
                    custkey
                    customer_name: name
                    located_in {
                        customer_nation: name
                    }
                    placed {
                        orderkey
                        orderdate
                        totalprice
                        contains {
                            linenumber
                            quantity
                            extendedprice
                            of_part {
                                partkey
                                part_name: name
                                brand
                            }
                            supplied_by {
                                suppkey
                                supplier_name: name
                            }
                        }
                    }
                }
            }
        """,
    ),
    ("join", 4): Query(
        category="join",
        variant=4,
        description="Complex Join 2: Retrieve all customers having more than 1 order",
        query="""
            {
                var(func: type(Customer)) {
                    oc as count(placed)
                }
                result(func: type(Customer)) @filter(gt(val(oc), 1)) {
                    custkey
                    customer_name: name
                    mktsegment
                    order_count: val(oc)
                    located_in {
                        nation_name: name
                    }
                }
            }
        """,
    ),
    ("join", 5): Query(
        category="join",
        variant=5,
        description="Neighborhood Search: Find all direct and indirect relationships between customers up to a depth of 3",
        query="""
            query result($custkey: int) {
                var(func: eq(custkey, $custkey)) @filter(type(Customer)) {
                    c1 as uid
                }
                result(func: uid(c1)) @recurse(depth: 4) {
                    uid
                    dgraph.type
                    placed
                    ~placed
                    contains
                    ~contains
                    located_in
                    ~located_in
                    of_part
                    ~of_part
                    supplied_by
                    ~supplied_by
                    belongs_to
                    ~belongs_to
                    supplies
                    ~supplies
                }
            }
        """,
        params={"custkey": "1"},
        setup=_make_index_setup("custkey: int @index(int) ."),
        teardown=_make_index_teardown("custkey: int ."),
    ),
    ("join", 6): Query(
        category="join",
        variant=6,
        description="Shortest Path: Find the shortest path between two customers",
        query="""
            query result($custkey1: int, $custkey2: int) {
                var(func: eq(custkey, $custkey1)) @filter(type(Customer)) {
                    c1 as uid
                }
                var(func: eq(custkey, $custkey2)) @filter(type(Customer)) {
                    c2 as uid
                }
                path as shortest(from: uid(c1), to: uid(c2)) {
                    placed
                    ~placed
                    contains
                    ~contains
                    located_in
                    ~located_in
                    of_part
                    ~of_part
                    supplied_by
                    ~supplied_by
                    belongs_to
                    ~belongs_to
                    supplies
                    ~supplies
                }
                result(func: uid(path)) {
                    uid
                    dgraph.type
                }
            }
        """,
        params={"custkey1": "1", "custkey2": "2"},
        setup=_make_index_setup("custkey: int @index(int) ."),
        teardown=_make_index_teardown("custkey: int ."),
    ),
    ("join", 7): Query(
        category="join",
        variant=7,
        description="Optional Traversal: Get a list of all suppliers and their part count (0 if they supply no parts)",
        query="""
            {
                result(func: type(Supplier)) {
                    suppkey
                    supplier_name: name
                    address
                    parts_supplied_count: count(supplies)
                }
            }
        """,
    ),
    # --- Set operation queries ---
    ("set", 1): Query(
        category="set",
        variant=1,
        description="Union: Get a list of contacts (phone) for both suppliers and customers",
        query="""
            {
                suppliers(func: type(Supplier)) {
                    contact_phone: phone
                    contact_name: name
                }
                customers(func: type(Customer)) {
                    contact_phone: phone
                    contact_name: name
                }
            }
        """,
    ),
    ("set", 2): Query(
        category="set",
        variant=2,
        description="Intersection: Find common nations between suppliers and customers",
        query="""
            {
                var(func: type(Supplier)) {
                    located_in {
                        sn as uid
                    }
                }
                var(func: type(Customer)) {
                    located_in {
                        cn as uid
                    }
                }
                result(func: uid(sn)) @filter(uid(cn)) {
                    common_nation: name
                }
            }
        """,
    ),
    ("set", 3): Query(
        category="set",
        variant=3,
        description="Difference: Find customers who have not made any orders",
        query="""
            {
                var(func: type(Customer)) {
                    oc as count(placed)
                }
                result(func: type(Customer), orderasc: custkey) @filter(eq(val(oc), 0)) {
                    custkey
                    customer_name: name
                    mktsegment
                    acctbal
                }
            }
        """,
    ),
    # --- Modification / ordering queries ---
    ("modification", 1): Query(
        category="modification",
        variant=1,
        description="Non-Indexed Columns Sorting: Sort products by brand",
        query="""
            {
                result(func: type(Part), orderasc: brand) {
                    partkey
                    part_name: name
                    brand
                    retailprice
                }
            }
        """,
    ),
    ("modification", 2): Query(
        category="modification",
        variant=2,
        description="Indexed Columns Sorting: Sort products by brand",
        query="""
            {
                result(func: type(Part), orderasc: brand) {
                    partkey
                    part_name: name
                    brand
                    retailprice
                }
            }
        """,
        setup=_make_index_setup("brand: string @index(exact) ."),
        teardown=_make_index_teardown("brand: string ."),
    ),
    ("modification", 3): Query(
        category="modification",
        variant=3,
        description="Distinct: Find unique combinations of product brands and the countries of the suppliers selling those products",
        query="""
            {
                result(func: type(Part)) {
                    product_brand: brand
                    ~supplies {
                        located_in {
                            supplier_nation: name
                        }
                    }
                }
            }
        """,
    ),
}
