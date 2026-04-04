"""Memgraph Cypher query definitions for TPC-H benchmarking.

19 queries across 5 categories:
    Selection (4 variants): point lookups and range queries
    Aggregation (2 variants): COUNT and MAX
    Join (7 variants): multi-hop traversals, shortest path, neighborhood search
    Set (3 variants): UNION, intersection, difference
    Modification (3 variants): sorting (indexed/non-indexed), DISTINCT

Key Memgraph vs Neo4j Cypher differences:
    - Index DDL: CREATE INDEX ON :Label(prop) / DROP INDEX ON :Label(prop)
    - No APOC: intersection uses graph-pattern approach instead of apoc.coll.intersection()
    - Shortest path: *BFS instead of shortestPath()
    - Path length: size(path) instead of length(path)
    - List extraction: extract(n IN ...) instead of [n IN ... | ...]
"""

from datetime import date

from graphonauts.base.index_helpers import make_index_setup, make_index_teardown
from graphonauts.base.query import Query

QUERIES: dict[tuple[str, int], Query] = {
    # --- Selection queries ---
    ("selection", 1): Query(
        category="selection",
        variant=1,
        description="Non-Indexed Columns: Select supplier named 'Supplier#000000666'",
        query="""
            MATCH (s:Supplier {name: $name})
            RETURN s.suppkey, s.name, s.address, s.phone
        """,
        params={"name": "Supplier#000000666"},
    ),
    ("selection", 2): Query(
        category="selection",
        variant=2,
        description="Non-Indexed Columns — Range Query: Select line items shipped between 1995-03-01 and 1995-03-31",
        query="""
            MATCH (li:LineItem)
            WHERE li.shipdate >= $from AND li.shipdate <= $to
            RETURN li.orderkey, li.linenumber, li.shipdate, li.extendedprice
        """,
        params={"from": date(1995, 3, 1), "to": date(1995, 3, 31)},
    ),
    ("selection", 3): Query(
        category="selection",
        variant=3,
        description="Indexed Columns: Select supplier with the ID 1337",
        query="""
            MATCH (s:Supplier {suppkey: $suppkey})
            RETURN s.suppkey, s.name, s.address, s.phone
        """,
        params={"suppkey": 1337},
        setup=make_index_setup("CREATE INDEX ON :Supplier(suppkey)"),
        teardown=make_index_teardown("DROP INDEX ON :Supplier(suppkey)"),
    ),
    ("selection", 4): Query(
        category="selection",
        variant=4,
        description="Indexed Columns — Range Query: Select line items shipped between 1995-03-01 and 1995-03-31",
        query="""
            MATCH (li:LineItem)
            WHERE li.shipdate >= $from AND li.shipdate <= $to
            RETURN li.orderkey, li.linenumber, li.shipdate, li.extendedprice
        """,
        params={"from": date(1995, 3, 1), "to": date(1995, 3, 31)},
        setup=make_index_setup("CREATE INDEX ON :LineItem(shipdate)"),
        teardown=make_index_teardown("DROP INDEX ON :LineItem(shipdate)"),
    ),
    # --- Aggregation queries ---
    ("aggregation", 1): Query(
        category="aggregation",
        variant=1,
        description="COUNT: Count the number of products per brand",
        query="""
            MATCH (p:Part)
            RETURN p.brand AS brand, COUNT(p) AS product_count
            ORDER BY product_count DESC
        """,
    ),
    ("aggregation", 2): Query(
        category="aggregation",
        variant=2,
        description="MAX: Find the most expensive product per brand",
        query="""
            MATCH (p:Part)
            RETURN p.brand AS brand, MAX(p.retailprice) AS max_price
            ORDER BY max_price DESC
        """,
    ),
    # --- Join / traversal queries ---
    ("join", 1): Query(
        category="join",
        variant=1,
        description="Non-Indexed Columns: Join supplier and customer through nations on non-indexed comment keywords",
        query="""
            MATCH (s:Supplier)-[:LOCATED_IN]->(:Nation)<-[:LOCATED_IN]-(c:Customer)
            WHERE s.comment CONTAINS 'special' AND c.comment CONTAINS 'special'
            RETURN s.name AS supplier_name, c.name AS customer_name, c.comment AS customer_comment
        """,
    ),
    ("join", 2): Query(
        category="join",
        variant=2,
        description="Indexed Columns: Join all products with their orders",
        query="""
            MATCH (p:Part)<-[:OF_PART]-(:LineItem)<-[:CONTAINS]-(o:Order)
            RETURN p.name AS part_name, o.orderdate AS order_date, o.totalprice AS order_totalprice
        """,
    ),
    ("join", 3): Query(
        category="join",
        variant=3,
        description="Complex Join 1: Retrieve all order details",
        query="""
            MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(li:LineItem)
            MATCH (li)-[:OF_PART]->(p:Part)
            MATCH (li)-[:SUPPLIED_BY]->(s:Supplier)
            MATCH (c)-[:LOCATED_IN]->(n:Nation)
            RETURN c.custkey, c.name AS customer_name, n.name AS customer_nation,
                o.orderkey, o.orderdate, o.totalprice,
                li.linenumber, li.quantity, li.extendedprice,
                p.partkey, p.name AS part_name, p.brand,
                s.suppkey, s.name AS supplier_name
        """,
    ),
    ("join", 4): Query(
        category="join",
        variant=4,
        description="Complex Join 2: Retrieve all customers having more than 1 order",
        query="""
            MATCH (c:Customer)-[:PLACED]->(o:Order)
            WITH c, COUNT(o) AS order_count
            WHERE order_count > 1
            MATCH (c)-[:LOCATED_IN]->(n:Nation)
            RETURN c.custkey, c.name AS customer_name, c.mktsegment,
                n.name AS nation_name, order_count
        """,
    ),
    ("join", 5): Query(
        category="join",
        variant=5,
        description="Neighborhood Search: Find all direct and indirect relationships between customers up to a depth of 3",
        query="""
            MATCH (c1:Customer {custkey: 1})
            MATCH path = (c1)-[*1..3]-(c2:Customer)
            WHERE c1 <> c2
            RETURN c1.custkey AS customer1_key, c1.name AS customer1_name,
                c2.custkey AS customer2_key, c2.name AS customer2_name,
                size(path) AS path_length,
                extract(n in nodes(path) | labels(n)[0]) AS node_types
        """,
        setup=make_index_setup("CREATE INDEX ON :Customer(custkey)"),
        teardown=make_index_teardown("DROP INDEX ON :Customer(custkey)"),
    ),
    ("join", 6): Query(
        category="join",
        variant=6,
        description="Shortest Path: Find the shortest path between two customers",
        query="""
            MATCH (c1:Customer {custkey: 1}), (c2:Customer {custkey: 2})
            MATCH path = (c1)-[*BFS]-(c2)
            RETURN c1.name AS customer1_name, c2.name AS customer2_name,
                size(path) AS path_length,
                extract(n in nodes(path) | labels(n)[0]) AS node_types,
                extract(r in relationships(path) | type(r)) AS relationship_types
        """,
        setup=make_index_setup("CREATE INDEX ON :Customer(custkey)"),
        teardown=make_index_teardown("DROP INDEX ON :Customer(custkey)"),
    ),
    ("join", 7): Query(
        category="join",
        variant=7,
        description="Optional Traversal: Get a list of all suppliers and their part count (0 if they supply no parts)",
        query="""
            MATCH (s:Supplier)
            OPTIONAL MATCH (s)-[:SUPPLIES]->(p:Part)
            RETURN s.suppkey, s.name AS supplier_name, s.address, COUNT(p) AS parts_supplied_count
        """,
    ),
    # --- Set operation queries ---
    ("set", 1): Query(
        category="set",
        variant=1,
        description="Union: Get a list of contacts (phone) for both suppliers and customers",
        query="""
            MATCH (s:Supplier)
            RETURN s.phone AS contact_phone, 'Supplier' AS contact_type, s.name AS contact_name
            UNION
            MATCH (c:Customer)
            RETURN c.phone AS contact_phone, 'Customer' AS contact_type, c.name AS contact_name
        """,
    ),
    ("set", 2): Query(
        category="set",
        variant=2,
        description="Intersection: Find common nations between suppliers and customers",
        query="""
            MATCH (:Supplier)-[:LOCATED_IN]->(n:Nation)<-[:LOCATED_IN]-(:Customer)
            RETURN COLLECT(DISTINCT n.name) AS common_nations
        """,
    ),
    ("set", 3): Query(
        category="set",
        variant=3,
        description="Difference: Find customers who have not made any orders",
        query="""
            MATCH (c:Customer)
            WHERE NOT EXISTS((c)-[:PLACED]->(:Order))
            RETURN c.custkey, c.name AS customer_name, c.mktsegment, c.acctbal
            ORDER BY c.custkey
        """,
    ),
    # --- Modification / ordering queries ---
    ("modification", 1): Query(
        category="modification",
        variant=1,
        description="Non-Indexed Columns Sorting: Sort products by brand",
        query="""
            MATCH (p:Part)
            RETURN p.partkey, p.name AS part_name, p.brand, p.retailprice
            ORDER BY p.brand, p.name
        """,
    ),
    ("modification", 2): Query(
        category="modification",
        variant=2,
        description="Indexed Columns Sorting: Sort products by brand",
        query="""
            MATCH (p:Part)
            RETURN p.partkey, p.name AS part_name, p.brand, p.retailprice
            ORDER BY p.brand, p.name
        """,
        setup=make_index_setup("CREATE INDEX ON :Part(brand)"),
        teardown=make_index_teardown("DROP INDEX ON :Part(brand)"),
    ),
    ("modification", 3): Query(
        category="modification",
        variant=3,
        description="Distinct: Find unique combinations of product brands and the countries of the suppliers selling those products",
        query="""
            MATCH (p:Part)<-[:SUPPLIES]-(s:Supplier)-[:LOCATED_IN]->(n:Nation)
            RETURN DISTINCT p.brand AS product_brand, n.name AS supplier_nation
            ORDER BY p.brand, n.name
        """,
    ),
}
