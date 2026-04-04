"""ArangoDB data loader for TPC-H dataset.

Loads 8 TPC-H entity types into ArangoDB as a property graph using document
collections for nodes and edge collections for relationships. Uses the
``python-arango`` batch insert API (``insert_many``) for efficient bulk loading.

Unlike Neo4j and Memgraph loaders which use MATCH queries to resolve foreign keys
during relationship creation, the ArangoDB loader constructs edge ``_from``/``_to``
references directly from primary key values. This is possible because ArangoDB's
``_key`` field is deterministically set from TPC-H primary keys, allowing edge
endpoints to be computed without database lookups and eliminating the need for
temporary loading indexes.

Loading order follows entity dependencies:
    Region -> Nation -> Supplier/Customer -> Part -> PartSupp -> Orders -> LineItems

Document collections: Region, Nation, Supplier, Customer, Part, Order, LineItem
Edge collections: belongs_to, located_in, supplies, placed, contains, of_part, supplied_by
"""

import datetime
from typing import Any

from graphonauts.arangodb_db.client import ArangoDBClient
from graphonauts.base.tpch import (
    CHUNK_SIZE,
    CHUNK_SIZE_LINEITEM,
    CHUNK_SIZE_ORDERS,
    CHUNK_SIZE_PARTSUPP,
    CUSTOMER,
    LINEITEM,
    NATION,
    ORDER,
    PART,
    PARTSUPP,
    REGION,
    SUPPLIER,
    read_entity,
    read_entity_chunks,
    total_batches,
)
from utils import printer

# Document collection names (nodes)
_NODE_COLLECTIONS = ["Region", "Nation", "Supplier", "Customer", "Part", "Order", "LineItem"]

# Edge collection names (relationships)
_EDGE_COLLECTIONS = ["belongs_to", "located_in", "supplies", "placed", "contains", "of_part", "supplied_by"]

# Named graph edge definitions linking vertex and edge collections
_GRAPH_EDGE_DEFINITIONS = [
    {"edge_collection": "belongs_to", "from_vertex_collections": ["Nation"], "to_vertex_collections": ["Region"]},
    {
        "edge_collection": "located_in",
        "from_vertex_collections": ["Supplier", "Customer"],
        "to_vertex_collections": ["Nation"],
    },
    {"edge_collection": "supplies", "from_vertex_collections": ["Supplier"], "to_vertex_collections": ["Part"]},
    {"edge_collection": "placed", "from_vertex_collections": ["Customer"], "to_vertex_collections": ["Order"]},
    {"edge_collection": "contains", "from_vertex_collections": ["Order"], "to_vertex_collections": ["LineItem"]},
    {"edge_collection": "of_part", "from_vertex_collections": ["LineItem"], "to_vertex_collections": ["Part"]},
    {"edge_collection": "supplied_by", "from_vertex_collections": ["LineItem"], "to_vertex_collections": ["Supplier"]},
]


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert datetime.date values to ISO format strings for JSON serialisation.

    The ``python-arango`` driver serialises documents via ``json.dumps()``, which
    cannot handle ``datetime.date`` objects. TPC-H date columns (orderdate, shipdate,
    commitdate, receiptdate) are returned as ``datetime.date`` by pandas and must be
    converted before insertion.

    Args:
        row: A dictionary representing a single TPC-H row.

    Returns:
        A new dictionary with date values converted to ISO format strings.
    """
    return {k: v.isoformat() if isinstance(v, datetime.date) else v for k, v in row.items()}


class ArangoDBLoader:
    """Loads TPC-H data into ArangoDB with batch progress printing.

    Usage as async context manager (handles connect/close):
        loader = ArangoDBLoader(ArangoDBClient())
        await loader.aload()

    The loader creates document collections for each TPC-H entity type and edge
    collections for each relationship type. Edges are constructed directly from
    foreign keys without database lookups, leveraging ArangoDB's deterministic
    ``_key`` field.
    """

    def __init__(self, client: ArangoDBClient) -> None:
        self.client = client

    async def aload(self) -> None:
        """Load all TPC-H entities into ArangoDB in dependency order."""
        async with self:
            printer.task_start("Clearing database")
            await self.aclear()
            printer.task_done("Clearing database")

            printer.task_start("Creating collections")
            await self._acreate_collections()
            printer.task_done("Creating collections")

            steps: list[tuple[str, str]] = [
                ("regions", "aload_region"),
                ("nations", "aload_nation"),
                ("suppliers", "aload_supplier"),
                ("customers", "aload_customer"),
                ("parts", "aload_part"),
                ("part_suppliers", "aload_partsupp"),
                ("orders", "aload_orders"),
                ("line_items", "aload_lineitem"),
            ]

            for label, method_name in steps:
                printer.task_start(f"Loading {label}")
                await getattr(self, method_name)()
                printer.task_done(f"Loading {label}")

            printer.task_start("Creating named graph")
            await self._acreate_graph()
            printer.task_done("Creating named graph")

    async def aload_region(self) -> None:
        """Load Region nodes (5 rows)."""
        rows = read_entity(REGION)
        docs = [
            {
                "_key": str(row["regionkey"]),
                "regionkey": row["regionkey"],
                "name": row["name"],
                "comment": row["comment"],
            }
            for row in rows
        ]
        printer.task_progress("Loading regions", 1, 1)
        await self.client.ainsert_many("Region", docs)

    async def aload_nation(self) -> None:
        """Load Nation nodes (25 rows) and belongs_to edges to Region."""
        rows = read_entity(NATION)
        nodes = [
            {
                "_key": str(row["nationkey"]),
                "nationkey": row["nationkey"],
                "name": row["name"],
                "comment": row["comment"],
            }
            for row in rows
        ]
        edges = [{"_from": f"Nation/{row['nationkey']}", "_to": f"Region/{row['regionkey']}"} for row in rows]
        printer.task_progress("Loading nations", 1, 1)
        await self.client.ainsert_many("Nation", nodes)
        await self.client.ainsert_many("belongs_to", edges)

    async def aload_supplier(self) -> None:
        """Load Supplier nodes (10K rows) and located_in edges to Nation."""
        rows = read_entity(SUPPLIER)
        nodes = [
            {
                "_key": str(row["suppkey"]),
                "suppkey": row["suppkey"],
                "name": row["name"],
                "address": row["address"],
                "phone": row["phone"],
                "acctbal": row["acctbal"],
                "comment": row["comment"],
            }
            for row in rows
        ]
        edges = [{"_from": f"Supplier/{row['suppkey']}", "_to": f"Nation/{row['nationkey']}"} for row in rows]
        printer.task_progress("Loading suppliers", 1, 1)
        await self.client.ainsert_many("Supplier", nodes)
        await self.client.ainsert_many("located_in", edges)

    async def aload_customer(self) -> None:
        """Load Customer nodes (150K rows) and located_in edges to Nation."""
        num_batches = total_batches("customers", CHUNK_SIZE)
        for i, rows in enumerate(read_entity_chunks(CUSTOMER, CHUNK_SIZE), 1):
            nodes = [
                {
                    "_key": str(row["custkey"]),
                    "custkey": row["custkey"],
                    "name": row["name"],
                    "address": row["address"],
                    "phone": row["phone"],
                    "acctbal": row["acctbal"],
                    "mktsegment": row["mktsegment"],
                    "comment": row["comment"],
                }
                for row in rows
            ]
            edges = [{"_from": f"Customer/{row['custkey']}", "_to": f"Nation/{row['nationkey']}"} for row in rows]
            printer.task_progress("Loading customers", i, num_batches)
            await self.client.ainsert_many("Customer", nodes)
            await self.client.ainsert_many("located_in", edges)

    async def aload_part(self) -> None:
        """Load Part nodes (200K rows), no edges."""
        num_batches = total_batches("parts", CHUNK_SIZE)
        for i, rows in enumerate(read_entity_chunks(PART, CHUNK_SIZE), 1):
            docs = [
                {
                    "_key": str(row["partkey"]),
                    "partkey": row["partkey"],
                    "name": row["name"],
                    "mfgr": row["mfgr"],
                    "brand": row["brand"],
                    "type": row["type"],
                    "size": row["size"],
                    "container": row["container"],
                    "retailprice": row["retailprice"],
                    "comment": row["comment"],
                }
                for row in rows
            ]
            printer.task_progress("Loading parts", i, num_batches)
            await self.client.ainsert_many("Part", docs)

    async def aload_partsupp(self) -> None:
        """Load supplies edges with properties (800K rows). No node collection."""
        num_batches = total_batches("part_suppliers", CHUNK_SIZE_PARTSUPP)
        for i, rows in enumerate(read_entity_chunks(PARTSUPP, CHUNK_SIZE_PARTSUPP), 1):
            edges = [
                {
                    "_from": f"Supplier/{row['suppkey']}",
                    "_to": f"Part/{row['partkey']}",
                    "availqty": row["availqty"],
                    "supplycost": row["supplycost"],
                    "comment": row["comment"],
                }
                for row in rows
            ]
            printer.task_progress("Loading part_suppliers", i, num_batches)
            await self.client.ainsert_many("supplies", edges)

    async def aload_orders(self) -> None:
        """Load Order nodes (1.5M rows) and placed edges from Customer."""
        num_batches = total_batches("orders", CHUNK_SIZE_ORDERS)
        for i, rows in enumerate(read_entity_chunks(ORDER, CHUNK_SIZE_ORDERS), 1):
            nodes = [
                {
                    "_key": str(row["orderkey"]),
                    "orderkey": row["orderkey"],
                    "orderstatus": row["orderstatus"],
                    "totalprice": row["totalprice"],
                    "orderdate": _serialize_row(row)["orderdate"],
                    "orderpriority": row["orderpriority"],
                    "clerk": row["clerk"],
                    "shippriority": row["shippriority"],
                    "comment": row["comment"],
                }
                for row in rows
            ]
            edges = [{"_from": f"Customer/{row['custkey']}", "_to": f"Order/{row['orderkey']}"} for row in rows]
            printer.task_progress("Loading orders", i, num_batches)
            await self.client.ainsert_many("Order", nodes)
            await self.client.ainsert_many("placed", edges)

    async def aload_lineitem(self) -> None:
        """Load LineItem nodes (6M rows) and three edge types per row."""
        num_batches = total_batches("line_items", CHUNK_SIZE_LINEITEM)
        for i, rows in enumerate(read_entity_chunks(LINEITEM, CHUNK_SIZE_LINEITEM), 1):
            nodes: list[dict[str, Any]] = []
            contains_edges: list[dict[str, Any]] = []
            of_part_edges: list[dict[str, Any]] = []
            supplied_by_edges: list[dict[str, Any]] = []

            for row in rows:
                li_key = f"{row['orderkey']}_{row['linenumber']}"
                sr = _serialize_row(row)
                nodes.append(
                    {
                        "_key": li_key,
                        "linenumber": row["linenumber"],
                        "quantity": row["quantity"],
                        "extendedprice": row["extendedprice"],
                        "discount": row["discount"],
                        "tax": row["tax"],
                        "returnflag": row["returnflag"],
                        "linestatus": row["linestatus"],
                        "shipdate": sr["shipdate"],
                        "commitdate": sr["commitdate"],
                        "receiptdate": sr["receiptdate"],
                        "shipinstruct": row["shipinstruct"],
                        "shipmode": row["shipmode"],
                        "comment": row["comment"],
                    }
                )
                contains_edges.append(
                    {
                        "_from": f"Order/{row['orderkey']}",
                        "_to": f"LineItem/{li_key}",
                    }
                )
                of_part_edges.append(
                    {
                        "_from": f"LineItem/{li_key}",
                        "_to": f"Part/{row['partkey']}",
                    }
                )
                supplied_by_edges.append(
                    {
                        "_from": f"LineItem/{li_key}",
                        "_to": f"Supplier/{row['suppkey']}",
                    }
                )

            printer.task_progress("Loading line_items", i, num_batches)
            await self.client.ainsert_many("LineItem", nodes)
            await self.client.ainsert_many("contains", contains_edges)
            await self.client.ainsert_many("of_part", of_part_edges)
            await self.client.ainsert_many("supplied_by", supplied_by_edges)

    async def aclear(self) -> None:
        """Remove all data by dropping the graph and all collections."""
        await self.client.adrop_graph("tpch")
        for name in _EDGE_COLLECTIONS:
            await self.client.adrop_collection(name)
        for name in _NODE_COLLECTIONS:
            await self.client.adrop_collection(name)

    async def __aenter__(self) -> "ArangoDBLoader":
        self.client.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[no-untyped-def]
        await self.client.aclose()

    async def _acreate_collections(self) -> None:
        """Create all document and edge collections for the TPC-H schema."""
        for name in _NODE_COLLECTIONS:
            await self.client.acreate_collection(name, edge=False)
        for name in _EDGE_COLLECTIONS:
            await self.client.acreate_collection(name, edge=True)

    async def _acreate_graph(self) -> None:
        """Create the named graph ``tpch`` with all edge definitions.

        The named graph enables AQL graph traversal queries using the
        ``GRAPH "tpch"`` syntax. It is metadata only and does not duplicate data.
        """
        await self.client.acreate_graph("tpch", _GRAPH_EDGE_DEFINITIONS)
