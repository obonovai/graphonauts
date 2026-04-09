"""Dgraph data loader for TPC-H dataset.

Loads 8 TPC-H entity types into Dgraph as a property graph using JSON mutations
via the ``pydgraph`` gRPC client. Nodes are assigned UIDs by Dgraph during mutation,
and a UID cache maps primary keys to UIDs for constructing edges to previously
loaded entities.

Unlike Neo4j and Memgraph loaders which use MATCH queries to resolve foreign keys
during relationship creation, the Dgraph loader constructs edge references directly
from cached UIDs. This eliminates the need for temporary loading indexes.

Edge properties on the ``supplies`` relationship (availqty, supplycost, comment) are
modeled as Dgraph facets --- metadata attached directly to the edge predicate.

Loading order follows entity dependencies:
    Region -> Nation -> Supplier/Customer -> Part -> PartSupp -> Orders -> LineItems
"""

import datetime
from typing import Any

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
from graphonauts.dgraph_db.client import DgraphClient
from utils import printer

# Dgraph schema: predicate declarations and type definitions.
# Predicates are global (shared across types). ``ptype`` replaces ``type``
# because ``type`` is a reserved keyword in DQL.
# No indexes are defined here; benchmark queries add them via setup/teardown.
_SCHEMA = """
regionkey: int .
nationkey: int .
suppkey: int .
custkey: int .
partkey: int .
orderkey: int .
linenumber: int .
name: string .
comment: string .
address: string .
phone: string .
acctbal: float .
mktsegment: string .
mfgr: string .
brand: string .
ptype: string .
size: int .
container: string .
retailprice: float .
orderstatus: string .
totalprice: float .
orderdate: dateTime .
orderpriority: string .
clerk: string .
shippriority: int .
quantity: float .
extendedprice: float .
discount: float .
tax: float .
returnflag: string .
linestatus: string .
shipdate: dateTime .
commitdate: dateTime .
receiptdate: dateTime .
shipinstruct: string .
shipmode: string .

belongs_to: uid @reverse .
located_in: uid @reverse .
supplies: [uid] @reverse .
placed: [uid] @reverse .
contains: [uid] @reverse .
of_part: uid @reverse .
supplied_by: uid @reverse .

type Region {
    regionkey
    name
    comment
}

type Nation {
    nationkey
    name
    comment
    belongs_to
}

type Supplier {
    suppkey
    name
    address
    phone
    acctbal
    comment
    located_in
    supplies
}

type Customer {
    custkey
    name
    address
    phone
    acctbal
    mktsegment
    comment
    located_in
    placed
}

type Part {
    partkey
    name
    mfgr
    brand
    ptype
    size
    container
    retailprice
    comment
}

type Order {
    orderkey
    orderstatus
    totalprice
    orderdate
    orderpriority
    clerk
    shippriority
    comment
}

type LineItem {
    linenumber
    quantity
    extendedprice
    discount
    tax
    returnflag
    linestatus
    shipdate
    commitdate
    receiptdate
    shipinstruct
    shipmode
    comment
    of_part
    supplied_by
}
"""


def _serialize_date(value: Any) -> str:
    """Convert a ``datetime.date`` to RFC 3339 format for Dgraph's ``dateTime`` type.

    Dgraph requires dates in RFC 3339 format (``YYYY-MM-DDT00:00:00Z``). TPC-H date
    columns are returned as ``datetime.date`` objects by pandas and must be converted
    before inclusion in JSON mutations.

    Args:
        value: A ``datetime.date`` instance.

    Returns:
        An RFC 3339 formatted date string at midnight UTC.
    """
    if isinstance(value, datetime.date):
        return f"{value.isoformat()}T00:00:00Z"
    return str(value)


class DgraphLoader:
    """Loads TPC-H data into Dgraph with batch progress printing.

    Usage as async context manager (handles connect/close)::

        loader = DgraphLoader(DgraphClient())
        await loader.aload()

    The loader maintains a UID cache that maps entity primary keys to Dgraph UIDs.
    This cache is populated during loading and used to construct edges between
    entities without database lookups.
    """

    def __init__(self, client: DgraphClient) -> None:
        self.client = client
        self._uid_cache: dict[str, dict[int, str]] = {}

    def _cache_uids(self, entity: str, key_prefix: str, uids: dict[str, str]) -> None:
        """Store blank-node UID mappings in the cache.

        Args:
            entity: Entity type name (e.g., ``"Region"``).
            key_prefix: The prefix used in blank node labels (e.g., ``"region_"``).
                Blank nodes are named ``_:{prefix}{key}``, so the mapping key is
                extracted by stripping the prefix.
            uids: The blank-node-to-UID mapping returned by ``amutate()``.
        """
        if entity not in self._uid_cache:
            self._uid_cache[entity] = {}
        for label, uid in uids.items():
            if label.startswith(key_prefix):
                key = int(label[len(key_prefix) :])
                self._uid_cache[entity][key] = uid

    async def aload(self) -> None:
        """Load all TPC-H entities into Dgraph in dependency order.

        Sequence: clear database, set schema, load 8 entity types. Each entity
        type populates the UID cache for use by subsequent entity loads.
        """
        async with self:
            printer.task_start("Clearing database")
            await self.aclear()
            printer.task_done("Clearing database")

            printer.task_start("Setting schema")
            await self.client.aalter(_SCHEMA)
            printer.task_done("Setting schema")

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

    async def aload_region(self) -> None:
        """Load Region nodes (5 rows)."""
        rows = read_entity(REGION)
        data = [
            {
                "uid": f"_:region_{row['regionkey']}",
                "dgraph.type": "Region",
                "regionkey": row["regionkey"],
                "name": row["name"],
                "comment": row["comment"],
            }
            for row in rows
        ]
        printer.task_progress("Loading regions", 1, 1)
        uids = await self.client.amutate(data)
        self._cache_uids("Region", "region_", uids)

    async def aload_nation(self) -> None:
        """Load Nation nodes (25 rows) and belongs_to edges to Region."""
        rows = read_entity(NATION)
        data = [
            {
                "uid": f"_:nation_{row['nationkey']}",
                "dgraph.type": "Nation",
                "nationkey": row["nationkey"],
                "name": row["name"],
                "comment": row["comment"],
                "belongs_to": {"uid": self._uid_cache["Region"][row["regionkey"]]},
            }
            for row in rows
        ]
        printer.task_progress("Loading nations", 1, 1)
        uids = await self.client.amutate(data)
        self._cache_uids("Nation", "nation_", uids)

    async def aload_supplier(self) -> None:
        """Load Supplier nodes (10K rows) and located_in edges to Nation."""
        rows = read_entity(SUPPLIER)
        data = [
            {
                "uid": f"_:supplier_{row['suppkey']}",
                "dgraph.type": "Supplier",
                "suppkey": row["suppkey"],
                "name": row["name"],
                "address": row["address"],
                "phone": row["phone"],
                "acctbal": row["acctbal"],
                "comment": row["comment"],
                "located_in": {"uid": self._uid_cache["Nation"][row["nationkey"]]},
            }
            for row in rows
        ]
        printer.task_progress("Loading suppliers", 1, 1)
        uids = await self.client.amutate(data)
        self._cache_uids("Supplier", "supplier_", uids)

    async def aload_customer(self) -> None:
        """Load Customer nodes (150K rows) and located_in edges to Nation."""
        num_batches = total_batches("customers", CHUNK_SIZE)
        for i, rows in enumerate(read_entity_chunks(CUSTOMER, CHUNK_SIZE), 1):
            data = [
                {
                    "uid": f"_:customer_{row['custkey']}",
                    "dgraph.type": "Customer",
                    "custkey": row["custkey"],
                    "name": row["name"],
                    "address": row["address"],
                    "phone": row["phone"],
                    "acctbal": row["acctbal"],
                    "mktsegment": row["mktsegment"],
                    "comment": row["comment"],
                    "located_in": {"uid": self._uid_cache["Nation"][row["nationkey"]]},
                }
                for row in rows
            ]
            printer.task_progress("Loading customers", i, num_batches)
            uids = await self.client.amutate(data)
            self._cache_uids("Customer", "customer_", uids)

    async def aload_part(self) -> None:
        """Load Part nodes (200K rows), no edges."""
        num_batches = total_batches("parts", CHUNK_SIZE)
        for i, rows in enumerate(read_entity_chunks(PART, CHUNK_SIZE), 1):
            data = [
                {
                    "uid": f"_:part_{row['partkey']}",
                    "dgraph.type": "Part",
                    "partkey": row["partkey"],
                    "name": row["name"],
                    "mfgr": row["mfgr"],
                    "brand": row["brand"],
                    "ptype": row["type"],
                    "size": row["size"],
                    "container": row["container"],
                    "retailprice": row["retailprice"],
                    "comment": row["comment"],
                }
                for row in rows
            ]
            printer.task_progress("Loading parts", i, num_batches)
            uids = await self.client.amutate(data)
            self._cache_uids("Part", "part_", uids)

    async def aload_partsupp(self) -> None:
        """Load supplies edges with facets (800K rows).

        Each PartSupp row creates a ``supplies`` edge from Supplier to Part with
        facets for availqty, supplycost, and comment. The mutation updates existing
        Supplier nodes by appending to their ``supplies`` edge list.
        """
        num_batches = total_batches("part_suppliers", CHUNK_SIZE_PARTSUPP)
        for i, rows in enumerate(read_entity_chunks(PARTSUPP, CHUNK_SIZE_PARTSUPP), 1):
            data = [
                {
                    "uid": self._uid_cache["Supplier"][row["suppkey"]],
                    "supplies": {
                        "uid": self._uid_cache["Part"][row["partkey"]],
                        "supplies|availqty": row["availqty"],
                        "supplies|supplycost": row["supplycost"],
                        "supplies|comment": row["comment"],
                    },
                }
                for row in rows
            ]
            printer.task_progress("Loading part_suppliers", i, num_batches)
            await self.client.amutate(data)

    async def aload_orders(self) -> None:
        """Load Order nodes (1.5M rows) and placed edges from Customer.

        Each mutation batch creates Order nodes and simultaneously updates Customer
        nodes to add ``placed`` edges pointing to the new Orders.
        """
        num_batches = total_batches("orders", CHUNK_SIZE_ORDERS)
        for i, rows in enumerate(read_entity_chunks(ORDER, CHUNK_SIZE_ORDERS), 1):
            data: list[dict[str, Any]] = []
            for row in rows:
                order_blank = f"_:order_{row['orderkey']}"
                data.append(
                    {
                        "uid": order_blank,
                        "dgraph.type": "Order",
                        "orderkey": row["orderkey"],
                        "orderstatus": row["orderstatus"],
                        "totalprice": row["totalprice"],
                        "orderdate": _serialize_date(row["orderdate"]),
                        "orderpriority": row["orderpriority"],
                        "clerk": row["clerk"],
                        "shippriority": row["shippriority"],
                        "comment": row["comment"],
                    }
                )
                data.append(
                    {
                        "uid": self._uid_cache["Customer"][row["custkey"]],
                        "placed": [{"uid": order_blank}],
                    }
                )
            printer.task_progress("Loading orders", i, num_batches)
            uids = await self.client.amutate(data)
            self._cache_uids("Order", "order_", uids)

    async def aload_lineitem(self) -> None:
        """Load LineItem nodes (6M rows) and three edge types per row.

        Each mutation batch creates LineItem nodes with ``of_part`` and ``supplied_by``
        edges, and simultaneously updates Order nodes to add ``contains`` edges.
        LineItem UIDs are not cached as no subsequent entity references them.
        """
        num_batches = total_batches("line_items", CHUNK_SIZE_LINEITEM)
        for i, rows in enumerate(read_entity_chunks(LINEITEM, CHUNK_SIZE_LINEITEM), 1):
            data: list[dict[str, Any]] = []
            for row in rows:
                li_blank = f"_:li_{row['orderkey']}_{row['linenumber']}"
                data.append(
                    {
                        "uid": li_blank,
                        "dgraph.type": "LineItem",
                        "linenumber": row["linenumber"],
                        "quantity": row["quantity"],
                        "extendedprice": row["extendedprice"],
                        "discount": row["discount"],
                        "tax": row["tax"],
                        "returnflag": row["returnflag"],
                        "linestatus": row["linestatus"],
                        "shipdate": _serialize_date(row["shipdate"]),
                        "commitdate": _serialize_date(row["commitdate"]),
                        "receiptdate": _serialize_date(row["receiptdate"]),
                        "shipinstruct": row["shipinstruct"],
                        "shipmode": row["shipmode"],
                        "comment": row["comment"],
                        "of_part": {"uid": self._uid_cache["Part"][row["partkey"]]},
                        "supplied_by": {"uid": self._uid_cache["Supplier"][row["suppkey"]]},
                    }
                )
                data.append(
                    {
                        "uid": self._uid_cache["Order"][row["orderkey"]],
                        "contains": [{"uid": li_blank}],
                    }
                )
            printer.task_progress("Loading line_items", i, num_batches)
            await self.client.amutate(data)

    async def aclear(self) -> None:
        """Remove all data, schema, and types from Dgraph."""
        await self.client.adrop_all()
        self._uid_cache.clear()

    async def __aenter__(self) -> "DgraphLoader":
        self.client.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[no-untyped-def]
        await self.client.aclose()
