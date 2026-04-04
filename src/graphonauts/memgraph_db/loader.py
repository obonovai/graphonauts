"""Memgraph data loader for TPC-H dataset.

Loads 8 TPC-H entity types into Memgraph as a property graph with nodes and relationships.
Uses plain UNWIND + CREATE patterns (Memgraph does not support CALL { ... } IN TRANSACTIONS
or CONCURRENT TRANSACTIONS). Calls FREE MEMORY between entity loads to reclaim memory
in this in-memory database.

Loading order follows entity dependencies:
    Region -> Nation -> Supplier/Customer -> Part -> PartSupp -> Orders -> LineItems

Temporary indexes are created on key columns before loading (for MATCH lookups during
relationship creation) and dropped after loading (for fair query benchmarking).
"""

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
from graphonauts.memgraph_db.client import MemgraphClient
from utils import printer


class MemgraphLoader:
    """Loads TPC-H data into Memgraph with batch progress printing.

    Usage as async context manager (handles connect/close):
        loader = MemgraphLoader(MemgraphClient())
        await loader.aload()

    After loading, FREE MEMORY is called between entity loads to manage
    Memgraph's in-memory footprint.
    """

    def __init__(self, client: MemgraphClient) -> None:
        self.client = client

    async def aload(self) -> None:
        async with self:
            printer.task_start("Clearing database")
            await self.aclear()
            printer.task_done("Clearing database")

            printer.task_start("Setting up indexes")
            await self._acreate_indices()
            printer.task_done("Setting up indexes")

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
                await self._afree_memory()

            printer.task_start("Dropping indexes")
            await self._adrop_indices()
            printer.task_done("Dropping indexes")

            printer.task_start("Creating snapshot")
            await self._acreate_snapshot()
            printer.task_done("Creating snapshot")

    async def aload_region(self) -> None:
        rows = read_entity(REGION)
        query = """
            UNWIND $rows AS row
            CREATE (:Region {regionkey: row.regionkey, name: row.name, comment: row.comment})
        """
        printer.task_progress("Loading regions", 1, 1)
        async with self.client.asession() as session:
            await session.run(query, rows=rows)

    async def aload_nation(self) -> None:
        rows = read_entity(NATION)
        query = """
            UNWIND $rows AS row
            MATCH (region:Region {regionkey: row.regionkey})
            CREATE (nation:Nation {nationkey: row.nationkey, name: row.name, comment: row.comment})-[:BELONGS_TO]->(region)
        """
        printer.task_progress("Loading nations", 1, 1)
        async with self.client.asession() as session:
            await session.run(query, rows=rows)

    async def aload_supplier(self) -> None:
        rows = read_entity(SUPPLIER)
        query = """
            UNWIND $rows AS row
            MATCH (nation:Nation {nationkey: row.nationkey})
            CREATE (:Supplier {suppkey: row.suppkey, name: row.name, address: row.address, phone: row.phone, acctbal: row.acctbal, comment: row.comment})-[:LOCATED_IN]->(nation)
        """
        printer.task_progress("Loading suppliers", 1, 1)
        async with self.client.asession() as session:
            await session.run(query, rows=rows)

    async def aload_customer(self) -> None:
        num_batches = total_batches("customers", CHUNK_SIZE)
        query = """
            UNWIND $rows AS row
            MATCH (nation:Nation {nationkey: row.nationkey})
            CREATE (:Customer {custkey: row.custkey, name: row.name, address: row.address, phone: row.phone, acctbal: row.acctbal, mktsegment: row.mktsegment, comment: row.comment})-[:LOCATED_IN]->(nation)
        """
        for i, rows in enumerate(read_entity_chunks(CUSTOMER, CHUNK_SIZE), 1):
            printer.task_progress("Loading customers", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_part(self) -> None:
        num_batches = total_batches("parts", CHUNK_SIZE)
        query = """
            UNWIND $rows AS row
            CREATE (:Part {partkey: row.partkey, name: row.name, mfgr: row.mfgr, brand: row.brand, type: row.type, size: row.size, container: row.container, retailprice: row.retailprice, comment: row.comment})
        """
        for i, rows in enumerate(read_entity_chunks(PART, CHUNK_SIZE), 1):
            printer.task_progress("Loading parts", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_partsupp(self) -> None:
        num_batches = total_batches("part_suppliers", CHUNK_SIZE_PARTSUPP)
        query = """
            UNWIND $rows AS row
            MATCH (supplier:Supplier {suppkey: row.suppkey})
            MATCH (part:Part {partkey: row.partkey})
            CREATE (supplier)-[:SUPPLIES {availqty: row.availqty, supplycost: row.supplycost, comment: row.comment}]->(part)
        """
        for i, rows in enumerate(read_entity_chunks(PARTSUPP, CHUNK_SIZE_PARTSUPP), 1):
            printer.task_progress("Loading part_suppliers", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_orders(self) -> None:
        num_batches = total_batches("orders", CHUNK_SIZE_ORDERS)
        query = """
            UNWIND $rows AS row
            MATCH (customer:Customer {custkey: row.custkey})
            CREATE (customer)-[:PLACED]->(order:Order {orderkey: row.orderkey, orderstatus: row.orderstatus, totalprice: row.totalprice, orderdate: row.orderdate, orderpriority: row.orderpriority, clerk: row.clerk, shippriority: row.shippriority, comment: row.comment})
        """
        for i, rows in enumerate(read_entity_chunks(ORDER, CHUNK_SIZE_ORDERS), 1):
            printer.task_progress("Loading orders", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_lineitem(self) -> None:
        num_batches = total_batches("line_items", CHUNK_SIZE_LINEITEM)
        query = """
            UNWIND $rows AS row
            MATCH (order:Order {orderkey: row.orderkey})
            MATCH (part:Part {partkey: row.partkey})
            MATCH (supplier:Supplier {suppkey: row.suppkey})
            CREATE (lineitem:LineItem {
                linenumber: row.linenumber,
                quantity: row.quantity, extendedprice: row.extendedprice, discount: row.discount, tax: row.tax,
                returnflag: row.returnflag, linestatus: row.linestatus, shipdate: row.shipdate, commitdate: row.commitdate,
                receiptdate: row.receiptdate, shipinstruct: row.shipinstruct, shipmode: row.shipmode, comment: row.comment
            })
            CREATE (order)-[:CONTAINS]->(lineitem)
            CREATE (lineitem)-[:OF_PART]->(part)
            CREATE (lineitem)-[:SUPPLIED_BY]->(supplier)
        """
        for i, rows in enumerate(read_entity_chunks(LINEITEM, CHUNK_SIZE_LINEITEM), 1):
            printer.task_progress("Loading line_items", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aclear(self) -> None:
        async with self.client.asession() as session:
            await session.run("MATCH (n) DETACH DELETE n")

    async def __aenter__(self) -> "MemgraphLoader":
        self.client.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[no-untyped-def]
        await self.client.aclose()

    async def _acreate_indices(self) -> None:
        async with self.client.asession() as session:
            for query in [
                "CREATE INDEX ON :Region(regionkey)",
                "CREATE INDEX ON :Nation(nationkey)",
                "CREATE INDEX ON :Supplier(suppkey)",
                "CREATE INDEX ON :Customer(custkey)",
                "CREATE INDEX ON :Part(partkey)",
                "CREATE INDEX ON :Order(orderkey)",
            ]:
                await session.run(query)

    async def _afree_memory(self) -> None:
        """Reclaim memory in Memgraph's in-memory storage engine.

        Memgraph keeps all data in RAM. Without explicit reclamation between
        large entity loads, the cumulative memory pressure can cause OOM failures.
        """
        async with self.client.asession() as session:
            await session.run("FREE MEMORY")

    async def _acreate_snapshot(self) -> None:
        """Create a persistent snapshot for recovery across container restarts.

        Since snapshot-on-exit and periodic snapshots are disabled in the Docker
        Compose configuration to prevent disk exhaustion during repeated benchmark
        restarts, a single manual snapshot after loading ensures that the loaded
        TPC-H data persists through container restarts without accumulating
        snapshot files on every shutdown.
        """
        async with self.client.asession() as session:
            await session.run("CREATE SNAPSHOT")

    async def _adrop_indices(self) -> None:
        async with self.client.asession() as session:
            for query in [
                "DROP INDEX ON :Region(regionkey)",
                "DROP INDEX ON :Nation(nationkey)",
                "DROP INDEX ON :Supplier(suppkey)",
                "DROP INDEX ON :Customer(custkey)",
                "DROP INDEX ON :Part(partkey)",
                "DROP INDEX ON :Order(orderkey)",
            ]:
                await session.run(query)
