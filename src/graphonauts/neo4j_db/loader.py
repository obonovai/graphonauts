"""Neo4j data loader for TPC-H dataset.

Loads 8 TPC-H entity types into Neo4j as a property graph with nodes and relationships.
Uses CALL { ... } IN TRANSACTIONS for batched writes and CONCURRENT TRANSACTIONS
for parallel ingestion of large tables.

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
from graphonauts.neo4j_db.client import Neo4jClient
from utils import printer


class Neo4jLoader:
    """Loads TPC-H data into Neo4j with batch progress printing.

    Usage as async context manager (handles connect/close):
        loader = Neo4jLoader(Neo4jClient())
        await loader.aload()

    After loading, entity_times contains per-entity durations in seconds.
    """

    def __init__(self, client: Neo4jClient) -> None:
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

            printer.task_start("Dropping indexes")
            await self._adrop_indices()
            printer.task_done("Dropping indexes")

    async def aload_region(self) -> None:
        rows = read_entity(REGION)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                CREATE (:Region {regionkey: row.regionkey, name: row.name, comment: row.comment})
            } IN TRANSACTIONS OF 5 ROWS
        """
        printer.task_progress("Loading regions", 1, 1)
        async with self.client.asession() as session:
            await session.run(query, rows=rows)

    async def aload_nation(self) -> None:
        rows = read_entity(NATION)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                MATCH (region:Region {regionkey: row.regionkey})
                CREATE (nation:Nation {nationkey: row.nationkey, name: row.name, comment: row.comment})-[:BELONGS_TO]->(region)
            } IN TRANSACTIONS OF 25 ROWS
        """
        printer.task_progress("Loading nations", 1, 1)
        async with self.client.asession() as session:
            await session.run(query, rows=rows)

    async def aload_supplier(self) -> None:
        rows = read_entity(SUPPLIER)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                MATCH (nation:Nation {nationkey: row.nationkey})
                CREATE (:Supplier {suppkey: row.suppkey, name: row.name, address: row.address, phone: row.phone, acctbal: row.acctbal, comment: row.comment})-[:LOCATED_IN]->(nation)
            } IN CONCURRENT TRANSACTIONS OF 1000 ROWS
        """
        printer.task_progress("Loading suppliers", 1, 1)
        async with self.client.asession() as session:
            await session.run(query, rows=rows)

    async def aload_customer(self) -> None:
        num_batches = total_batches("customers", CHUNK_SIZE)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                MATCH (nation:Nation {nationkey: row.nationkey})
                CREATE (:Customer {custkey: row.custkey, name: row.name, address: row.address, phone: row.phone, acctbal: row.acctbal, mktsegment: row.mktsegment, comment: row.comment})-[:LOCATED_IN]->(nation)
            } IN CONCURRENT TRANSACTIONS OF 1000 ROWS
        """
        for i, rows in enumerate(read_entity_chunks(CUSTOMER, CHUNK_SIZE), 1):
            printer.task_progress("Loading customers", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_part(self) -> None:
        num_batches = total_batches("parts", CHUNK_SIZE)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                CREATE (:Part {partkey: row.partkey, name: row.name, mfgr: row.mfgr, brand: row.brand, type: row.type, size: row.size, container: row.container, retailprice: row.retailprice, comment: row.comment})
            } IN CONCURRENT TRANSACTIONS OF 1000 ROWS
        """
        for i, rows in enumerate(read_entity_chunks(PART, CHUNK_SIZE), 1):
            printer.task_progress("Loading parts", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_partsupp(self) -> None:
        num_batches = total_batches("part_suppliers", CHUNK_SIZE_PARTSUPP)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                MATCH (supplier:Supplier {suppkey: row.suppkey})
                MATCH (part:Part {partkey: row.partkey})
                CREATE (supplier)-[:SUPPLIES {availqty: row.availqty, supplycost: row.supplycost, comment: row.comment}]->(part)
            } IN CONCURRENT TRANSACTIONS OF 1000 ROWS
        """
        for i, rows in enumerate(read_entity_chunks(PARTSUPP, CHUNK_SIZE_PARTSUPP), 1):
            printer.task_progress("Loading part_suppliers", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_orders(self) -> None:
        num_batches = total_batches("orders", CHUNK_SIZE_ORDERS)
        query = """
            UNWIND $rows AS row
            CALL (row) {
                MATCH (customer:Customer {custkey: row.custkey})
                CREATE (customer)-[:PLACED]->(order:Order {orderkey: row.orderkey, orderstatus: row.orderstatus, totalprice: row.totalprice, orderdate: row.orderdate, orderpriority: row.orderpriority, clerk: row.clerk, shippriority: row.shippriority, comment: row.comment})
            } IN CONCURRENT TRANSACTIONS OF 1000 ROWS
        """
        for i, rows in enumerate(read_entity_chunks(ORDER, CHUNK_SIZE_ORDERS), 1):
            printer.task_progress("Loading orders", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aload_lineitem(self) -> None:
        num_batches = total_batches("line_items", CHUNK_SIZE_LINEITEM)
        query = """
            UNWIND $rows AS row
            CALL (row) {
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
            } IN CONCURRENT TRANSACTIONS OF 1000 ROWS
        """
        for i, rows in enumerate(read_entity_chunks(LINEITEM, CHUNK_SIZE_LINEITEM), 1):
            printer.task_progress("Loading line_items", i, num_batches)
            async with self.client.asession() as session:
                await session.run(query, rows=rows)

    async def aclear(self) -> None:
        async with self.client.asession() as session:
            await session.run(
                """
                    MATCH (n)
                    CALL (n) {
                        DETACH DELETE n
                    } IN TRANSACTIONS OF 1000 ROWS;
                """
            )

    async def __aenter__(self) -> "Neo4jLoader":
        self.client.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[no-untyped-def]
        await self.client.aclose()

    async def _acreate_indices(self) -> None:
        async with self.client.asession() as session:
            for query in [
                "CREATE INDEX region_key IF NOT EXISTS FOR (region:Region) ON (region.regionkey)",
                "CREATE INDEX nation_key IF NOT EXISTS FOR (nation:Nation) ON (nation.nationkey)",
                "CREATE INDEX supplier_key IF NOT EXISTS FOR (supplier:Supplier) ON (supplier.suppkey)",
                "CREATE INDEX customer_key IF NOT EXISTS FOR (customer:Customer) ON (customer.custkey)",
                "CREATE INDEX part_key IF NOT EXISTS FOR (part:Part) ON (part.partkey)",
                "CREATE INDEX order_key IF NOT EXISTS FOR (order:Order) ON (order.orderkey)",
            ]:
                await session.run(query)

    async def _adrop_indices(self) -> None:
        async with self.client.asession() as session:
            for query in [
                "DROP INDEX region_key IF EXISTS",
                "DROP INDEX nation_key IF EXISTS",
                "DROP INDEX supplier_key IF EXISTS",
                "DROP INDEX customer_key IF EXISTS",
                "DROP INDEX part_key IF EXISTS",
                "DROP INDEX order_key IF EXISTS",
            ]:
                await session.run(query)
