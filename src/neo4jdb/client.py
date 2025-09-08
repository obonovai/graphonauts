import logging
import pandas as pd
import time
from neo4j import AsyncDriver, AsyncGraphDatabase
from pathlib import Path
from typing import Any

DATA_PATH = Path(__file__).parent.parent.parent / "tpch-osx" / "dbgen"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Neo4jTPCH:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """
        Initialize the Neo4jTPCH instance with configuration details.

        Args:
            config (dict): Configuration dictionary containing connection details.
        """

        self.config = config or {
            "uri": "bolt://localhost:7687",
            "user": "neo4j",
            "password": "password",
        }
        self.driver: AsyncDriver | None = None

    async def aconnect(self) -> None:
        """
        Establish a connection to the Neo4j database.
        """

        self.driver = AsyncGraphDatabase.driver(
            uri=self.config["uri"],
            auth=(self.config["user"], self.config["password"]),
            database=self.config.get("database"),
        )

    async def asetup(self) -> None:
        """
        Create indices on all key fields to optimize query performance.
        """

        async with self.driver.session() as session:
            indices = [
                "CREATE INDEX region_key IF NOT EXISTS FOR (r:Region) ON (r.regionkey)",
                "CREATE INDEX nation_key IF NOT EXISTS FOR (n:Nation) ON (n.nationkey)",
                "CREATE INDEX supplier_key IF NOT EXISTS FOR (s:Supplier) ON (s.suppkey)",
                "CREATE INDEX customer_key IF NOT EXISTS FOR (c:Customer) ON (c.custkey)",
                "CREATE INDEX part_key IF NOT EXISTS FOR (p:Part) ON (p.partkey)",
                "CREATE INDEX order_key IF NOT EXISTS FOR (o:Order) ON (o.orderkey)",
                "CREATE INDEX partsupp_composite IF NOT EXISTS FOR ()-[ps:SUPPLIES]-() ON (ps.partkey, ps.suppkey)",
                "CREATE INDEX customer_nation IF NOT EXISTS FOR (c:Customer) ON (c.nationkey)",
                "CREATE INDEX supplier_nation IF NOT EXISTS FOR (s:Supplier) ON (s.nationkey)",
                "CREATE INDEX nation_region IF NOT EXISTS FOR (n:Nation) ON (n.regionkey)",
                "CREATE INDEX order_customer IF NOT EXISTS FOR (o:Order) ON (o.custkey)",
                "CREATE INDEX lineitem_order IF NOT EXISTS FOR (li:LineItem) ON (li.orderkey)",
                "CREATE INDEX lineitem_part IF NOT EXISTS FOR (li:LineItem) ON (li.partkey)",
                "CREATE INDEX lineitem_supplier IF NOT EXISTS FOR (li:LineItem) ON (li.suppkey)",
            ]

            for index in indices:
                try:
                    await session.run(index)
                except Exception as e:
                    logger.error(f"Index creation error: {e}")

    async def adrop(self) -> None:
        """
        Drop all indices from the database.
        """

        async with self.driver.session() as session:
            indices = [
                "DROP INDEX region_key IF EXISTS",
                "DROP INDEX nation_key IF EXISTS",
                "DROP INDEX supplier_key IF EXISTS",
                "DROP INDEX customer_key IF EXISTS",
                "DROP INDEX part_key IF EXISTS",
                "DROP INDEX order_key IF EXISTS",
                "DROP INDEX partsupp_composite IF EXISTS",
                "DROP INDEX customer_nation IF EXISTS",
                "DROP INDEX supplier_nation IF EXISTS",
                "DROP INDEX nation_region IF EXISTS",
                "DROP INDEX order_customer IF EXISTS",
                "DROP INDEX lineitem_order IF EXISTS",
                "DROP INDEX lineitem_part IF EXISTS",
                "DROP INDEX lineitem_supplier IF EXISTS",
            ]

            for index in indices:
                try:
                    await session.run(index)
                except Exception as e:
                    logger.error(f"Index creation error: {e}")

    async def aclear(self) -> None:
        """
        Clear all nodes and relationships from the database.
        """

        async with self.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")

    async def aload(self):
        """
        Load all TPC-H tables into the database in the correct order.
        """

        logger.info("Starting TPC-H data load...")
        start_time = time.time()

        # Load in dependency order
        await self._aload_region()
        await self._aload_nation()
        await self._aload_supplier()
        await self._aload_customer()
        await self._aload_part()
        await self._aload_partsupp()
        await self._aload_orders()
        await self._aload_lineitem()

        logger.info(f"All tables loaded successfully in {time.time() - start_time:.2f} seconds!")

    async def arun(self, query: str, params: dict[str, Any] | None = None, log: bool = True) -> list[dict]:
        """
        Execute a Cypher query and return the results.

        Args:
            query (str): Cypher query to execute.
            params (dict): Parameters for the query.

        Returns:
            list: Query results.
        """

        start_time = time.time()
        async with self.driver.session() as session:
            result = await session.run(query, params)
            records = await result.data()
        if log:
            logger.info(f"Query executed in {time.time() - start_time:.2f} seconds.")
        return records

    async def aclose(self) -> None:
        """
        Close the connection to the Neo4j database.
        """

        if self.driver:
            await self.driver.close()

    async def _aload_region(self):
        """
        Load data from the region.tbl file into the database.
        """

        path = DATA_PATH / "region.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(3))
        df.columns = ["regionkey", "name", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (r:Region {regionkey: row.regionkey})
            SET r.name = row.name, r.comment = row.comment
        """
        logger.info("Loading Region data...")
        start_time = time.time()
        await self._aload_batch(query, data, "Region")
        logger.info(f"Region data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_nation(self):
        """
        Load data from the nation.tbl file into the database.
        """

        path = DATA_PATH / "nation.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(4))
        df.columns = ["nationkey", "name", "regionkey", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (n:Nation {nationkey: row.nationkey})
            SET n.name = row.name, n.comment = row.comment
            WITH n, row
            MATCH (r:Region {regionkey: row.regionkey})
            MERGE (n)-[:BELONGS_TO]->(r)
        """
        logger.info("Loading Nation data...")
        start_time = time.time()
        await self._aload_batch(query, data, "Nation")
        logger.info(f"Nation data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_supplier(self):
        """
        Load data from the supplier.tbl file into the database.
        """

        path = DATA_PATH / "supplier.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(7))
        df.columns = ["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (s:Supplier {suppkey: row.suppkey})
            SET s.name = row.name, s.address = row.address, s.phone = row.phone,
                s.acctbal = row.acctbal, s.comment = row.comment
            WITH s, row
            MATCH (n:Nation {nationkey: row.nationkey})
            MERGE (s)-[:LOCATED_IN]->(n)
        """
        logger.info("Loading Supplier data...")
        start_time = time.time()
        await self._aload_batch(query, data, "Supplier")
        logger.info(f"Supplier data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_customer(self):
        """
        Load data from the customer.tbl file into the database.
        """

        path = DATA_PATH / "customer.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(8))
        df.columns = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (c:Customer {custkey: row.custkey})
            SET c.name = row.name, c.address = row.address, c.phone = row.phone,
                c.acctbal = row.acctbal, c.mktsegment = row.mktsegment, c.comment = row.comment
            WITH c, row
            MATCH (n:Nation {nationkey: row.nationkey})
            MERGE (c)-[:LOCATED_IN]->(n)
        """
        logger.info("Loading Customer data...")
        start_time = time.time()
        await self._aload_batch(query, data, "Customer")
        logger.info(f"Customer data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_part(self):
        """
        Load data from the part.tbl file into the database.
        """

        path = DATA_PATH / "part.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (p:Part {partkey: row.partkey})
            SET p.name = row.name, p.mfgr = row.mfgr, p.brand = row.brand, p.type = row.type,
                p.size = row.size, p.container = row.container, p.retailprice = row.retailprice,
                p.comment = row.comment
        """
        logger.info("Loading Part data...")
        start_time = time.time()
        await self._aload_batch(query, data, "Part")
        logger.info(f"Part data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_partsupp(self):
        """
        Load data from the partsupp.tbl file into the database.
        """

        path = DATA_PATH / "partsupp.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(5))
        df.columns = ["partkey", "suppkey", "availqty", "supplycost", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MATCH (p:Part {partkey: row.partkey})
            MATCH (s:Supplier {suppkey: row.suppkey})
            MERGE (s)-[ps:SUPPLIES]->(p)
            SET ps.availqty = row.availqty, ps.supplycost = row.supplycost, ps.comment = row.comment
        """
        logger.info("Loading PartSupp data...")
        start_time = time.time()
        await self._aload_batch(query, data, "PartSupp")
        logger.info(f"PartSupp data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_orders(self):
        """
        Load data from the orders.tbl file into the database.
        """

        path = DATA_PATH / "orders.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MERGE (o:Order {orderkey: row.orderkey})
            SET o.orderstatus = row.orderstatus, o.totalprice = row.totalprice,
                o.orderdate = row.orderdate, o.orderpriority = row.orderpriority,
                o.clerk = row.clerk, o.shippriority = row.shippriority, o.comment = row.comment
            WITH o, row
            MATCH (c:Customer {custkey: row.custkey})
            MERGE (c)-[:PLACED]->(o)
        """
        logger.info("Loading Orders data...")
        start_time = time.time()
        await self._aload_batch(query, data, "Orders")
        logger.info(f"Orders data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_lineitem(self):
        """
        Load data from the lineitem.tbl file into the database.
        """

        path = DATA_PATH / "lineitem.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(16))
        df.columns = ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice",
                      "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate",
                      "receiptdate", "shipinstruct", "shipmode", "comment"]
        data = df.to_dict(orient="records")

        query = """
            UNWIND $rows AS row
            MATCH (o:Order {orderkey: row.orderkey})
            MATCH (p:Part {partkey: row.partkey})
            MATCH (s:Supplier {suppkey: row.suppkey})
            CREATE (li:LineItem {
                orderkey: row.orderkey, partkey: row.partkey, suppkey: row.suppkey,
                linenumber: row.linenumber, quantity: row.quantity, extendedprice: row.extendedprice,
                discount: row.discount, tax: row.tax, returnflag: row.returnflag,
                linestatus: row.linestatus, shipdate: row.shipdate, commitdate: row.commitdate,
                receiptdate: row.receiptdate, shipinstruct: row.shipinstruct,
                shipmode: row.shipmode, comment: row.comment
            })
            CREATE (o)-[:CONTAINS]->(li)
            CREATE (li)-[:OF_PART]->(p)
            CREATE (li)-[:SUPPLIED_BY]->(s)
        """
        logger.info("Loading LineItem data...")
        start_time = time.time()
        await self._aload_batch(query, data, "LineItem")
        logger.info(f"LineItem data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_batch(self, query: str, data: list[dict], table_name: str, batch_size: int = 1000) -> None:
        """
        Load data into the database in batches using the provided Cypher query.

        Args:
            query (str): Cypher query for loading data.
            data (list[dict]): Data to be loaded.
            table_name (str): Name of the table being loaded.
            batch_size (int): Number of records per batch.
        """

        async with self.driver.session() as session:
            total_batches = (len(data) + batch_size - 1) // batch_size
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                await session.run(query, rows=batch)

            logger.info(f"{table_name}: Loaded {len(data)} records in {total_batches} batches.")
