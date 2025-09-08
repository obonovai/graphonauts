import logging
import pandas as pd
import time

from arangoasync import ArangoClient
from arangoasync.typings import CollectionType
from arangoasync.auth import Auth
from pathlib import Path
from typing import Any

DATA_PATH = Path(__file__).parent.parent.parent / "tpch-osx" / "dbgen"

VERTICES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
EDGES = [
    "nation_region", "customer_nation", "supplier_nation", "customer_orders", "order_lineitems",
    "lineitem_supplier", "lineitem_part", "partsupp_part", "partsupp_supplier"
]
INDICES = {
    "customer": [("c_custkey", True)],
    "orders": [("o_orderkey", True), ("o_custkey", False)],
    "lineitem": [("l_orderkey", False), ("l_partkey", False), ("l_suppkey", False)],
    "partsupp": [("ps_partkey", False), ("ps_suppkey", False)],
    "supplier": [("s_suppkey", True)],
    "part": [("p_partkey", True)],
    "nation": [("n_nationkey", True)],
    "region": [("r_regionkey", True)],
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ArangodbTPCH:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """
        Initialize the ArangodbTPCH instance with configuration details.

        Args:
            config (dict): Configuration dictionary containing connection details.
        """

        self.config = config or {
            "host": "http://localhost:8529",
            "username": "root",
            "password": "password",
            "database": "tpch",
            "graph": "tpchgraph",
        }
        self.client = None
        self.db = None

    async def aconnect(self) -> None:
        """
        Establish a connection to the ArangoDB database.
        """

        self.client = ArangoClient(hosts=self.config["host"])
        auth = Auth(username=self.config["username"], password=self.config["password"])
        sys_db = await self.client.db("_system", auth=auth)
        if not await sys_db.has_database(self.config["database"]):
            await sys_db.create_database(self.config["database"])
        self.db = await self.client.db(self.config["database"], auth=auth)

    async def asetup(self) -> None:
        """
        Set up the database by creating necessary collections and indices.
        """

        for coll in VERTICES:
            if not await self.db.has_collection(coll):
                await self.db.create_collection(coll, col_type=CollectionType.DOCUMENT)

        for coll in EDGES:
            if not await self.db.has_collection(coll):
                await self.db.create_collection(coll, col_type=CollectionType.EDGE)

        for name, indices in INDICES.items():
            if await self.db.has_collection(name):
                collection = self.db.collection(name)
                for field, unique in indices:
                    try:
                        await collection.add_index(
                            type="persistent",
                            fields=[field],
                            options={"unique": unique, "name": f"{name}_{field}_idx"}
                        )
                    except Exception as e:
                        logger.error(f"Index creation warning for {name}_{field}_idx: {e}")

    async def adrop(self) -> None:
        """
        Drop all indices from the database.
        """

        for name, indices in INDICES.items():
            if await self.db.has_collection(name):
                collection = self.db.collection(name)
                try:
                    # Get all indices for the collection
                    all_indices = await collection.indexes()
                    for index in all_indices:
                        # Skip primary and edge indices (they cannot be dropped)
                        if index.get('type') not in ['primary', 'edge']:
                            index_id = index.get('id')
                            if index_id:
                                try:
                                    await collection.delete_index(index_id)
                                except Exception as e:
                                    logger.warning(f"Could not drop index {index_id} from {name}: {e}")
                except Exception as e:
                    logger.warning(f"Error accessing indices for collection {name}: {e}")


    async def aclear(self) -> None:
        """
        Clear all nodes and relationships from the database.
        """

        if await self.db.has_graph(self.config["graph"]):
            await self.db.delete_graph(self.config["graph"], drop_collections=True)
        else:
            for coll in VERTICES + EDGES:
                if await self.db.has_collection(coll):
                    await self.db.delete_collection(coll)

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

    async def agraph(self) -> None:
        """
        Create the graph structure in the database.
        """

        if not await self.db.has_graph(self.config["graph"]):
            graph = await self.db.create_graph(self.config["graph"])
            for coll in EDGES:
                _from, _to = coll.split("_")
                await graph.create_edge_definition(
                    edge_collection=coll,
                    from_vertex_collections=[_from],
                    to_vertex_collections=[_to]
                )

    async def arun(self, query: str, log: bool = True) -> list[dict]:
        """
        Execute a Cypher query and return the results.

        Args:
            query (str): Cypher query to execute.
            log (bool): Whether to log the execution time.
        Returns:
            list: Query results.
        """

        start_time = time.time()
        cursor = await self.db.aql.execute(query)
        results = [doc async for doc in cursor]
        if log:
            logger.info(f"Query executed in {time.time() - start_time:.2f} seconds.")
        return results

    async def aclose(self) -> None:
        """
        Close the connection to the ArangoDB database.
        """

        if self.client:
            await self.client.close()

    async def _aload_region(self):
        """
        Load data from the region.tbl file into the database.
        """

        path = DATA_PATH / "region.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(3))
        df.columns = ["r_regionkey", "r_name", "r_comment"]

        data = []
        for _, row in df.iterrows():
            doc = {
                "_key": str(row["r_regionkey"]),
                "r_regionkey": row["r_regionkey"],
                "r_name": row["r_name"],
                "r_comment": row["r_comment"]
            }
            data.append(doc)

        logger.info("Loading Region data...")
        start_time = time.time()
        region_collection = self.db.collection("region")
        await region_collection.insert_many(data)
        logger.info(f"Region data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_nation(self):
        """
        Load data from the nation.tbl file into the database.
        """

        path = DATA_PATH / "nation.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(4))
        df.columns = ["n_nationkey", "n_name", "n_regionkey", "n_comment"]

        nation_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create nation document
            nation_doc = {
                "_key": str(row["n_nationkey"]),
                "n_nationkey": row["n_nationkey"],
                "n_name": row["n_name"],
                "n_comment": row["n_comment"]
            }
            nation_data.append(nation_doc)

            # Create edge from nation to region
            edge_doc = {
                "_from": f"nation/{row['n_nationkey']}",
                "_to": f"region/{row['n_regionkey']}",
                "n_regionkey": row["n_regionkey"]
            }
            edge_data.append(edge_doc)

        logger.info("Loading Nation data...")
        start_time = time.time()

        # Insert nations
        nation_collection = self.db.collection("nation")
        await nation_collection.insert_many(nation_data)
        # Insert nation-region relationships
        nation_region_collection = self.db.collection("nation_region")
        await nation_region_collection.insert_many(edge_data)

        logger.info(f"Nation data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_supplier(self):
        """
        Load data from the supplier.tbl file into the database.
        """

        path = DATA_PATH / "supplier.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(7))
        df.columns = ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"]

        supplier_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create supplier document
            supplier_doc = {
                "_key": str(row["s_suppkey"]),
                "s_suppkey": row["s_suppkey"],
                "s_name": row["s_name"],
                "s_address": row["s_address"],
                "s_phone": row["s_phone"],
                "s_acctbal": row["s_acctbal"],
                "s_comment": row["s_comment"]
            }
            supplier_data.append(supplier_doc)

            # Create edge from supplier to nation
            edge_doc = {
                "_from": f"supplier/{row['s_suppkey']}",
                "_to": f"nation/{row['s_nationkey']}",
                "s_nationkey": row["s_nationkey"]
            }
            edge_data.append(edge_doc)

        logger.info("Loading Supplier data...")
        start_time = time.time()

        # Insert suppliers
        supplier_collection = self.db.collection("supplier")
        await supplier_collection.insert_many(supplier_data)
        # Insert supplier-nation relationships
        supplier_nation_collection = self.db.collection("supplier_nation")
        await supplier_nation_collection.insert_many(edge_data)

        logger.info(f"Supplier data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_customer(self):
        """
        Load data from the customer.tbl file into the database.
        """

        path = DATA_PATH / "customer.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(8))
        df.columns = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"]

        customer_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create customer document
            customer_doc = {
                "_key": str(row["c_custkey"]),
                "c_custkey": row["c_custkey"],
                "c_name": row["c_name"],
                "c_address": row["c_address"],
                "c_phone": row["c_phone"],
                "c_acctbal": row["c_acctbal"],
                "c_mktsegment": row["c_mktsegment"],
                "c_comment": row["c_comment"]
            }
            customer_data.append(customer_doc)

            # Create edge from customer to nation
            edge_doc = {
                "_from": f"customer/{row['c_custkey']}",
                "_to": f"nation/{row['c_nationkey']}",
                "c_nationkey": row["c_nationkey"]
            }
            edge_data.append(edge_doc)

        logger.info("Loading Customer data...")
        start_time = time.time()

        # Insert customers
        customer_collection = self.db.collection("customer")
        await customer_collection.insert_many(customer_data)
        # Insert customer-nation relationships
        customer_nation_collection = self.db.collection("customer_nation")
        await customer_nation_collection.insert_many(edge_data)

        logger.info(f"Customer data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_part(self):
        """
        Load data from the part.tbl file into the database.
        """

        path = DATA_PATH / "part.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"]

        data = []
        for _, row in df.iterrows():
            doc = {
                "_key": str(row["p_partkey"]),
                "p_partkey": row["p_partkey"],
                "p_name": row["p_name"],
                "p_mfgr": row["p_mfgr"],
                "p_brand": row["p_brand"],
                "p_type": row["p_type"],
                "p_size": row["p_size"],
                "p_container": row["p_container"],
                "p_retailprice": row["p_retailprice"],
                "p_comment": row["p_comment"]
            }
            data.append(doc)

        logger.info("Loading Part data...")
        start_time = time.time()
        part_collection = self.db.collection("part")
        await part_collection.insert_many(data)
        logger.info(f"Part data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_partsupp(self):
        """
        Load data from the partsupp.tbl file into the database.
        """

        path = DATA_PATH / "partsupp.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(5))
        df.columns = ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"]

        partsupp_data = []
        part_edges = []
        supplier_edges = []

        for _, row in df.iterrows():
            # Create partsupp document with composite key
            partsupp_key = f"{row['ps_partkey']}_{row['ps_suppkey']}"
            partsupp_doc = {
                "_key": partsupp_key,
                "ps_partkey": row["ps_partkey"],
                "ps_suppkey": row["ps_suppkey"],
                "ps_availqty": row["ps_availqty"],
                "ps_supplycost": row["ps_supplycost"],
                "ps_comment": row["ps_comment"]
            }
            partsupp_data.append(partsupp_doc)

            # Create edge from partsupp to part
            part_edge = {
                "_from": f"partsupp/{partsupp_key}",
                "_to": f"part/{row['ps_partkey']}",
                "ps_partkey": row["ps_partkey"]
            }
            part_edges.append(part_edge)

            # Create edge from partsupp to supplier
            supplier_edge = {
                "_from": f"partsupp/{partsupp_key}",
                "_to": f"supplier/{row['ps_suppkey']}",
                "ps_suppkey": row["ps_suppkey"]
            }
            supplier_edges.append(supplier_edge)

        logger.info("Loading PartSupp data...")
        start_time = time.time()

        # Insert partsupp documents
        partsupp_collection = self.db.collection("partsupp")
        await partsupp_collection.insert_many(partsupp_data)
        # Insert partsupp-part relationships
        partsupp_part_collection = self.db.collection("partsupp_part")
        await partsupp_part_collection.insert_many(part_edges)
        # Insert partsupp-supplier relationships
        partsupp_supplier_collection = self.db.collection("partsupp_supplier")
        await partsupp_supplier_collection.insert_many(supplier_edges)

        logger.info(f"PartSupp data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_orders(self):
        """
        Load data from the orders.tbl file into the database.
        """

        path = DATA_PATH / "orders.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = [
            "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
            "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"
        ]

        orders_data = []
        edge_data = []

        for _, row in df.iterrows():
            # Create order document
            order_doc = {
                "_key": str(row["o_orderkey"]),
                "o_orderkey": row["o_orderkey"],
                "o_orderstatus": row["o_orderstatus"],
                "o_totalprice": row["o_totalprice"],
                "o_orderdate": row["o_orderdate"],
                "o_orderpriority": row["o_orderpriority"],
                "o_clerk": row["o_clerk"],
                "o_shippriority": row["o_shippriority"],
                "o_comment": row["o_comment"]
            }
            orders_data.append(order_doc)

            # Create edge from customer to order
            edge_doc = {
                "_from": f"customer/{row['o_custkey']}",
                "_to": f"orders/{row['o_orderkey']}",
                "o_custkey": row["o_custkey"]
            }
            edge_data.append(edge_doc)

        logger.info("Loading Orders data...")
        start_time = time.time()

        # Insert orders
        orders_collection = self.db.collection("orders")
        await orders_collection.insert_many(orders_data)
        # Insert customer-orders relationships
        customer_orders_collection = self.db.collection("customer_orders")
        await customer_orders_collection.insert_many(edge_data)

        logger.info(f"Orders data loaded in {time.time() - start_time:.2f} seconds.")

    async def _aload_lineitem(self):
        """
        Load data from the lineitem.tbl file into the database.
        """

        path = DATA_PATH / "lineitem.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(16))
        df.columns = [
            "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice",
            "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
            "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"
        ]

        # Process in batches to avoid connection timeout
        batch_size = 1000
        total_rows = len(df)
        total_batches = (total_rows + batch_size - 1) // batch_size

        logger.info("Loading LineItem data...")
        start_time = time.time()

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            # Convert to list of dictionaries and add _key field for ArangoDB
            lineitem_data = []
            order_edges = []
            part_edges = []
            supplier_edges = []

            for _, row in batch_df.iterrows():
                # Create lineitem document with composite key
                lineitem_key = f"{row['l_orderkey']}_{row['l_linenumber']}"
                lineitem_doc = {
                    "_key": lineitem_key,
                    "l_orderkey": row["l_orderkey"],
                    "l_partkey": row["l_partkey"],
                    "l_suppkey": row["l_suppkey"],
                    "l_linenumber": row["l_linenumber"],
                    "l_quantity": row["l_quantity"],
                    "l_extendedprice": row["l_extendedprice"],
                    "l_discount": row["l_discount"],
                    "l_tax": row["l_tax"],
                    "l_returnflag": row["l_returnflag"],
                    "l_linestatus": row["l_linestatus"],
                    "l_shipdate": row["l_shipdate"],
                    "l_commitdate": row["l_commitdate"],
                    "l_receiptdate": row["l_receiptdate"],
                    "l_shipinstruct": row["l_shipinstruct"],
                    "l_shipmode": row["l_shipmode"],
                    "l_comment": row["l_comment"]
                }
                lineitem_data.append(lineitem_doc)

                # Create edge from order to lineitem
                order_edge = {
                    "_from": f"orders/{row['l_orderkey']}",
                    "_to": f"lineitem/{lineitem_key}",
                    "l_orderkey": row["l_orderkey"]
                }
                order_edges.append(order_edge)

                # Create edge from lineitem to part
                part_edge = {
                    "_from": f"lineitem/{lineitem_key}",
                    "_to": f"part/{row['l_partkey']}",
                    "l_partkey": row["l_partkey"]
                }
                part_edges.append(part_edge)

                # Create edge from lineitem to supplier
                supplier_edge = {
                    "_from": f"lineitem/{lineitem_key}",
                    "_to": f"supplier/{row['l_suppkey']}",
                    "l_suppkey": row["l_suppkey"]
                }
                supplier_edges.append(supplier_edge)

            # Insert lineitem documents
            lineitem_collection = self.db.collection("lineitem")
            await lineitem_collection.insert_many(lineitem_data)
            # Insert order-lineitem relationships
            order_lineitems_collection = self.db.collection("order_lineitems")
            await order_lineitems_collection.insert_many(order_edges)
            # Insert lineitem-part relationships
            lineitem_part_collection = self.db.collection("lineitem_part")
            await lineitem_part_collection.insert_many(part_edges)
            # Insert lineitem-supplier relationships
            lineitem_supplier_collection = self.db.collection("lineitem_supplier")
            await lineitem_supplier_collection.insert_many(supplier_edges)

        logger.info(f"LineItem data loaded in {time.time() - start_time:.2f} seconds.")
