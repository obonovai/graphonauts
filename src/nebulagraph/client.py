import logging
import pandas as pd
import time
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from pathlib import Path
from typing import Any

DATA_PATH = Path(__file__).parent.parent.parent / "tpch-osx" / "dbgen"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NebulagraphTPCH:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """
        Initialize the NebulaGraphTPCH instance with configuration details.

        Args:
            config (dict): Configuration dictionary containing connection details.
        """

        self.config = config or {
            "host": "127.0.0.1",
            "port": 9669,
            "user": "root",
            "password": "nebula",
            "space": "tpch",
        }
        self.connection: ConnectionPool | None = None

    def connect(self) -> None:
        """
        Establish a connection to the Neo4j database.
        """

        self.connection = ConnectionPool()
        if not self.connection.init([(self.config["host"], self.config["port"])], Config()):
            raise RuntimeError("Connection error")

    def setup(self) -> None:
        """
        Set up the database schema for TPC-H data.
        """

        with self.connection.session_context(self.config["user"], self.config["password"]) as session:
            session.execute(f'CREATE SPACE IF NOT EXISTS {self.config["space"]} (partition_num = 15, replica_factor = 1, vid_type = INT64);')
            time.sleep(30)  # Increase wait time for space creation propagation

        with self.connection.session_context(self.config["user"], self.config["password"]) as session:
            session.execute(f'USE {self.config["space"]};')
            time.sleep(30)  # Increase wait time for space propagation

            # Create vertex types
            session.execute("""
                CREATE TAG IF NOT EXISTS Region (
                    regionkey INT NOT NULL,
                    name STRING,
                    comment STRING
                );
            """)
            session.execute("""
                CREATE TAG IF NOT EXISTS Nation (
                    nationkey INT NOT NULL,
                    name STRING,
                    regionkey INT,
                    comment STRING
                );
            """)
            session.execute("""
                CREATE TAG IF NOT EXISTS Supplier (
                    suppkey INT NOT NULL,
                    name STRING,
                    address STRING,
                    nationkey INT,
                    phone STRING,
                    acctbal DOUBLE,
                    comment STRING
                );
            """)
            session.execute("""
                CREATE TAG IF NOT EXISTS Customer (
                    custkey INT NOT NULL,
                    name STRING,
                    address STRING,
                    nationkey INT,
                    phone STRING,
                    acctbal DOUBLE,
                    mktsegment STRING,
                    comment STRING
                );
            """)
            session.execute("""
                CREATE TAG IF NOT EXISTS Part (
                    partkey INT NOT NULL,
                    name STRING,
                    mfgr STRING,
                    brand STRING,
                    type STRING,
                    size INT,
                    container STRING,
                    retailprice DOUBLE,
                    comment STRING
                );
            """)
            session.execute("""
                CREATE TAG IF NOT EXISTS `Order` (
                    orderkey INT NOT NULL,
                    custkey INT,
                    orderstatus STRING,
                    totalprice DOUBLE,
                    orderdate STRING,
                    orderpriority STRING,
                    clerk STRING,
                    shippriority INT,
                    comment STRING
                );
            """)
            session.execute("""
                CREATE TAG IF NOT EXISTS LineItem (
                    lineitemkey INT NOT NULL,
                    orderkey INT NOT NULL,
                    partkey INT NOT NULL,
                    suppkey INT NOT NULL,
                    linenumber INT NOT NULL,
                    quantity DOUBLE,
                    extendedprice DOUBLE,
                    discount DOUBLE,
                    tax DOUBLE,
                    returnflag STRING,
                    linestatus STRING,
                    shipdate STRING,
                    commitdate STRING,
                    receiptdate STRING,
                    shipinstruct STRING,
                    shipmode STRING,
                    comment STRING
                );
            """)

            # Create edge types
            session.execute('CREATE EDGE IF NOT EXISTS BELONGS_TO ();')
            session.execute('CREATE EDGE IF NOT EXISTS LOCATED_IN ();')
            session.execute('CREATE EDGE IF NOT EXISTS SUPPLIES (availqty INT, supplycost DOUBLE, comment STRING);')
            session.execute('CREATE EDGE IF NOT EXISTS PLACED ();')
            session.execute('CREATE EDGE IF NOT EXISTS CONTAINS ();')
            session.execute('CREATE EDGE IF NOT EXISTS OF_PART ();')
            session.execute('CREATE EDGE IF NOT EXISTS SUPPLIED_BY ();')

            time.sleep(30)  # Increase wait time for schema propagation

    def drop(self) -> None:
        """
        Drop all indices from the database.
        """

        # NebulaGraph doesn't require explicit index dropping like Neo4j
        # Indices are automatically managed when the space is recreated

    def clear(self) -> None:
        """
        Clear all nodes and relationships from the database.
        """

        with self.connection.session_context(self.config["user"], self.config["password"]) as session:
            try:
                session.execute(f'DROP SPACE IF EXISTS {self.config["space"]};')
                time.sleep(30)  # Increase wait time for space drop propagation
            except Exception as e:
                logger.error(f"Error clearing space: {e}")

    def load(self):
        """
        Load all TPC-H tables into the database in the correct order.
        """

        logger.info("Starting TPC-H data load...")
        start_time = time.time()

        # Load in dependency order
        self._aload_region()
        self._aload_nation()
        self._aload_supplier()
        self._aload_customer()
        self._aload_part()
        self._aload_partsupp()
        self._aload_orders()
        self._aload_lineitem()

        logger.info(f"All tables loaded successfully in {time.time() - start_time:.2f} seconds!")

    def run(self, query: str, params: dict[str, Any] | None = None, log: bool = True) -> any:
        """
        Execute a nGQL query and return the results.

        Args:
            query (str): nGQL query to execute.
            params (dict): Parameters for the query (not used in NebulaGraph).
            log (bool): Whether to log the query execution.

        Returns:
            Result object from NebulaGraph.
        """

        if not self.connection:
            raise RuntimeError("Not connected to the database")

        start_time = time.time()
        with self.connection.session_context(self.config["user"], self.config["password"]) as session:
            session.execute(f"USE {self.config['space']};")
            time.sleep(10)
            result = session.execute(query)
            if result.is_succeeded():
                if log:
                    logger.info(f"Query executed in {time.time() - start_time:.2f} seconds.")
                return result
            else:
                raise RuntimeError(f"Query failed: {result.error_msg()}")

    def close(self) -> None:
        """
        Close the connection to the Neo4j database.
        """

        self.connection.close()

    def _aload_region(self):
        """
        Load data from the region.tbl file into the database.
        """

        path = DATA_PATH / "region.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(3))
        df.columns = ["regionkey", "name", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["name"] = df["name"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        logger.info("Loading Region data...")
        start_time = time.time()
        self._load_batch_vertices("Region", data, "regionkey")
        logger.info(f"Region data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_nation(self):
        """
        Load data from the nation.tbl file into the database.
        """

        path = DATA_PATH / "nation.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(4))
        df.columns = ["nationkey", "name", "regionkey", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["name"] = df["name"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        # Create relationships between nations and regions
        nation_region_edges = []
        for row in data:
            nation_region_edges.append({
                "nationkey": row["nationkey"],
                "regionkey": row["regionkey"]
            })

        logger.info("Loading Nation data...")
        start_time = time.time()
        self._load_batch_vertices("Nation", data, "nationkey")
        self._load_batch_edges("LOCATED_IN", nation_region_edges, "nationkey", "regionkey")
        logger.info(f"Nation data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_supplier(self):
        """
        Load data from the supplier.tbl file into the database.
        """

        path = DATA_PATH / "supplier.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(7))
        df.columns = ["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["name"] = df["name"].astype(str).str.strip()
        df["address"] = df["address"].astype(str).str.strip()
        df["phone"] = df["phone"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        # Create relationships between suppliers and nations
        supplier_nation_edges = []
        for row in data:
            supplier_nation_edges.append({
                "suppkey": row["suppkey"],
                "nationkey": row["nationkey"]
            })

        logger.info("Loading Supplier data...")
        start_time = time.time()
        self._load_batch_vertices("Supplier", data, "suppkey")
        self._load_batch_edges("BELONGS_TO", supplier_nation_edges, "suppkey", "nationkey")
        logger.info(f"Supplier data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_customer(self):
        """
        Load data from the customer.tbl file into the database.
        """

        path = DATA_PATH / "customer.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(8))
        df.columns = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["name"] = df["name"].astype(str).str.strip()
        df["address"] = df["address"].astype(str).str.strip()
        df["phone"] = df["phone"].astype(str).str.strip()
        df["mktsegment"] = df["mktsegment"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        # Create relationships between customers and nations
        customer_nation_edges = []
        for row in data:
            customer_nation_edges.append({
                "custkey": row["custkey"],
                "nationkey": row["nationkey"]
            })

        logger.info("Loading Customer data...")
        start_time = time.time()
        self._load_batch_vertices("Customer", data, "custkey")
        self._load_batch_edges("BELONGS_TO", customer_nation_edges, "custkey", "nationkey")
        logger.info(f"Customer data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_part(self):
        """
        Load data from the part.tbl file into the database.
        """

        path = DATA_PATH / "part.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["name"] = df["name"].astype(str).str.strip()
        df["mfgr"] = df["mfgr"].astype(str).str.strip()
        df["brand"] = df["brand"].astype(str).str.strip()
        df["type"] = df["type"].astype(str).str.strip()
        df["container"] = df["container"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        logger.info("Loading Part data...")
        start_time = time.time()
        self._load_batch_vertices("Part", data, "partkey")
        logger.info(f"Part data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_partsupp(self):
        """
        Load data from the partsupp.tbl file into the database.
        """

        path = DATA_PATH / "partsupp.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(5))
        df.columns = ["partkey", "suppkey", "availqty", "supplycost", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        logger.info("Loading PartSupp data...")
        start_time = time.time()
        self._load_batch_edges("SUPPLIES", data, "suppkey", "partkey")
        logger.info(f"PartSupp data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_orders(self):
        """
        Load data from the orders.tbl file into the database.
        """

        path = DATA_PATH / "orders.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(9))
        df.columns = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["orderstatus"] = df["orderstatus"].astype(str).str.strip()
        df["orderdate"] = df["orderdate"].astype(str).str.strip()
        df["orderpriority"] = df["orderpriority"].astype(str).str.strip()
        df["clerk"] = df["clerk"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        # Create relationships between orders and customers
        order_customer_edges = []
        for row in data:
            order_customer_edges.append({
                "orderkey": row["orderkey"],
                "custkey": row["custkey"]
            })

        logger.info("Loading Orders data...")
        start_time = time.time()
        self._load_batch_vertices("`Order`", data, "orderkey")
        self._load_batch_edges("PLACED", order_customer_edges, "custkey", "orderkey")
        logger.info(f"Orders data loaded in {time.time() - start_time:.2f} seconds.")

    def _aload_lineitem(self):
        """
        Load data from the lineitem.tbl file into the database.
        """

        path = DATA_PATH / "lineitem.tbl"
        df = pd.read_csv(path, sep="|", header=None, usecols=range(16))
        df.columns = ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice",
                     "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate",
                     "receiptdate", "shipinstruct", "shipmode", "comment"]

        # Clean up any trailing whitespace and handle empty values
        df = df.fillna("")
        df["returnflag"] = df["returnflag"].astype(str).str.strip()
        df["linestatus"] = df["linestatus"].astype(str).str.strip()
        df["shipdate"] = df["shipdate"].astype(str).str.strip()
        df["commitdate"] = df["commitdate"].astype(str).str.strip()
        df["receiptdate"] = df["receiptdate"].astype(str).str.strip()
        df["shipinstruct"] = df["shipinstruct"].astype(str).str.strip()
        df["shipmode"] = df["shipmode"].astype(str).str.strip()
        df["comment"] = df["comment"].astype(str).str.strip()

        data = df.to_dict(orient="records")

        # Create LineItem vertices with composite key (orderkey + linenumber)
        lineitem_data = []
        for row in data:
            # Create a unique vertex ID by combining orderkey and linenumber
            lineitem_id = int(f"{row['orderkey']}{row['linenumber']:04d}")
            lineitem_row = row.copy()
            lineitem_row['lineitemkey'] = lineitem_id
            lineitem_data.append(lineitem_row)

        # Create CONTAINS edges from orders to line items
        order_lineitem_edges = []
        lineitem_part_edges = []
        lineitem_supplier_edges = []

        for row in lineitem_data:
            lineitem_id = row['lineitemkey']

            # Order contains line item
            order_lineitem_edges.append({
                "orderkey": row["orderkey"],
                "lineitemkey": lineitem_id
            })

            # Line item is of part
            lineitem_part_edges.append({
                "lineitemkey": lineitem_id,
                "partkey": row["partkey"]
            })

            # Line item is supplied by supplier
            lineitem_supplier_edges.append({
                "lineitemkey": lineitem_id,
                "suppkey": row["suppkey"]
            })

        logger.info("Loading LineItem data...")
        start_time = time.time()
        self._load_batch_vertices("LineItem", lineitem_data, "lineitemkey")
        self._load_batch_edges("CONTAINS", order_lineitem_edges, "orderkey", "lineitemkey")
        self._load_batch_edges("OF_PART", lineitem_part_edges, "lineitemkey", "partkey")
        self._load_batch_edges("SUPPLIED_BY", lineitem_supplier_edges, "lineitemkey", "suppkey")
        logger.info(f"LineItem data loaded in {time.time() - start_time:.2f} seconds.")

    def _load_batch_vertices(self, tag: str, data: list[dict], key_field: str, batch_size: int = 1000) -> None:
        """
        Load vertices in batches.
        """

        with self.connection.session_context('root', 'nebula') as session:
            # Use the space
            result = session.execute(f'USE {self.config["space"]};')
            time.sleep(30)  # Increase wait time for space propagation
            if not result.is_succeeded():
                logger.error(f"Failed to use space: {result.error_msg()}")
                return

            if not data:
                return

            # Get all property names (including the key field)
            prop_names = list(data[0].keys())

            total_batches = (len(data) + batch_size - 1) // batch_size
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]

                # Build INSERT VERTEX query
                values_parts = []
                for row in batch:
                    vid = row[key_field]
                    # Build property values string for all properties
                    props = []
                    for prop_name in prop_names:
                        value = row.get(prop_name, "")
                        if isinstance(value, str):
                            # Escape quotes in strings
                            escaped_value = value.replace('"', '\\"')
                            props.append(f'"{escaped_value}"')
                        elif value is None or value == "":
                            props.append('""')
                        else:
                            props.append(str(value))

                    values_parts.append(f'{vid}: ({", ".join(props)})')

                # Include property names in the query
                prop_list = ", ".join(prop_names)
                query = f'INSERT VERTEX {tag}({prop_list}) VALUES {", ".join(values_parts)};'

                try:
                    result = session.execute(query)
                    if not result.is_succeeded():
                        logger.error(f"Error in batch {i // batch_size + 1}: {result.error_msg()}")
                        logger.error(f"Query was: {query[:200]}...")
                except Exception as e:
                    logger.error(f"Exception in batch {i // batch_size + 1}: {e}")


    def _load_batch_edges(self, edge_type: str, data: list[dict], src_field: str, dst_field: str, batch_size: int = 1000) -> None:
        """
        Load edges in batches
        """

        with self.connection.session_context('root', 'nebula') as session:
            result = session.execute(f'USE {self.config["space"]};')
            time.sleep(30)  # Increase wait time for space propagation
            if not result.is_succeeded():
                logger.error(f"Failed to use space: {result.error_msg()}")
                return

            if not data:
                return

            # Get property names (excluding src and dst fields)
            prop_names = [key for key in data[0].keys() if key not in [src_field, dst_field]]

            total_batches = (len(data) + batch_size - 1) // batch_size
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]

                # Build INSERT EDGE query
                values_parts = []
                for row in batch:
                    src_id = row[src_field]
                    dst_id = row[dst_field]

                    # Build property values string (excluding src and dst fields)
                    props = []
                    for prop_name in prop_names:
                        value = row.get(prop_name, "")
                        if isinstance(value, str):
                            # Escape quotes in strings
                            escaped_value = value.replace('"', '\\"')
                            props.append(f'"{escaped_value}"')
                        elif value is None or value == "":
                            props.append('""')
                        else:
                            props.append(str(value))

                    if props:
                        values_parts.append(f'{src_id} -> {dst_id}: ({", ".join(props)})')
                    else:
                        values_parts.append(f'{src_id} -> {dst_id}: ()')

                # Include property names in the query if there are any
                if prop_names:
                    prop_list = ", ".join(prop_names)
                    query = f'INSERT EDGE {edge_type}({prop_list}) VALUES {", ".join(values_parts)};'
                else:
                    query = f'INSERT EDGE {edge_type} VALUES {", ".join(values_parts)};'

                try:
                    result = session.execute(query)
                    if not result.is_succeeded():
                        logger.error(f"Error in batch {i // batch_size + 1}: {result.error_msg()}")
                        logger.error(f"Query was: {query[:200]}...")
                except Exception as e:
                    logger.error(f"Exception in batch {i // batch_size + 1}: {e}")
