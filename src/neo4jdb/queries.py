import asyncio
import logging
from neo4jdb.client import Neo4jTPCH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    db = Neo4jTPCH()
    await db.aconnect()
    await db.adrop()

    # A. Selection, Projection, Source (of Data)

    # A1. Non-Indexed Columns: Select supplier named 'Supplier#000000666'
    logger.info("Non-Indexed Columns: Select supplier named 'Supplier#000000666'")
    query_a1 = """
        MATCH (s:Supplier {name: 'Supplier#000000666'})
        RETURN s.suppkey, s.name, s.address, s.phone
    """
    logger.info(await db.arun(query_a1))

    # A2. Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31
    logger.info("Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31")
    query_a2 = """
        MATCH (o:Order)
        WHERE o.orderdate >= '1990-01-01' AND o.orderdate <= '1995-12-31'
        RETURN o.orderkey, o.orderdate, o.totalprice
    """
    logger.info(f"Matched records: {len(await db.arun(query_a2))}")

    # A3. Indexed Columns: Select supplier with the ID 1337
    await db.arun("CREATE INDEX supplier_key IF NOT EXISTS FOR (s:Supplier) ON (s.suppkey)", log=False)

    logger.info("Indexed Columns: Select supplier with the ID 1337")
    query_a3 = """
        MATCH (s:Supplier {suppkey: 1337})
        RETURN s.suppkey, s.name, s.address, s.phone
    """
    logger.info(await db.arun(query_a3))

    # A4. Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31
    await db.arun("CREATE INDEX order_key IF NOT EXISTS FOR (o:Order) ON (o.orderkey, o.orderdate)", log=False)

    logger.info("Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31")
    query_a4 = """
        MATCH (o:Order)
        WHERE o.orderdate >= '1990-01-01' AND o.orderdate <= '1995-12-31'
        RETURN o.orderkey, o.orderdate, o.totalprice
    """
    logger.info(f"Matched records: {len(await db.arun(query_a4))}")

    # B. Aggregation

    # B1. COUNT: Count the number of products per brand
    logger.info("COUNT: Count the number of products per brand")
    query_b1 = """
        MATCH (p:Part)
        RETURN p.brand AS brand, COUNT(p) AS product_count
        ORDER BY product_count DESC
    """
    logger.info(await db.arun(query_b1))

    # B2. MAX: Find the most expensive product per brand
    logger.info("MAX: Find the most expensive product per brand")
    query_b2 = """
        MATCH (p:Part)
        RETURN p.brand AS brand, MAX(p.retailprice) AS max_price
        ORDER BY max_price DESC
    """
    logger.info(await db.arun(query_b2))

    await db.aclose()


if __name__ == "__main__":
    asyncio.run(main())
