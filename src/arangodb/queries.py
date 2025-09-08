import asyncio
import logging
from arangodb.client import ArangodbTPCH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    db = ArangodbTPCH()
    await db.aconnect()
    await db.agraph()
    await db.adrop()

    # A. Selection, Projection, Source (of Data)

    # A1. Non-Indexed Columns: Select supplier named 'Supplier#000000666'
    logger.info("Non-Indexed Columns: Select supplier named 'Supplier#000000666'")
    query_a1 = """
        FOR s IN supplier
        FILTER s.s_name == 'Supplier#000000666'
        RETURN {
            s_suppkey: s.s_suppkey,
            s_name: s.s_name,
            s_address: s.s_address,
            s_phone: s.s_phone
        }
    """
    logger.info(await db.arun(query_a1))

    # A2. Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31
    logger.info("Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31")
    query_a2 = """
        FOR o IN orders
        FILTER o.o_orderdate >= '1990-01-01' AND o.o_orderdate <= '1995-12-31'
        RETURN {
            o_orderkey: o.o_orderkey,
            o_orderdate: o.o_orderdate,
            o_totalprice: o.o_totalprice
        }
    """
    logger.info(f"Matched records: {len(await db.arun(query_a2))}")

    # A3. Indexed Columns: Select supplier with the ID 1337

    collection = db.db.collection("supplier")
    await collection.add_index(
        type="persistent",
        fields=["s_suppkey"],
        options={"unique": True, "name": "supplier_s_suppkey_query_idx"}
    )

    logger.info("Indexed Columns: Select supplier with the ID 1337")
    query_a3 = """
        FOR s IN supplier
        FILTER s.s_suppkey == 1337
        RETURN {
            s_suppkey: s.s_suppkey,
            s_name: s.s_name,
            s_address: s.s_address,
            s_phone: s.s_phone
        }
    """
    logger.info(await db.arun(query_a3))

    # A4. Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31

    collection = db.db.collection("orders")
    await collection.add_index(
        type="persistent",
        fields=["o_orderkey", "o_orderdate"],
        options={"unique": False, "name": "orders_orderkey_orderdate_query_idx"}
    )

    logger.info("Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31")
    query_a4 = """
        FOR o IN orders
        FILTER o.o_orderdate >= '1990-01-01' AND o.o_orderdate <= '1995-12-31'
        RETURN {
            o_orderkey: o.o_orderkey,
            o_orderdate: o.o_orderdate,
            o_totalprice: o.o_totalprice
        }
    """
    logger.info(f"Matched records: {len(await db.arun(query_a4))}")

    # B. Aggregation

    # B1. COUNT: Count the number of products per brand
    logger.info("COUNT: Count the number of products per brand")
    query_b1 = """
        FOR p IN part
        COLLECT brand = p.p_brand
        AGGREGATE product_count = COUNT(1)
        SORT product_count DESC
        RETURN {
            brand: brand,
            product_count: product_count
        }
    """
    logger.info(await db.arun(query_b1))

    # B2. MAX: Find the most expensive product per brand
    logger.info("MAX: Find the most expensive product per brand")
    query_b2 = """
        FOR p IN part
        COLLECT brand = p.p_brand
        AGGREGATE max_price = MAX(p.p_retailprice)
        SORT max_price DESC
        RETURN {
            brand: brand,
            max_price: max_price
        }
    """
    logger.info(await db.arun(query_b2))

    await db.aclose()


if __name__ == "__main__":
    asyncio.run(main())
