import asyncio
import logging
from arangodb.client import ArangodbTPCH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    db = ArangodbTPCH()
    await db.aconnect()

    # Get database statistics
    logger.info("=== DATABASE STORAGE INFORMATION ===")

    # Get collection statistics
    logger.info("\n=== COLLECTION STATISTICS ===")
    collection_stats_query = """
        FOR collection IN COLLECTIONS()
        LET stats = COLLECTION_COUNT(collection.name)
        RETURN {
            name: collection.name,
            type: collection.type == 2 ? "document" : "edge",
            count: stats
        }
    """

    collections = await db.arun(collection_stats_query)
    total_documents = 0

    for coll in collections:
        logger.info(f"Collection: {coll['name']} ({coll['type']}) - Documents: {coll['count']:,}")
        total_documents += coll['count']

    logger.info(f"\nTotal documents across all collections: {total_documents:,}")

    # Get detailed storage information for each collection
    logger.info("\n=== DETAILED COLLECTION INFORMATION ===")

    # Check each collection individually for more detailed stats
    collections_to_check = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

    for coll_name in collections_to_check:
        try:
            # Get document count
            count_query = f"RETURN LENGTH({coll_name})"
            count_result = await db.arun(count_query)
            count = count_result[0] if count_result else 0

            # Get sample document to estimate size
            sample_query = f"""
                FOR doc IN {coll_name}
                LIMIT 1
                RETURN doc
            """
            sample_result = await db.arun(sample_query)

            if sample_result:
                sample_doc = sample_result[0]
                # Estimate document size (rough approximation)
                doc_size_estimate = len(str(sample_doc).encode('utf-8'))
                estimated_total_size = doc_size_estimate * count

                logger.info(f"{coll_name}:")
                logger.info(f"  - Document count: {count:,}")
                logger.info(f"  - Sample doc size: ~{doc_size_estimate:,} bytes")
                logger.info(f"  - Estimated total size: ~{estimated_total_size / (1024*1024):.2f} MB")
                logger.info("")

        except Exception as e:
            logger.warning(f"Could not get stats for {coll_name}: {e}")

    # Get index information
    logger.info("\n=== INDEX INFORMATION ===")
    for coll_name in collections_to_check:
        try:
            collection = db.db.collection(coll_name)
            indices = await collection.indexes()
            logger.info(f"{coll_name} indices:")
            for idx in indices:
                logger.info(f"  - {idx.get('name', 'unnamed')}: {idx.get('type')} on {idx.get('fields', [])}")
        except Exception as e:
            logger.warning(f"Could not get indices for {coll_name}: {e}")

    await db.aclose()


if __name__ == "__main__":
    asyncio.run(main())
