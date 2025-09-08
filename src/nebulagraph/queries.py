import asyncio
import logging
from nebulagraph.prettyprint import print_resp
from nebulagraph.client import NebulagraphTPCH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    db = NebulagraphTPCH()
    db.connect()

    # A. Selection, Projection, Source (of Data)

    # A1. Non-Indexed Columns: Select supplier named 'Supplier#000000666'
    logger.info("Non-Indexed Columns: Select supplier named 'Supplier#000000666'")
    query_a1 = """
        FETCH PROP ON Supplier 666 YIELD vertex as node;
    """
    resp = db.run(query_a1)
    logger.info(print_resp(resp))

    # Other queries to be added...

    db.close()


if __name__ == "__main__":
    main()
