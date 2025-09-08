import asyncio
from neo4jdb.client import Neo4jTPCH


async def main():
    client = Neo4jTPCH()
    await client.aconnect()
    await client.aclear()
    await client.asetup()
    await client.aload()
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
