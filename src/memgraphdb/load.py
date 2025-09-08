import asyncio
from memgraphdb.client import MemgraphTPCH


async def main():
    client = MemgraphTPCH()
    await client.aconnect()
    await client.aclear()
    await client.asetup()
    await client.aload()
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
