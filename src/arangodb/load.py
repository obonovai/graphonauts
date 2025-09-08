import asyncio
from arangodb.client import ArangodbTPCH


async def main():
    client = ArangodbTPCH()
    await client.aconnect()
    await client.aclear()
    await client.asetup()
    await client.aload()
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
