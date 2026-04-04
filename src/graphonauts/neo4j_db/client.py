"""Neo4j async client implementation using the official neo4j Python driver.

Communicates via the Bolt protocol. All query execution is async.
The client wraps the driver's session management into a simple interface
matching the BaseClient protocol.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession


class Neo4jClient:
    """Async Neo4j client with connection management and query execution."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        if config and not all(key in config for key in ["uri", "user", "password"]):
            raise ValueError("Missing required configuration keys (uri, user or password).")

        self.config = config or {
            "uri": "bolt://localhost:7687",
            "user": "neo4j",
            "password": "password",
        }

        self.driver: AsyncDriver | None = None

    def connect(self) -> None:
        self.driver = AsyncGraphDatabase.driver(
            uri=self.config["uri"],
            auth=(self.config["user"], self.config["password"]),
        )

    async def areconnect(self) -> None:
        await self.aclose()
        self.connect()

    @asynccontextmanager
    async def asession(self) -> AsyncIterator[AsyncSession]:
        if not self.driver:
            raise RuntimeError("Driver is not connected. Call 'connect()' first.")
        async with self.driver.session() as session:
            yield session

    async def aexecute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        async with self.asession() as session:
            result = await session.run(query, params)
            return await result.consume()

    async def afetch(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        async with self.asession() as session:
            result = await session.run(query, params)
            return [record.data() async for record in result]

    async def aclose(self) -> None:
        if self.driver:
            await self.driver.close()
