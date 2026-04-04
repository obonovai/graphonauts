from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession


class MemgraphClient:
    """Asynchronous client for Memgraph using the Bolt protocol via the Neo4j Python driver.

    Memgraph implements the Bolt protocol for client communication, which allows the
    Neo4j Python driver (``neo4j.AsyncGraphDatabase``) to be used as a client library.
    This class wraps the async driver and provides the methods required by the
    ``BaseClient`` protocol: ``connect()``, ``areconnect()``, ``afetch()``,
    ``aexecute()``, and ``aclose()``.

    Unlike Neo4j, Memgraph does not require authentication by default, so the ``auth``
    parameter is set to ``None``. The default Bolt endpoint is ``bolt://localhost:7688``
    (Memgraph's standard port, distinct from Neo4j's 7687).

    Attributes:
        config: Connection configuration dictionary. Must contain a ``"uri"`` key
            specifying the Bolt endpoint.
        driver: The underlying ``AsyncDriver`` instance, or ``None`` if not yet
            connected.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {
            "uri": "bolt://localhost:7688",
        }

        self.driver: AsyncDriver | None = None

    def connect(self) -> None:
        """Initialise the asynchronous Bolt driver.

        This method is synchronous because driver construction does not perform
        any network I/O --- the actual TCP connection is established lazily when
        the first session is opened. Authentication is disabled (``auth=None``)
        as Memgraph does not require it by default.
        """
        self.driver = AsyncGraphDatabase.driver(
            uri=self.config["uri"],
            auth=None,
        )

    async def areconnect(self) -> None:
        """Close the existing driver (if any) and create a fresh connection.

        This is used after a container restart during memory benchmarking to
        re-establish the Bolt connection to the restarted database instance.
        """
        await self.aclose()
        self.connect()

    @asynccontextmanager
    async def asession(self) -> AsyncIterator[AsyncSession]:
        if not self.driver:
            raise RuntimeError("Driver is not connected. Call 'connect()' first.")
        async with self.driver.session() as session:
            yield session

    async def aexecute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a Cypher query and return the result summary (discarding records).

        Suitable for write operations (e.g., data loading, index creation) where
        the result records are not needed.

        Args:
            query: The Cypher query string to execute.
            params: Optional dictionary of query parameters.

        Returns:
            The ``ResultSummary`` object from the Neo4j driver.
        """
        async with self.asession() as session:
            result = await session.run(query, params)
            return await result.consume()

    async def afetch(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a Cypher query and return all result records as dictionaries.

        Suitable for read operations where the full result set is needed (e.g.,
        benchmark queries, verification count queries).

        Args:
            query: The Cypher query string to execute.
            params: Optional dictionary of query parameters.

        Returns:
            A list of dictionaries, one per result record, with keys corresponding
            to the returned field names.
        """
        async with self.asession() as session:
            result = await session.run(query, params)
            return [record.data() async for record in result]

    async def aclose(self) -> None:
        """Close the Bolt driver and release all associated resources.

        Safe to call multiple times; subsequent calls are no-ops if the driver
        has already been closed.
        """
        if self.driver:
            await self.driver.close()
