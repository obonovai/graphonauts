"""ArangoDB async client implementation wrapping the synchronous python-arango driver.

Communicates via the HTTP REST API. The ``python-arango`` library is synchronous,
so all I/O operations are offloaded to a thread pool via ``asyncio.to_thread()``
to satisfy the framework's async ``BaseClient`` protocol.

In addition to the five protocol methods (``connect``, ``areconnect``, ``aexecute``,
``afetch``, ``aclose``), this client exposes helper methods for collection and graph
management that the loader uses during data ingestion.
"""

import asyncio
from typing import Any

from arango.client import ArangoClient as _ArangoClient
from arango.database import StandardDatabase


class ArangoDBClient:
    """Async ArangoDB client with connection management and query execution.

    Wraps the synchronous ``python-arango`` driver. Each I/O call is offloaded
    to a thread via ``asyncio.to_thread()`` so the asyncio event loop is never
    blocked.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {
            "host": "http://localhost:8529",
            "username": "root",
            "password": "password",
            "db_name": "graphonauts",
        }

        self.client: _ArangoClient | None = None
        self.db: StandardDatabase | None = None

    def connect(self) -> None:
        """Initialise the ArangoDB HTTP client and ensure the target database exists.

        Creates the ``python-arango`` client object and a handle to the working
        database (creating it if it does not already exist). The client constructor
        is lightweight (no network I/O); the database existence check is a single
        HTTP call.
        """
        self.client = _ArangoClient(hosts=self.config["host"], request_timeout=900)

        sys_db = self.client.db(
            "_system",
            username=self.config["username"],
            password=self.config["password"],
        )
        db_name = self.config["db_name"]
        if not sys_db.has_database(db_name):
            sys_db.create_database(db_name)

        self.db = self.client.db(
            db_name,
            username=self.config["username"],
            password=self.config["password"],
        )

    async def areconnect(self) -> None:
        """Close the existing connection and establish a new one."""
        await self.aclose()
        self.connect()

    async def aexecute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Execute an AQL query without returning a structured result set.

        Intended for DDL operations, data modifications, and other side-effect-only
        queries. The cursor is consumed to completion to ensure the query finishes.

        Args:
            query: An AQL query string.
            params: Optional bind variables (keys without the ``@`` prefix).

        Returns:
            None.
        """
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")

        def _execute() -> None:
            assert self.db is not None
            cursor = self.db.aql.execute(query, bind_vars=params or {})
            # Consume cursor to ensure query completes
            for _ in cursor:  # type: ignore[union-attr]
                pass

        await asyncio.to_thread(_execute)

    async def afetch(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute an AQL query and return all result records as dictionaries.

        Args:
            query: An AQL query string.
            params: Optional bind variables (keys without the ``@`` prefix).

        Returns:
            A list of dictionaries, one per result record.
        """
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")

        def _fetch() -> list[dict[str, Any]]:
            assert self.db is not None
            cursor = self.db.aql.execute(query, bind_vars=params or {})
            return [doc for doc in cursor]  # type: ignore[union-attr]

        return await asyncio.to_thread(_fetch)

    async def aclose(self) -> None:
        """Close the HTTP client and release all associated resources."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None

    # --- Helper methods for the loader (not part of BaseClient protocol) ---

    async def acreate_collection(self, name: str, edge: bool = False) -> None:
        """Create a document or edge collection."""
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")
        await asyncio.to_thread(self.db.create_collection, name, edge=edge)

    async def ainsert_many(self, collection: str, docs: list[dict[str, Any]]) -> None:
        """Batch-insert documents into a collection."""
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")

        def _insert() -> None:
            assert self.db is not None
            self.db.collection(collection).insert_many(docs)

        await asyncio.to_thread(_insert)

    async def adrop_collection(self, name: str) -> None:
        """Drop a collection, ignoring if it does not exist."""
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")
        await asyncio.to_thread(self.db.delete_collection, name, ignore_missing=True)

    async def acreate_graph(self, name: str, edge_definitions: list[dict[str, Any]]) -> None:
        """Create a named graph with the given edge definitions."""
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")
        await asyncio.to_thread(self.db.create_graph, name, edge_definitions=edge_definitions)

    async def adrop_graph(self, name: str) -> None:
        """Drop a named graph without dropping its collections."""
        if not self.db:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")
        await asyncio.to_thread(self.db.delete_graph, name, ignore_missing=True, drop_collections=False)
