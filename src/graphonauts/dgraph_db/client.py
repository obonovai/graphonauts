"""Dgraph async client implementation using the pydgraph v25 native async API.

Communicates via gRPC on port 9080. The ``pydgraph`` v25.2+ library provides
native async support through ``AsyncDgraphClientStub`` and ``AsyncDgraphClient``,
eliminating the need for ``asyncio.to_thread()`` wrappers.

In addition to the five protocol methods (``connect``, ``areconnect``, ``aexecute``,
``afetch``, ``aclose``), this client exposes helper methods for schema management
and JSON mutations that the loader uses during data ingestion.
"""

import json
from typing import Any

import pydgraph  # type: ignore[import-untyped]

# TPC-H benchmark queries (e.g., full order details join) can return responses
# exceeding 100 MB. The default gRPC limit of 4 MB is insufficient.
_GRPC_MAX_MSG = 1024 * 1024 * 1024  # 1 GB


class DgraphClient:
    """Async Dgraph client with connection management and query execution.

    Uses the native async gRPC client from ``pydgraph`` v25.2+. The stub and
    client constructors are lightweight (lazy gRPC channel), so ``connect()``
    remains synchronous per the ``BaseClient`` protocol.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {
            "host": "localhost:9080",
        }
        self.stub: Any = None
        self.client: Any = None

    def connect(self) -> None:
        """Initialise the async gRPC stub and Dgraph client.

        Creates an ``AsyncDgraphClientStub`` (gRPC channel) and an
        ``AsyncDgraphClient`` wrapping it. The gRPC channel is established
        lazily on the first actual call, so no network I/O occurs here.
        """
        self.stub = pydgraph.AsyncDgraphClientStub(
            self.config["host"],
            options=[
                ("grpc.max_send_message_length", _GRPC_MAX_MSG),
                ("grpc.max_receive_message_length", _GRPC_MAX_MSG),
            ],
        )
        self.client = pydgraph.AsyncDgraphClient(self.stub)

    async def areconnect(self) -> None:
        """Close the existing connection and establish a new one."""
        await self.aclose()
        self.connect()

    async def aexecute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a DQL query without returning a structured result set.

        Intended for fire-and-forget queries such as upsert blocks or mutation
        queries. Uses a read-write transaction that is committed after execution.

        Args:
            query: A DQL query string.
            params: Optional query variables (keys without the ``$`` prefix).

        Returns:
            None.
        """
        if not self.client:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")

        vars_dict = {f"${k}": str(v) for k, v in params.items()} if params else None
        txn = self.client.txn()
        try:
            await txn.query(query, variables=vars_dict)
            await txn.commit()
        finally:
            await txn.discard()

    async def afetch(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a DQL query and return all result records as dictionaries.

        Parses the JSON response from Dgraph and returns the first named query
        block's results. Variable (``var``) blocks are excluded from the response
        by Dgraph, so this method naturally returns the first data block.

        Args:
            query: A DQL query string.
            params: Optional query variables (keys without the ``$`` prefix).

        Returns:
            A list of dictionaries, one per result record.
        """
        if not self.client:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")

        vars_dict = {f"${k}": str(v) for k, v in params.items()} if params else None
        txn = self.client.txn(read_only=True)
        try:
            res = await txn.query(query, variables=vars_dict)
            data: dict[str, Any] = json.loads(res.json)
            for key in data:
                if isinstance(data[key], list):
                    return data[key]  # type: ignore[no-any-return]
            return []
        finally:
            await txn.discard()

    async def aclose(self) -> None:
        """Close the gRPC channel and release all associated resources."""
        if self.stub:
            await self.stub.close()
            self.stub = None
            self.client = None

    # --- Helper methods for the loader (not part of BaseClient protocol) ---

    async def aalter(self, schema: str) -> None:
        """Alter the Dgraph schema (set predicates and types).

        Args:
            schema: A Dgraph schema definition string containing predicate
                declarations and type definitions.
        """
        if not self.client:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")
        await self.client.alter(pydgraph.Operation(schema=schema))

    async def adrop_all(self) -> None:
        """Drop all data and schema from Dgraph.

        This is the most thorough cleanup operation, removing all predicates,
        types, and data. The schema must be re-applied after this operation
        before any new data can be ingested.
        """
        if not self.client:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")
        await self.client.alter(pydgraph.Operation(drop_all=True))

    async def amutate(self, data: list[dict[str, Any]]) -> dict[str, str]:
        """Execute a JSON mutation and return the blank-node UID mapping.

        Each dictionary in ``data`` represents a node or an update to an existing
        node. Blank node references (``uid`` values starting with ``_:``) are
        resolved by Dgraph and the assigned UIDs are returned in the response.

        Args:
            data: List of JSON-serializable dicts representing nodes/edges.

        Returns:
            A dictionary mapping blank node labels (without the ``_:`` prefix)
            to the UIDs assigned by Dgraph.
        """
        if not self.client:
            raise RuntimeError("Client is not connected. Call 'connect()' first.")

        txn = self.client.txn()
        try:
            response = await txn.mutate(set_obj=data, commit_now=True)
            return dict(response.uids)
        finally:
            await txn.discard()
