"""Protocol defining the database client interface via structural typing.

This module employs Python's ``typing.Protocol`` to define the contract that all
database client implementations must satisfy. The use of structural typing (as
opposed to nominal inheritance via abstract base classes) is a deliberate design
decision motivated by several considerations:

1. **Decoupling**: Concrete client implementations (e.g., ``Neo4jClient``,
   ``MemgraphClient``) are not required to import or inherit from a shared base
   class. This eliminates coupling between the framework core and individual
   database modules, enabling each module to be developed, tested, and packaged
   independently.

2. **Duck typing formalisation**: Python's duck typing philosophy allows any
   object with the right methods to be used interchangeably. ``Protocol``
   formalises this convention with static type checking support, allowing
   ``mypy`` to verify conformance at type-check time without runtime overhead.

3. **Extensibility**: Adding support for a new graph database requires only
   implementing a class with matching method signatures. No registration,
   inheritance chain modification, or framework awareness is needed.

The ``@runtime_checkable`` decorator additionally enables ``isinstance()``
checks at runtime, which is used by the CLI layer for defensive validation.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BaseClient(Protocol):
    """Database client interface for connecting to and executing queries against
    a graph database backend.

    This protocol specifies the minimal set of operations required by the
    benchmarking framework to interact with any supported graph database.
    Implementations must provide all five methods with compatible signatures.

    The interface distinguishes between synchronous connection initialisation
    and asynchronous I/O operations. The ``connect()`` method is intentionally
    synchronous because it performs only driver object construction (e.g.,
    allocating a Neo4j ``Driver`` instance), which involves no network I/O.
    All subsequent operations that perform actual network communication --
    query execution, result fetching, connection teardown -- are asynchronous,
    following the ``async/await`` convention with the ``a`` prefix.

    Design note:
        The separation of ``aexecute`` (fire-and-forget) and ``afetch``
        (result-returning) reflects a common pattern in database driver APIs
        where DDL statements and data modification queries do not return
        meaningful result sets, while read queries do. This distinction allows
        implementations to optimise each path independently.
    """

    def connect(self) -> None:
        """Initialise the database driver connection.

        This method performs synchronous driver construction only. No network
        I/O or authentication handshake occurs here; those are deferred to the
        first asynchronous operation. Implementations should store the driver
        instance as internal state for use by subsequent async methods.
        """
        ...

    async def areconnect(self) -> None:
        """Close the existing connection and establish a new one.

        This method is invoked between benchmark phases to ensure a clean
        connection state, preventing any session-level caching or connection
        pooling artefacts from influencing measurement results.
        """
        ...

    async def aexecute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a query without returning a structured result set.

        Intended for DDL operations (index creation and removal), data
        modification statements, and other side-effect-only queries.

        Args:
            query: A database-specific query string (e.g., Cypher for Neo4j).
            params: Optional dictionary of query parameters for parameterised
                execution, which prevents injection vulnerabilities and may
                enable query plan caching in the database engine.

        Returns:
            Implementation-defined metadata about the execution (e.g., result
            summary, counters). The return value is not used by the framework.
        """
        ...

    async def afetch(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a query and return all result records as dictionaries.

        This is the primary method used during benchmark execution. Each
        record in the result set is represented as a dictionary mapping
        column names (or return aliases) to their values.

        Args:
            query: A database-specific query string (e.g., Cypher for Neo4j).
            params: Optional dictionary of query parameters for parameterised
                execution.

        Returns:
            A list of dictionaries, where each dictionary represents one
            record from the query result set. An empty list is returned if
            the query produces no results.
        """
        ...

    async def aclose(self) -> None:
        """Close the database driver and release all associated resources.

        Implementations should ensure that all outstanding transactions are
        either committed or rolled back, and that connection pool resources
        are properly deallocated.
        """
        ...
