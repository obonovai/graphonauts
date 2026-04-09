# Dgraph Memory Exhaustion on Complex Multi-Hop Join Queries

## 1. Problem Statement

During benchmark execution of query **join 3** ("Complex Join 1: Retrieve all order details") against
Dgraph v24.0.2, the Alpha server process terminated with a gRPC `UNAVAILABLE` error indicating an
abrupt socket closure:

```
grpc.aio._call.AioRpcError: <AioRpcError of RPC that terminated with:
    status = StatusCode.UNAVAILABLE
    details = "Socket closed"
>
```

Docker Desktop resource monitoring showed memory consumption of the `dgraph-alpha-graphonaut` container
exceeding 15 GB before the process was terminated by the kernel OOM killer, even with a configured Docker
memory limit of 32 GB. Subsequent attempts with increased memory allocations exhibited the same behaviour,
with consumption growing unboundedly until the process was killed.

## 2. Query Structure and Data Cardinality

The query performs a six-hop traversal across the full TPC-H Scale Factor 1 graph, starting from all
Customer nodes and expanding through the entire order-lineitem-part-supplier chain:

```dql
{
    result(func: type(Customer)) {
        custkey
        customer_name: name
        located_in { customer_nation: name }
        placed {
            orderkey, orderdate, totalprice
            contains {
                linenumber, quantity, extendedprice
                of_part { partkey, part_name: name, brand }
                supplied_by { suppkey, supplier_name: name }
            }
        }
    }
}
```

The equivalent Neo4j Cypher query:

```cypher
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(li:LineItem)
MATCH (li)-[:OF_PART]->(p:Part)
MATCH (li)-[:SUPPLIED_BY]->(s:Supplier)
MATCH (c)-[:LOCATED_IN]->(n:Nation)
RETURN c.custkey, c.name, n.name, o.orderkey, o.orderdate, o.totalprice,
       li.linenumber, li.quantity, li.extendedprice,
       p.partkey, p.name, p.brand, s.suppkey, s.name
```

The TPC-H SF1 cardinalities involved are:

| Entity | Count |
|--------|-------|
| Customer | 150,000 |
| Order | 1,500,000 |
| LineItem | 6,001,215 |
| Part | 200,000 |
| Supplier | 10,000 |
| Nation | 25 |

The full cross-product yields approximately 6 million result rows (one per LineItem, each joined with its
Order, Customer, Part, Supplier, and Nation).

## 3. Root Cause: Nested JSON Response Format

The fundamental issue lies in how Dgraph's DQL returns query results compared to Cypher-based databases.

**Cypher (Neo4j, Memgraph)** returns results as a **flat tabular stream**. Each of the ~6 million
result rows is an independent record containing denormalised scalar values. The server serialises rows
incrementally, and the total response payload is proportional to `row_count * columns * avg_value_size`.
For this query, the serialised result is on the order of hundreds of megabytes.

**DQL (Dgraph)** returns results as a **deeply nested JSON tree**. The root array contains 150,000
Customer objects, each embedding an array of Order objects, each embedding an array of LineItem objects,
each embedding Part and Supplier objects. This tree representation introduces substantial structural
overhead:

1. **Repeated object wrappers.** Every intermediate node (Customer, Order, LineItem) carries its own JSON
   object boundary (`{}`), array boundary (`[]`), and key names at every nesting level.

2. **Denormalisation via duplication.** When multiple LineItems reference the same Part or Supplier, the
   nested format duplicates the full Part/Supplier object within each LineItem's subtree, whereas a flat
   tabular format shares column values across rows.

3. **In-memory tree construction.** Dgraph Alpha must construct the entire nested response tree in memory
   before serialising it to the gRPC response. Unlike streaming tabular results, the tree cannot be
   emitted incrementally because child arrays must be fully populated before the parent object can be
   closed in JSON.

The estimated memory footprint for the full nested tree is on the order of tens of gigabytes, far exceeding
practical container memory limits.

## 4. Comparison with Other Databases

| Database | Response Format | Join 3 Behaviour | Peak Memory |
|----------|----------------|-------------------|-------------|
| Neo4j | Flat tabular (Bolt protocol) | Completes successfully | ~4 GB |
| Memgraph | Flat tabular (Bolt protocol) | Completes successfully | ~6 GB |
| ArangoDB | Flat tabular (HTTP JSON array) | Completes successfully | ~3 GB |
| Dgraph | Nested JSON tree (gRPC) | OOM at >15 GB | >32 GB (unbounded) |

This discrepancy is not a deficiency in Dgraph's query execution engine but rather a consequence of its
response serialisation model. DQL's nested format is well-suited for graph-shaped results with moderate
fan-out (e.g., social networks, hierarchical data) but becomes problematic for relational-style
cross-product joins with high cardinality at multiple nesting levels.

## 5. Mitigation: Result Set Pagination

To enable benchmarking of the join traversal pattern, the query was modified to limit the number of root
Customer nodes processed using DQL's `first:` pagination parameter:

```dql
{
    result(func: type(Customer), first: 10000) {
        ...  // same traversal structure
    }
}
```

This reduces the root cardinality from 150,000 to 10,000 customers, which proportionally reduces the
nested tree size to approximately 1/15th of the original. The traversal pattern, join depth, and
per-node expansion behaviour remain identical, making the measurement representative of Dgraph's
join performance characteristics while avoiding the serialisation bottleneck.

The query description in the benchmark framework was annotated to reflect this limitation:

```
"Complex Join 1: Retrieve all order details
 (limited to 10000 customers — DQL nested response format OOMs on full dataset)"
```

## 6. Implications for Benchmark Interpretation

When comparing join 3 results across databases, the following considerations apply:

1. **Dgraph's execution time** reflects processing 10,000 customers (~6.7% of the full dataset). Direct
   time comparisons with other databases (which process the full 150,000 customers) are not meaningful
   without normalisation.

2. **Dgraph's memory measurement** reflects the memory consumed for the partial result set. The memory
   benchmark remains valid for characterising Dgraph's per-unit memory efficiency but does not represent
   the total memory required for the full query.

## 7. Future Work

The current pagination-based mitigation is a pragmatic workaround that enables benchmarking but does not
fully resolve the underlying challenge. Several avenues merit further investigation:

1. **Server-side pagination and cursor-based streaming.** Dgraph's gRPC API may support or could be
   extended with cursor-based result streaming, allowing the client to consume large result sets
   incrementally without requiring the server to materialise the full response tree in memory. The
   feasibility of this approach within the current pydgraph client and Dgraph Alpha architecture
   requires further exploration.

2. **Query decomposition.** The multi-hop join could be decomposed into multiple sequential queries
   (e.g., first retrieving Customer-Order pairs, then enriching with LineItem details in batches),
   trading latency for reduced peak memory. This approach would more closely mirror how DQL is
   idiomatically used in production Dgraph deployments.

3. **Alternative response formats.** Dgraph supports RDF (N-Quad) response format in addition to JSON.
   The RDF format produces a flat triple stream rather than a nested tree, which may avoid the
   memory amplification observed with JSON serialisation. Whether the benchmark framework can be
   adapted to consume RDF responses is an open question.

4. **Dgraph configuration tuning.** Parameters such as `--query-edge` (maximum edges traversed per
   query), `--limit` flags, and Alpha memory allocation settings may offer partial improvements. A
   systematic exploration of these parameters was not undertaken within the scope of this work.

The result set limitation should be revisited as the framework evolves, with the goal of achieving
full-dataset execution parity across all benchmarked databases where technically feasible.
