# ![Graphonauts](img/ChatGPT%20Image%20Sep%206,%202025,%2011_20_57%20AM.png)

# Graphonauts – Exploring the Graph “Space”

Welcome to **Graphonauts**, a project dedicated to exploring the fascinating world of graph databases and their capabilities.

> **Note:** This project is part of my diploma thesis and is currently a work in progress.

## About the Project
Graph databases are powerful tools for modeling and analyzing complex relationships. This repository focuses on:

- Understanding the strengths and weaknesses of different graph databases.
- Running benchmarks and queries using the TPC-H dataset.
- Exploring use cases and scenarios where graph databases excel.

## Graph Databases Explored
This project includes experiments with the following graph databases:

- **ArangoDB**
- **Memgraph**
- **NebulaGraph**
- **Neo4j**

## Dataset
The [TPC-H dataset](https://www.tpc.org/tpch/) is a decision support benchmark that consists of a suite of business-oriented ad-hoc queries and concurrent data modifications. It is widely used for evaluating database performance.

### Graph Representation of TPC-H Dataset
Below is a visualization of how the TPC-H dataset is modeled in a graph database. Each entity is represented as a node, and relationships between entities are modeled as edges:

```mermaid
graph TB
    %% Node definitions with properties
    R[Region<br/>regionkey, name, comment]
    N[Nation<br/>nationkey, name, comment]
    S[Supplier<br/>suppkey, name, address,<br/>phone, acctbal, comment]
    C[Customer<br/>custkey, name, address,<br/>phone, acctbal, mktsegment, comment]
    P[Part<br/>partkey, name, mfgr, brand,<br/>type, size, container,<br/>retailprice, comment]
    O[Order<br/>orderkey, orderstatus, totalprice,<br/>orderdate, orderpriority, clerk,<br/>shippriority, comment]
    LI[LineItem<br/>orderkey, partkey, suppkey,<br/>linenumber, quantity, extendedprice,<br/>discount, tax, returnflag, linestatus,<br/>shipdate, commitdate, receiptdate,<br/>shipinstruct, shipmode, comment]

    %% Relationships
    N -->|BELONGS_TO| R
    S -->|LOCATED_IN| N
    C -->|LOCATED_IN| N
    C -->|PLACED| O
    S -->|SUPPLIES<br/>availqty, supplycost, comment| P
    O -->|CONTAINS| LI
    LI -->|OF_PART| P
    LI -->|SUPPLIED_BY| S

    %% Styling for better visibility
    classDef nodeClass fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000
    classDef relClass fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000

    class R,N,S,C,P,O,LI nodeClass
```

### Queries to Test
We will test the following types of queries on the graph database:

#### A. Selection, Projection, and Source (of Data)
1. **Non-Indexed Columns**: Select supplier named 'Supplier#000000666'.
2. **Non-Indexed Columns — Range Query**: Select orders placed between `1990-01-01` and `1995-12-31`.
3. **Indexed Columns**: Select supplier with the ID `1337`.
4. **Indexed Columns — Range Query**: Select orders placed between `1990-01-01` and `1995-12-31`.

#### B. Aggregation
1. **COUNT**: Count the number of products per brand.
2. **MAX**: Find the most expensive product per brand.

These queries will help evaluate the performance and capabilities of the graph database in handling both traditional and graph-specific operations.

---

## Repository Structure
- `src/arangodb/` - Code and experiments for ArangoDB.
- `src/memgraph/` - Code and experiments for Memgraph.
- `src/nebulagraph/` - Code and experiments for NebulaGraph.
- `src/neo4j/` - Code and experiments for Neo4j.

## License
This project is licensed under the [MIT License](LICENSE).

---

Happy graph exploring! 🚀
