# Neo4j TPC-H Data Load Notes

## Data Load Times
The following are the load times for the TPC-H dataset using the `load.py` script:

| Table       | Load Time (seconds)     |
|-------------|-------------------------|
| Region      | 0.02                    |
| Nation      | 0.04                    |
| Supplier    | 0.48                    |
| Customer    | 5.09                    |
| Part        | 5.64                    |
| PartSupp    | 21.69                   |
| Orders      | 59.90                   |
| LineItem    | 413.38 (~7 minutes)     |
| **Total**   | **533.56 (~9 minutes)** |

All tables were successfully loaded into Neo4j in approximately **9 minutes**.

---

## Data Storage
The total storage used by the TPC-H dataset in Neo4j is **4.2 GB**. This was determined using the following commands:

```bash
docker exec -it graphonauts-neo4j bash
cd /data/databases
du -sh neo4j
```

Output:
```
4.2G    neo4j
```

---

## Query Execution Times
The following are the execution times for various queries performed on the TPC-H dataset:

### A. Selection, Projection, Source (of Data)
| Query Description                                           | Execution Time (seconds) |
|-------------------------------------------------------------|---------------------------|
| Non-Indexed Columns: Select supplier named 'Supplier#000000666' | 0.45                      |
| Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31 | 16.05                     |
| Indexed Columns: Select supplier with the ID 1337          | 0.07                      |
| Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31    | 15.93                     |

### B. Aggregation
| Query Description                                           | Execution Time (seconds) |
|-------------------------------------------------------------|---------------------------|
| COUNT: Count the number of products per brand              | 0.11                      |
| MAX: Find the most expensive product per brand             | 0.08                      |

---

## Summary
- **Load Time:** 533.56 seconds (~9 minutes)
- **Storage Used:** 4.2 GB
- **Query Performance:** Detailed execution times for various queries are provided above.

These notes provide a quick reference for the performance, storage metrics, and query execution times of the TPC-H dataset in Neo4j.
