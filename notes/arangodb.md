# ArangoDB TPC-H Data Load Notes

## Data Load Times
The following are the load times for the TPC-H dataset using the `load.py` script:

| Table       | Load Time (seconds)     |
|-------------|-------------------------|
| Region      | 0.00                    |
| Nation      | 0.00                    |
| Supplier    | 0.25                    |
| Customer    | 3.70                    |
| Part        | 2.48                    |
| PartSupp    | 38.10                   |
| Orders      | 72.28                   |
| LineItem    | 505.80 (~8 minutes)     |
| **Total**   | **673.70 (~9 minutes)** |

All tables were successfully loaded into ArangoDB in approximately **11 minutes**.

---

## Data Storage
The total storage used by the TPC-H dataset in ArangoDB is **3.5 GB**. This was determined using the following commands:

```bash
docker exec graphonauts-arangodb du -sh /var/lib/arangodb3
```

Output:
```
3.5G    /var/lib/arangodb3
```

---

## Query Execution Times
The following are the execution times for various queries performed on the TPC-H dataset:

### A. Selection, Projection, Source (of Data)
| Query Description                                           | Execution Time (seconds)  |
|-------------------------------------------------------------|---------------------------|
| Non-Indexed Columns: Select supplier named 'Supplier#000000666' | 0.01                  |
| Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31 | 2.60                     |
| Indexed Columns: Select supplier with the ID 1337          | 0.00                      |
| Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31    | 2.02                     |

### B. Aggregation
| Query Description                                           | Execution Time (seconds) |
|-------------------------------------------------------------|---------------------------|
| COUNT: Count the number of products per brand              | 0.10                      |
| MAX: Find the most expensive product per brand             | 0.03                      |

---

## Summary
- **Load Time:** 673.70 seconds (~11 minutes)
- **Storage Used:** 3.5 GB
- **Query Performance:** Detailed execution times for various queries are provided above.

These notes provide a quick reference for the performance, storage metrics, and query execution times of the TPC-H dataset in ArangoDB.
