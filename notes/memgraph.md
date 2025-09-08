# Memgraph TPC-H Data Load Notes

## Data Load Times
The following are the load times for the TPC-H dataset using the `load.py` script:

| Table       | Load Time (seconds)     |
|-------------|-------------------------|
| Region      | 0.00                    |
| Nation      | 0.00                    |
| Supplier    | 0.30                    |
| Customer    | 4.19                    |
| Part        | 5.85                    |
| PartSupp    | 14.23                   |
| Orders      | 47.88                   |
| LineItem    | 477.45 (~8 minutes)     |
| **Total**   | **576.95 (~10 minutes)** |

All tables were successfully loaded into Memgraph in approximately **10 minutes**.

---

## Data Storage
The total storage used by the TPC-H dataset in Memgraph is approximately **15GB**. This was determined using the following commands:

```bash
docker exec graphonauts-memgraph du -h /var/lib/memgraph/databases /var/lib/memgraph/wal /var/lib/memgraph/snapshots
docker system df -v | grep memgraph
```

Output:
```
75M     /var/lib/memgraph/databases/.durability
4.0K    /var/lib/memgraph/databases/memgraph
75M     /var/lib/memgraph/databases
7.2G    /var/lib/memgraph/wal
7.0G    /var/lib/memgraph/snapshots

memgraph/memgraph                 latest             30092df18881   12 days ago    789MB     100.7MB       688.6MB       1
1c8ebdc90963   memgraph/memgraph:latest   "/usr/lib/memgraph/m…"   3               0B        18 minutes ago   Up 18 minutes   graphonauts-memgraph
memgraphdb_memgraph_data                                           1         15.15GB
```

Based on the Docker inspection, here's what TPC-H data is using in storage:
- Total Memgraph data: ~15GB
- Database files: 75MB
- WAL (Write-Ahead Log): 7.2GB
- Snapshots: 7.0GB
- Docker volume: 15.15GB

The large storage usage is primarily due to:
- WAL files (7.2GB) - These store transaction logs for durability
- Snapshots (7.0GB) - These are periodic database snapshots for recovery
- Database files (75MB) - The actual compressed data

This is normal for Memgraph as it maintains both WAL and snapshot files for data durability and recovery. The original raw TPC-H data files were much smaller, but the graph database format with relationships and indexing requires more storage space.

---

## Query Execution Times
The following are the execution times for various queries performed on the TPC-H dataset:

### A. Selection, Projection, Source (of Data)
| Query Description                                           | Execution Time (seconds) |
|-------------------------------------------------------------|---------------------------|
| Non-Indexed Columns: Select supplier named 'Supplier#000000666' | 1.39                      |
| Non-Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31 | 15.43                     |
| Indexed Columns: Select supplier with the ID 1337          | 0.01                      |
| Indexed Columns — Range Query: Select orders placed between 1990-01-01 and 1995-12-31    | 14.70                     |

### B. Aggregation
| Query Description                                           | Execution Time (seconds) |
|-------------------------------------------------------------|---------------------------|
| COUNT: Count the number of products per brand              | 0.38                      |
| MAX: Find the most expensive product per brand             | 0.26                      |

---

## Summary
- **Load Time:** 576.95 (~10 minutes)
- **Storage Used:** 75 MB (Database files) / 15.15 GB (Docker volume)
- **Query Performance:** Detailed execution times for various queries are provided above.

These notes provide a quick reference for the performance, storage metrics, and query execution times of the TPC-H dataset in Memgraph.
