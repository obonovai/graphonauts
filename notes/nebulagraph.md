# NebulaGraph TPC-H Data Load Notes

## Data Load Times
The following are the load times for the TPC-H dataset using the `load.py` script:

| Table       | Load Time (seconds)      |
|-------------|--------------------------|
| Region      | 30.09 (0.09)             |
| Nation      | 60.07 (0.07)             |
| Supplier    | 60.27 (0.27)             |
| Customer    | 63.18 (3.18)             |
| Part        | 33.33 (3.33)             |
| PartSupp    | 39.91 (9.91)             |
| Orders      | 90.54 (30.54)            |
| LineItem    | 348.26 (~6 minutes) (228.26 (~4 minutes))  |
| **Total**   | **764.21 (~13 minutes)** |

All tables were successfully loaded into NebulaGraph in approximately **13 minutes**.

> Note: There is a consistent load penalty of 30 seconds for every `_load_batch_vertices` and `_load_batch_edges` transaction due to switching to a `tpch` space. While 30 seconds may seem excessive, this threshold was determined iteratively during the process of loading the TPC-H dataset. Since NebulaGraph is designed for massive-scale data, loading all tables in a single session might mitigate connection overhead and significantly reduce the load time.
