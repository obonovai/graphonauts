# ArangoDB Storage Analysis Summary

## Overall Storage Usage
- **Total Docker container storage**: 3.5GB
- **Estimated data size (calculated)**: ~3.36GB
- **Total documents**: 29,924,916

## Storage Breakdown by Collection

### Main Data Collections (Documents)
| Collection | Documents | Est. Size (MB) | Percentage |
|------------|-----------|----------------|------------|
| lineitem   | 6,001,215 | 2,586.89      | 77.0%      |
| orders     | 1,500,000 | 400.54        | 11.9%      |
| partsupp   | 800,000   | 267.79        | 8.0%       |
| part       | 200,000   | 58.17         | 1.7%       |
| customer   | 150,000   | 42.49         | 1.3%       |
| supplier   | 10,000    | 2.42          | 0.1%       |
| nation     | 25        | 0.00          | 0.0%       |
| region     | 5         | 0.00          | 0.0%       |

### Edge Collections (Relationships)
| Collection | Documents | Purpose |
|------------|-----------|---------|
| lineitem_part | 6,001,215 | Links line items to parts |
| lineitem_supplier | 6,001,215 | Links line items to suppliers |
| order_lineitems | 6,001,215 | Links orders to line items |
| customer_orders | 1,500,000 | Links customers to orders |
| partsupp_part | 800,000 | Links part suppliers to parts |
| partsupp_supplier | 800,000 | Links part suppliers to suppliers |
| customer_nation | 150,000 | Links customers to nations |
| nation_region | 25 | Links nations to regions |
| supplier_nation | 10,000 | Links suppliers to nations |

## Key Insights
1. **Lineitem dominates storage**: The `lineitem` collection accounts for ~77% of all data
2. **Graph overhead**: Edge collections add significant storage (18M+ edge documents)
3. **Storage efficiency**: Average document size ranges from 162 bytes (nation) to 452 bytes (lineitem)
4. **Indices**: Custom indices created for queries:
   - `supplier_s_suppkey_query_idx` on supplier.s_suppkey
   - `orders_orderkey_orderdate_query_idx` on orders.o_orderkey and o_orderdate

## Storage Optimization Recommendations
1. Consider compression for large collections like `lineitem`
2. Monitor index size vs. query performance trade-offs
3. Archive old data if time-based queries are common
4. Consider sharding for the largest collections in production

## Comparison Context
This TPC-H dataset represents a typical OLAP workload with:
- Heavy fact table (lineitem) storage
- Multiple relationship mappings
- Mixed query patterns (point lookups, range queries, aggregations)
