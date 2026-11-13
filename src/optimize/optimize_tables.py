"""Delta maintenance: file compaction, Z-ordering, statistics, vacuum.

Usage: optimize_tables.py <catalog> [vacuum_retention_hours=168]

Z-ORDER columns follow the dominant filter/join predicates (see docs/performance.md).
Only recent partitions of the large OBT are rewritten, which keeps the cost bounded.
"""
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
catalog = sys.argv[1]
retention = int(sys.argv[2]) if len(sys.argv) > 2 else 168

# table -> (ZORDER cols, optional partition predicate)
PLAN = {
    "gold.obt_retail_sales": (["store_id", "product_id", "customer_id"], "order_date >= current_date() - INTERVAL 14 DAYS"),
    "staging.inventory_snapshot": (["store_id", "product_id"], "ingest_date >= current_date() - INTERVAL 14 DAYS"),
    "staging.returns": (["order_id"], None),
}
for table, (zcols, where) in PLAN.items():
    fq = f"{catalog}.{table}"
    sql = f"OPTIMIZE {fq}" + (f" WHERE {where}" if where else "") + f" ZORDER BY ({', '.join(zcols)})"
    print(sql)
    spark.sql(sql)
    spark.sql(f"ANALYZE TABLE {fq} COMPUTE STATISTICS FOR ALL COLUMNS")
    spark.sql(f"VACUUM {fq} RETAIN {retention} HOURS")

# SDP-managed tables (silver / gold facts & dims) are maintained by predictive optimization - nothing to do.
