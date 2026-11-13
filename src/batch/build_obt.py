"""Build the One Big Table (OBT): gold.obt_retail_sales.

Denormalises fact_sales + current store attributes + CRM signals + same-day inventory + returns
into one wide, partitioned Delta table for BI and Genie. Incremental: only the trailing
`lookback_days` partitions are rewritten (replaceWhere), which is idempotent and cheap.

Usage: build_obt.py <catalog> [lookback_days=3]
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
catalog = sys.argv[1]
lookback = int(sys.argv[2]) if len(sys.argv) > 2 else 3
cutoff = f"current_date() - INTERVAL {lookback} DAYS"

fact = spark.table(f"{catalog}.gold.fact_sales").filter(f"order_date >= {cutoff}")
store = spark.table(f"{catalog}.gold.dim_store").select(
    "store_id", "store_name", F.col("city").alias("store_city"), F.col("state").alias("store_state"),
    "region", "store_type")
cust = spark.table(f"{catalog}.gold.dim_customer").filter("__END_AT IS NULL").select("customer_id", "email")
crm = spark.table(f"{catalog}.gold.customer_service_summary")
inv = (spark.table(f"{catalog}.staging.inventory_snapshot")
       .select("store_id", "product_id", F.to_date("snapshot_date").alias("order_date"),
               F.col("on_hand").alias("inventory_on_hand")))
ret = (spark.table(f"{catalog}.staging.returns").groupBy("order_id", "product_id")
       .agg(F.sum("quantity").alias("returned_qty"), F.sum("refund_amount").alias("refund_amount")))

obt = (fact.join(F.broadcast(store), "store_id", "left")
       .join(cust, "customer_id", "left").join(crm, "email", "left")
       .join(inv, ["store_id", "product_id", "order_date"], "left")
       .join(ret, ["order_id", "product_id"], "left")
       .drop("email")
       .fillna({"returned_qty": 0, "refund_amount": 0.0, "total_cases": 0, "open_cases": 0})
       .withColumn("net_revenue", F.round(F.col("line_amount") - F.col("refund_amount"), 2))
       .withColumn("_obt_built_at", F.current_timestamp()))

target = f"{catalog}.gold.obt_retail_sales"
w = obt.write.format("delta").mode("overwrite").partitionBy("order_date")
if spark.catalog.tableExists(target):
    w = w.option("replaceWhere", f"order_date >= {cutoff}")
w.saveAsTable(target)
spark.sql(f"COMMENT ON TABLE {target} IS 'One Big Table: order-line grain retail sales with store, customer, CRM, inventory and returns attributes'")
print(f"OBT refreshed for last {lookback} days -> {target}")
