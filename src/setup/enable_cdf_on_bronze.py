"""Enable Change Data Feed on Lakeflow Connect bronze tables so SDP can derive SCD 1/2 flows.
Usage: enable_cdf_on_bronze.py <catalog>"""
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
catalog = sys.argv[1]
for t in ["stores", "products", "customers", "orders", "order_items", "sf_contact", "sf_case"]:
    spark.sql(f"ALTER TABLE {catalog}.bronze.{t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print("CDF enabled:", t)
