"""Batch load from ADLS Gen2 into the staging layer.

Reads daily files (inventory snapshots, returns) with an explicit schema (no inference cost, no drift),
adds audit columns, de-duplicates, and idempotently overwrites only the day's partition via replaceWhere.

Usage: load_adls_to_staging.py <catalog> <landing_root> <ingest_date>
  landing_root: abfss://landing@<account>.dfs.core.windows.net/retail   (or a /Volumes/... path)
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMAS = {
    "inventory_snapshot": StructType([
        StructField("store_id", StringType()), StructField("product_id", StringType()),
        StructField("snapshot_date", TimestampType()), StructField("on_hand", LongType()),
        StructField("reorder_point", LongType())]),
    "returns": StructType([
        StructField("return_id", StringType()), StructField("order_id", StringType()),
        StructField("product_id", StringType()), StructField("quantity", LongType()),
        StructField("return_date", TimestampType()), StructField("reason", StringType()),
        StructField("refund_amount", DoubleType())]),
}
DEDUPE_KEYS = {"inventory_snapshot": ["store_id", "product_id", "snapshot_date"], "returns": ["return_id"]}

spark = SparkSession.builder.getOrCreate()
catalog, root, ingest_date = sys.argv[1], sys.argv[2].rstrip("/"), sys.argv[3]

for table, schema in SCHEMAS.items():
    path = f"{root}/adls/{table}/ingest_date={ingest_date}"
    df = (spark.read.schema(schema).parquet(path)
          .dropDuplicates(DEDUPE_KEYS[table])
          .withColumn("ingest_date", F.lit(ingest_date).cast(DateType()))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    target = f"{catalog}.staging.{table}"
    writer = df.write.format("delta").mode("overwrite").partitionBy("ingest_date")
    if spark.catalog.tableExists(target):
        writer = writer.option("replaceWhere", f"ingest_date = '{ingest_date}'")
    writer.saveAsTable(target)
    print(f"{target}: loaded partition {ingest_date}")
