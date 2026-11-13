"""DEV ONLY: stand-in for Lakeflow Connect.

Loads the generator's `sqlserver/` and `salesforce/` parquet files from the landing volume into
`<catalog>.bronze.*` Delta tables (MERGE on key, latest `updated_at` wins) with Change Data Feed enabled,
so the downstream SDP pipeline behaves exactly as it does against real Lakeflow Connect tables.

Usage: simulate_bronze.py <catalog> <ingest_date> [landing_root]
"""
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

KEYS = {
    "sqlserver": {"stores": "store_id", "products": "product_id", "customers": "customer_id",
                  "orders": "order_id", "order_items": "order_item_id"},
    "salesforce": {"sf_contact": "contact_id", "sf_case": "case_id"},
}

spark = SparkSession.builder.getOrCreate()
catalog, ingest_date = sys.argv[1], sys.argv[2]
root = sys.argv[3] if len(sys.argv) > 3 else f"/Volumes/{catalog}/landing/raw"

for source, tables in KEYS.items():
    for table, key in tables.items():
        path = f"{root}/{source}/{table}/ingest_date={ingest_date}"
        try:
            df = spark.read.parquet(path)
        except Exception as e:  # table not produced on this date (e.g. static dims in incremental runs)
            print(f"skip {source}.{table}: {e.__class__.__name__}")
            continue
        target = f"{catalog}.bronze.{table}"
        if not spark.catalog.tableExists(target):
            df.write.format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(target)
        else:
            (DeltaTable.forName(spark, target).alias("t")
             .merge(df.alias("s"), f"t.{key} = s.{key}")
             .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")
             .whenNotMatchedInsertAll().execute())
        print(f"merged {df.count():,} rows into {target}")
