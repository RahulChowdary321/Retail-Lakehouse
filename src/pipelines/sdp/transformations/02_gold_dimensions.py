"""Gold dimensions with SCD Type 1 and Type 2 via AUTO CDC flows.

* dim_customer : SCD2 - full history of city / state / loyalty_tier changes (__START_AT / __END_AT)
* dim_product  : SCD2 - full price & category history
* dim_store    : SCD1 - current state only
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("retail.catalog")  # noqa: F821
CDC_META = ["_change_type", "_commit_version", "_commit_timestamp", "is_deleted"]


def bronze_cdf(table: str):
    return (spark.readStream.option("readChangeFeed", "true").table(f"{CATALOG}.bronze.{table}")  # noqa: F821
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .withColumn("is_deleted", F.col("_change_type") == "delete"))


@dp.temporary_view(name="customer_changes")
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email LIKE '%@%'")
def customer_changes():
    return bronze_cdf("customers")


dp.create_streaming_table(name=f"{CATALOG}.gold.dim_customer", comment="Customer dimension - SCD Type 2")
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.dim_customer", source="customer_changes", keys=["customer_id"],
    sequence_by=F.col("updated_at"), apply_as_deletes=F.expr("is_deleted"),
    except_column_list=CDC_META, stored_as_scd_type=2,
    track_history_column_list=["city", "state", "loyalty_tier"])  # other column edits overwrite in place


@dp.temporary_view(name="product_changes")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect_or_drop("positive_price", "list_price > 0")
def product_changes():
    return bronze_cdf("products")


dp.create_streaming_table(name=f"{CATALOG}.gold.dim_product", comment="Product dimension - SCD Type 2")
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.dim_product", source="product_changes", keys=["product_id"],
    sequence_by=F.col("updated_at"), apply_as_deletes=F.expr("is_deleted"),
    except_column_list=CDC_META, stored_as_scd_type=2,
    track_history_column_list=["list_price", "unit_cost", "category", "brand"])


@dp.temporary_view(name="store_changes")
@dp.expect_or_drop("store_id_not_null", "store_id IS NOT NULL")
def store_changes():
    return bronze_cdf("stores")


dp.create_streaming_table(name=f"{CATALOG}.gold.dim_store", comment="Store dimension - SCD Type 1")
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.dim_store", source="store_changes", keys=["store_id"],
    sequence_by=F.col("updated_at"), apply_as_deletes=F.expr("is_deleted"),
    except_column_list=CDC_META, stored_as_scd_type=1)
