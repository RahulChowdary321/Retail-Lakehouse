"""Silver layer - Spark Declarative Pipelines (SDP).

Bronze tables (Lakeflow Connect) -> streaming tables with data-quality expectations.
Orders use SCD Type 1 (current state); order_items / cases are append-only streaming tables.
`spark` is provided by the pipeline runtime.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("retail.catalog")  # noqa: F821
CDC_META = ["_change_type", "_commit_version", "_commit_timestamp", "is_deleted"]


def bronze_cdf(table: str):
    """Row-level change stream from a bronze table (requires Change Data Feed)."""
    return (spark.readStream.option("readChangeFeed", "true").table(f"{CATALOG}.bronze.{table}")  # noqa: F821
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .withColumn("is_deleted", F.col("_change_type") == "delete"))


# ---- Orders: SCD1 -----------------------------------------------------------------------------------
@dp.temporary_view(name="orders_changes")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_status", "status IN ('PLACED','SHIPPED','DELIVERED','CANCELLED')")
def orders_changes():
    return bronze_cdf("orders")


dp.create_streaming_table(name=f"{CATALOG}.silver.orders", comment="Current state of every order (SCD1)")
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.orders", source="orders_changes", keys=["order_id"],
    sequence_by=F.col("updated_at"), apply_as_deletes=F.expr("is_deleted"),
    except_column_list=CDC_META, stored_as_scd_type=1)


# ---- Order items: append-only ------------------------------------------------------------------------
@dp.table(name=f"{CATALOG}.silver.order_items", comment="Validated order lines")
@dp.expect_or_drop("positive_qty", "quantity > 0")
@dp.expect_or_drop("positive_price", "unit_price >= 0")
@dp.expect("discount_le_price", "discount <= unit_price")
def order_items():
    return (spark.readStream.option("skipChangeCommits", "true")  # noqa: F821
            .table(f"{CATALOG}.bronze.order_items")
            .withColumn("line_amount", F.round(F.col("quantity") * (F.col("unit_price") - F.col("discount")), 2)))


# ---- Salesforce CRM ----------------------------------------------------------------------------------
@dp.temporary_view(name="contact_changes")
@dp.expect_or_drop("contact_id_not_null", "contact_id IS NOT NULL")
def contact_changes():
    return bronze_cdf("sf_contact")


dp.create_streaming_table(name=f"{CATALOG}.silver.crm_contact", comment="Salesforce contacts (SCD1)")
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.crm_contact", source="contact_changes", keys=["contact_id"],
    sequence_by=F.col("updated_at"), apply_as_deletes=F.expr("is_deleted"),
    except_column_list=CDC_META, stored_as_scd_type=1)


@dp.table(name=f"{CATALOG}.silver.crm_case", comment="Salesforce service cases")
@dp.expect_or_drop("case_id_not_null", "case_id IS NOT NULL")
def crm_case():
    return spark.readStream.option("skipChangeCommits", "true").table(f"{CATALOG}.bronze.sf_case")  # noqa: F821
