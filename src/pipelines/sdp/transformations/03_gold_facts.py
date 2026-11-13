"""Gold facts (materialized views). Point-in-time joins to the SCD2 dimensions give as-was attributes."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("retail.catalog")  # noqa: F821


def _asof(left, dim, key):
    """Join `left` (must have `key` and `order_ts`) to the SCD2 row of `dim` valid at order_ts."""
    d = dim.alias("d")
    cond = ((left[key] == d[key]) & (left["order_ts"] >= F.col("d.__START_AT"))
            & (F.col("d.__END_AT").isNull() | (left["order_ts"] < F.col("d.__END_AT"))))
    return left.join(d, cond, "left").drop(d[key]).drop("__START_AT", "__END_AT")


@dp.materialized_view(name=f"{CATALOG}.gold.fact_sales", comment="Order-line grain sales fact (as-was dimensions)")
def fact_sales():
    o = spark.read.table(f"{CATALOG}.silver.orders").alias("o")  # noqa: F821
    i = spark.read.table(f"{CATALOG}.silver.order_items").alias("i")  # noqa: F821
    base = (i.join(o, F.col("i.order_id") == F.col("o.order_id"))
            .select("i.order_item_id", "o.order_id", "o.customer_id", "o.store_id", "i.product_id",
                    "o.channel", "o.status", "o.order_ts", F.to_date("o.order_ts").alias("order_date"),
                    "i.quantity", "i.unit_price", "i.discount", "i.line_amount"))
    cust = spark.read.table(f"{CATALOG}.gold.dim_customer").select(  # noqa: F821
        "customer_id", F.col("city").alias("customer_city"), F.col("state").alias("customer_state"),
        F.col("loyalty_tier").alias("loyalty_tier_at_purchase"), "__START_AT", "__END_AT")
    prod = spark.read.table(f"{CATALOG}.gold.dim_product").select(  # noqa: F821
        "product_id", "product_name", "category", "brand", F.col("unit_cost").alias("unit_cost_at_sale"),
        "__START_AT", "__END_AT")
    out = _asof(_asof(base, cust, "customer_id"), prod, "product_id")
    return out.withColumn(
        "gross_margin", F.round(F.col("line_amount") - F.col("quantity") * F.col("unit_cost_at_sale"), 2))


@dp.materialized_view(name=f"{CATALOG}.gold.customer_service_summary", comment="Total/open cases per CRM contact")
def customer_service_summary():
    cases = spark.read.table(f"{CATALOG}.silver.crm_case")  # noqa: F821
    contacts = spark.read.table(f"{CATALOG}.silver.crm_contact")  # noqa: F821
    agg = cases.groupBy("contact_id").agg(
        F.count("*").alias("total_cases"),
        F.sum(F.when(F.col("status") != "Closed", 1).otherwise(0)).alias("open_cases"))
    return contacts.join(agg, "contact_id", "left").select(
        "email", "loyalty_member", "marketing_opt_in", "nps_score",
        F.coalesce("total_cases", F.lit(0)).alias("total_cases"),
        F.coalesce("open_cases", F.lit(0)).alias("open_cases"))
