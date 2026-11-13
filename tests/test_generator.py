from datetime import date

import pandas as pd
from generate_retail_data import run


def test_initial_and_incremental(tmp_path):
    kw = dict(customers_n=200, products_n=40, stores_n=5, orders_per_day=50, history_days=2)
    run("initial", date(2026, 1, 10), tmp_path, **kw)
    items = pd.read_parquet(tmp_path / "sqlserver/order_items/ingest_date=2026-01-10")
    orders = pd.read_parquet(tmp_path / "sqlserver/orders/ingest_date=2026-01-10")
    assert len(orders) == 100 and items["order_id"].isin(orders["order_id"]).all()
    assert items["order_item_id"].is_unique and (items["quantity"] > 0).all()

    run("incremental", date(2026, 1, 11), tmp_path, **kw)
    cust = pd.read_parquet(tmp_path / "sqlserver/customers/ingest_date=2026-01-11")
    assert 0 < len(cust) < 200 and (cust["updated_at"] > pd.Timestamp("2026-01-11")).all()
    inc_orders = pd.read_parquet(tmp_path / "sqlserver/orders/ingest_date=2026-01-11")
    assert (inc_orders["status"] == "SHIPPED").any()   # status updates to yesterday's orders
    assert inc_orders["order_id"].is_unique
