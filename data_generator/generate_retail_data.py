"""Synthetic retail data generator.

Emulates the three upstream feeds from the project:
  * sqlserver/   -> operational OLTP tables (what Lakeflow Connect would ingest from SQL Server)
  * salesforce/  -> CRM objects (what Lakeflow Connect would ingest from Salesforce)
  * adls/        -> daily batch files landed in ADLS Gen2 (inventory snapshots, returns)

Output layout:  <out>/<source>/<table>/ingest_date=YYYY-MM-DD/part-0.parquet

Modes
  initial      full dimension load + `--history-days` of orders
  incremental  only changed dimension rows (CDC-like), new orders for --date,
               status updates for the previous day's orders, new snapshots
Scale up with --orders-per-day (about 1.5M orders/day with ~3 items each is in the 15-20 GB/day range
once wide columns are added; the defaults are laptop-friendly).
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
CITIES = [("Seattle", "WA", "West"), ("Portland", "OR", "West"), ("San Jose", "CA", "West"),
          ("Austin", "TX", "South"), ("Atlanta", "GA", "South"), ("Miami", "FL", "South"),
          ("Chicago", "IL", "Midwest"), ("Columbus", "OH", "Midwest"), ("Boston", "MA", "Northeast"),
          ("New York", "NY", "Northeast")]
CATEGORIES = {"Apparel": ["Northwind", "Contoso", "Fabrikam"], "Footwear": ["StrideCo", "Fabrikam"],
              "Electronics": ["Voltix", "Contoso"], "Home": ["Hearth", "Northwind"],
              "Beauty": ["Lumen", "Aurea"], "Grocery": ["FreshPick", "Hearth"]}
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
CHANNELS = ["store", "online", "mobile"]
ORDER_STATUS = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


def _ts(d: date, hour: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour)


def gen_stores(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 1)
    idx = rng.integers(0, len(CITIES), n)
    base = datetime(2023, 1, 1)
    return pd.DataFrame({
        "store_id": [f"S{i:03d}" for i in range(1, n + 1)],
        "store_name": [f"{CITIES[j][0]} Store {i}" for i, j in enumerate(idx, 1)],
        "city": [CITIES[j][0] for j in idx], "state": [CITIES[j][1] for j in idx],
        "region": [CITIES[j][2] for j in idx],
        "store_type": rng.choice(["Flagship", "Mall", "Outlet", "Express"], n),
        "updated_at": base,
    })


def gen_products(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 2)
    cats = rng.choice(list(CATEGORIES), n)
    cost = np.round(rng.uniform(2, 120, n), 2)
    return pd.DataFrame({
        "product_id": [f"P{i:05d}" for i in range(1, n + 1)],
        "sku": [f"SKU-{i:07d}" for i in range(1, n + 1)],
        "product_name": [f"{c} Item {i}" for i, c in enumerate(cats, 1)],
        "category": cats,
        "brand": [rng.choice(CATEGORIES[c]) for c in cats],
        "unit_cost": cost,
        "list_price": np.round(cost * rng.uniform(1.3, 2.4, n), 2),
        "updated_at": datetime(2023, 1, 1),
    })


def gen_customers(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 3)
    idx = rng.integers(0, len(CITIES), n)
    created = pd.to_datetime("2023-01-01") + pd.to_timedelta(rng.integers(0, 365, n), unit="D")
    return pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(1, n + 1)],
        "first_name": rng.choice(["Ava", "Liam", "Mia", "Noah", "Zoe", "Ethan", "Ivy", "Omar", "Priya", "Chen"], n),
        "last_name": rng.choice(["Smith", "Garcia", "Patel", "Nguyen", "Brown", "Kim", "Lopez", "Khan"], n),
        "email": [f"customer{i}@example.com" for i in range(1, n + 1)],
        "city": [CITIES[j][0] for j in idx], "state": [CITIES[j][1] for j in idx],
        "loyalty_tier": rng.choice(TIERS, n, p=[0.5, 0.3, 0.15, 0.05]),
        "created_at": created, "updated_at": created,
    })


def gen_sf_contacts(customers: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 4)
    sub = customers.sample(frac=0.7, random_state=SEED).reset_index(drop=True)
    n = len(sub)
    return pd.DataFrame({
        "contact_id": [f"003{i:012d}" for i in range(n)], "email": sub["email"],
        "loyalty_member": rng.random(n) < 0.6, "marketing_opt_in": rng.random(n) < 0.45,
        "nps_score": rng.integers(0, 11, n), "updated_at": datetime(2024, 1, 1),
    })


def gen_orders(day: date, n_orders: int, customers: pd.DataFrame, products: pd.DataFrame, stores: pd.DataFrame):
    """Deterministic per day, so incremental runs can re-derive yesterday's orders."""
    rng = np.random.default_rng(SEED + day.toordinal())
    tag = f"{day:%Y%m%d}"
    order_ids = np.array([f"O{tag}{i:07d}" for i in range(n_orders)])
    secs = rng.integers(8 * 3600, 22 * 3600, n_orders)
    order_ts = pd.to_datetime(_ts(day)) + pd.to_timedelta(secs, unit="s")
    orders = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": rng.choice(customers["customer_id"].to_numpy(), n_orders),
        "store_id": rng.choice(stores["store_id"].to_numpy(), n_orders),
        "channel": rng.choice(CHANNELS, n_orders, p=[0.55, 0.3, 0.15]),
        "order_ts": order_ts,
        "status": rng.choice(ORDER_STATUS, n_orders, p=[0.4, 0.3, 0.27, 0.03]),
        "updated_at": order_ts,
    })
    n_items = rng.integers(1, 6, n_orders)
    rep = np.repeat(np.arange(n_orders), n_items)
    seq = np.concatenate([np.arange(k) for k in n_items])
    pid = rng.integers(0, len(products), len(rep))
    price = products["list_price"].to_numpy()[pid]
    items = pd.DataFrame({
        "order_item_id": [f"{order_ids[r]}-{s}" for r, s in zip(rep, seq)],
        "order_id": order_ids[rep], "product_id": products["product_id"].to_numpy()[pid],
        "quantity": rng.integers(1, 4, len(rep)), "unit_price": price,
        "discount": np.round(price * rng.choice([0, 0, 0, 0.1, 0.2], len(rep)), 2),
        "updated_at": order_ts[rep],
    })
    return orders, items


def gen_sf_cases(contacts: pd.DataFrame, day: date, n: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 5 + day.toordinal())
    ts = pd.to_datetime(_ts(day)) + pd.to_timedelta(rng.integers(0, 86400, n), unit="s")
    return pd.DataFrame({
        "case_id": [f"500{day:%y%m%d}{i:06d}" for i in range(n)],
        "contact_id": rng.choice(contacts["contact_id"].to_numpy(), n),
        "status": rng.choice(["New", "Working", "Closed"], n, p=[0.3, 0.2, 0.5]),
        "priority": rng.choice(["Low", "Medium", "High"], n, p=[0.5, 0.35, 0.15]),
        "reason": rng.choice(["Late delivery", "Damaged item", "Billing", "Product question"], n),
        "updated_at": ts,
    })


def gen_inventory(day: date, products: pd.DataFrame, stores: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 6 + day.toordinal())
    prods = products.sample(n=min(len(products), 200), random_state=SEED)
    df = stores[["store_id"]].merge(prods[["product_id"]], how="cross")
    df["snapshot_date"] = pd.to_datetime(day)
    df["on_hand"] = rng.integers(0, 250, len(df))
    df["reorder_point"] = 25
    return df


def gen_returns(day: date, orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 7 + day.toordinal())
    pick = items.sample(frac=0.03, random_state=int(rng.integers(0, 10_000)))
    return pd.DataFrame({
        "return_id": [f"R{day:%Y%m%d}{i:06d}" for i in range(len(pick))],
        "order_id": pick["order_id"].to_numpy(), "product_id": pick["product_id"].to_numpy(),
        "quantity": 1, "return_date": pd.to_datetime(day),
        "reason": rng.choice(["Defective", "Wrong size", "Changed mind", "Late"], len(pick)),
        "refund_amount": np.round(pick["unit_price"].to_numpy() - pick["discount"].to_numpy(), 2),
    })


def write(df: pd.DataFrame, out: Path, source: str, table: str, day: date) -> None:
    p = out / source / table / f"ingest_date={day.isoformat()}"
    p.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p / "part-0.parquet", index=False)
    print(f"  {source}/{table:<20} {len(df):>9,} rows -> {p}")


def run(mode: str, day: date, out: Path, customers_n: int, products_n: int, stores_n: int,
        orders_per_day: int, history_days: int) -> None:
    stores, products, customers = gen_stores(stores_n), gen_products(products_n), gen_customers(customers_n)
    contacts = gen_sf_contacts(customers)
    print(f"mode={mode} date={day}")

    if mode == "initial":
        for t, df in {"stores": stores, "products": products, "customers": customers}.items():
            write(df, out, "sqlserver", t, day)
        write(contacts, out, "salesforce", "sf_contact", day)
        days = [day - timedelta(days=i) for i in range(history_days - 1, -1, -1)]
    else:
        rng = np.random.default_rng(SEED + day.toordinal())
        ts = _ts(day, 6)
        c = customers.sample(frac=0.05, random_state=int(rng.integers(0, 10_000))).copy()
        c["loyalty_tier"] = rng.choice(TIERS, len(c))
        new_city = rng.integers(0, len(CITIES), len(c))
        c["city"], c["state"] = [CITIES[i][0] for i in new_city], [CITIES[i][1] for i in new_city]
        c["updated_at"] = ts
        write(c, out, "sqlserver", "customers", day)
        p = products.sample(frac=0.05, random_state=int(rng.integers(0, 10_000))).copy()
        p["list_price"] = np.round(p["list_price"] * rng.uniform(0.9, 1.15, len(p)), 2)
        p["updated_at"] = ts
        write(p, out, "sqlserver", "products", day)
        days = [day]
        prev_orders, _ = gen_orders(day - timedelta(days=1), orders_per_day, customers, products, stores)
        upd = prev_orders[prev_orders["status"] == "PLACED"].copy()
        upd["status"], upd["updated_at"] = "SHIPPED", ts
        write(upd, out, "sqlserver", "orders", day)  # status updates; new orders appended below

    all_orders, all_items, all_returns = [], [], []
    for d in days:
        o, i = gen_orders(d, orders_per_day, customers, products, stores)
        all_orders.append(o)
        all_items.append(i)
        all_returns.append(gen_returns(d, o, i))
    orders, items = pd.concat(all_orders), pd.concat(all_items)
    if mode == "initial":
        write(orders, out, "sqlserver", "orders", day)
    else:  # merge status updates + new orders into one file for the day
        existing = pd.read_parquet(out / "sqlserver" / "orders" / f"ingest_date={day.isoformat()}")
        write(pd.concat([existing, orders]), out, "sqlserver", "orders", day)
    write(items, out, "sqlserver", "order_items", day)
    write(gen_sf_cases(contacts, day, max(10, orders_per_day // 50)), out, "salesforce", "sf_case", day)
    write(gen_inventory(day, products, stores), out, "adls", "inventory_snapshot", day)
    write(pd.concat(all_returns), out, "adls", "returns", day)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["initial", "incremental"], default="initial")
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--out", default="out")
    ap.add_argument("--customers", type=int, default=5_000)
    ap.add_argument("--products", type=int, default=500)
    ap.add_argument("--stores", type=int, default=25)
    ap.add_argument("--orders-per-day", type=int, default=2_000)
    ap.add_argument("--history-days", type=int, default=30)
    a = ap.parse_args()
    run(a.mode, date.fromisoformat(a.date), Path(a.out), a.customers, a.products, a.stores,
        a.orders_per_day, a.history_days)
