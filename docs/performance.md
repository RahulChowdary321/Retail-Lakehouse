# Performance & cost tuning

| Lever | Where | Why |
|---|---|---|
| **Z-ORDER** `store_id, product_id, customer_id` on the OBT | `optimize_tables.py` | Most Genie/BI filters and joins hit these; data skipping cuts bytes scanned. Applied to the last 14 days of partitions only. |
| **File compaction** | `OPTIMIZE` weekly + `optimizeWrite`/`autoCompact` on the ETL cluster | Avoids small files from incremental loads and streaming micro-batches (target ~128 MB-1 GB). |
| **Partitioning** by `order_date` (OBT, staging by `ingest_date`) | `build_obt.py`, `load_adls_to_staging.py` | Enables `replaceWhere` idempotent rewrites and partition pruning. Don't partition small tables. |
| **Shuffle partitions** | `jobs.yml` spark_conf | 15-20 GB shuffle / ~128 MB ≈ 160 -> 200, with AQE coalescing and skew-join handling. Revisit when volume doubles. |
| **Cluster sizing** | `jobs.yml` | Autoscale 2-8 workers, spot with on-demand driver fallback; separate small cluster for maintenance. Check Spark UI spill/GC before adding nodes. |
| **Broadcast joins** | `build_obt.py` | `dim_store` is tiny; broadcast avoids shuffling the fact. |
| **Explicit schemas** | staging loader | No inference scan; schema drift fails fast. |
| **Serverless SDP / predictive optimization** | `pipelines.yml` | SDP-managed tables are auto-maintained; manual OPTIMIZE is limited to batch-owned tables. |

Measure before/after with query history (bytes read, files pruned) and job cost per run; keep the numbers in your own runbook.
