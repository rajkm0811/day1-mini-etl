# Day 1 — Mini ETL: CSV → Clean → Validate → Partitioned Parquet

A complete, tested mini ETL pipeline in Python. Reads raw order data from CSV,
cleans and validates every row, quarantines bad records instead of dropping
them silently, and writes clean data as **partitioned Parquet** (by order
month) — the same quarantine pattern used in production data pipelines.

## What it does

1. **Extract** — loads `orders.csv` (102 synthetic rows)
2. **Clean** — strips whitespace, normalizes case, parses dates, casts types
3. **Validate** — every row must have: non-null `order_id`, `price > 0`,
   `order_date` not in the future
4. **Quarantine** — rows failing validation go to `quarantine.csv` with a
   `rejection_reason` column (never silently dropped)
5. **Load** — clean rows written as Parquet, partitioned by `order_month`
   (`output/order_month=YYYY-MM/`)

## Verified test result

102 rows in → **98 clean rows** out, 4 quarantined:
- 1 duplicate row
- 1 missing `order_id`
- 1 negative `price`
- 1 future `order_date`

Verified by reading the Parquet output back and recounting.

## Run it

### On Replit (zero install)
1. Upload `generate_data.py`, `etl.py`, and `orders.csv` to a new Python repl
2. Shell: `pip install pandas pyarrow`
3. Run: `python etl.py`
4. Check the `output/` folder for partitioned Parquet files

### Locally
```bash
pip install pandas pyarrow
python generate_data.py   # regenerates orders.csv (optional)
python etl.py
```

## Files

| File | Purpose |
|---|---|
| `generate_data.py` | Generates the synthetic `orders.csv` with realistic dirty data |
| `etl.py` | The pipeline: extract → clean → validate → quarantine → load |
| `orders.csv` | Raw input data (102 rows) |

## Key concepts demonstrated

- **Quarantine pattern** — bad rows are isolated with reasons, not deleted
- **Partitioned Parquet** — columnar storage partitioned by month for fast,
  cheap downstream reads
- **Idempotent runs** — output is fully rebuilt from scratch each run
- **Data validation rules** — null checks, range checks, temporal sanity checks

Part of a daily data-engineering project series — one new project every day.
