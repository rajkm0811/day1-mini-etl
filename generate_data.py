"""
Day 1 — Mini ETL pipeline: CSV -> clean -> validate -> partitioned Parquet.

WHAT THIS IS
-----------
A small but real ETL (Extract, Transform, Load) pipeline. It reads raw order
data from a CSV file, cleans it, runs data-quality checks, and writes the
result as partitioned Parquet files -- the standard layout for a data lake.

HOW TO RUN
----------
1. pip install pandas pyarrow
2. python generate_data.py   (creates the sample orders.csv)
3. python etl.py             (runs the pipeline, writes to output/)

WHAT EACH PART DOES
-------------------
- generate_data.py : builds a fake orders.csv with 102 rows, including 4
  intentionally dirty rows (a duplicate, a missing order_id, a negative
  price, a future date) so the cleaning steps have visible work to do.
- etl.py:
  1. EXTRACT  - reads orders.csv into a pandas DataFrame (in-memory table)
  2. CLEAN    - drops duplicates, missing order_ids, fixes types
  3. VALIDATE - quarantines bad rows (negative prices, future dates) and
                 counts them instead of crashing the whole load
  4. LOAD     - writes partitioned Parquet to output/order_month=YYYY-MM/
"""

import csv
import random
from datetime import datetime, timedelta

random.seed(42)  # fixed seed -> same "random" data on every run (reproducible)

with open("orders.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer", "product", "price", "order_date"])

    base = datetime(2026, 9, 1)
    ord5_row = None
    for i in range(1, 101):
        price = round(random.uniform(5, 500), 2)
        order_date = (base + timedelta(days=random.randint(0, 25))).strftime("%Y-%m-%d")
        if i == 7:
            price = -25.00               # dirty row 1: negative price
        if i == 13:
            order_date = "2027-06-01"    # dirty row 2: future date
        row = [
            f"ORD-{i:04d}",
            f"cust_{random.randint(1, 20)}",
            f"prod_{random.randint(1, 10)}",
            price,
            order_date,
        ]
        if i == 5:
            ord5_row = row              # remember it to duplicate exactly below
        writer.writerow(row)

    # dirty row 3: byte-for-byte duplicate of the ORD-0005 row generated above
    writer.writerow(ord5_row)
    # dirty row 4: missing order_id
    writer.writerow(["", "cust_9", "prod_4", 19.99, "2026-09-12"])

print("orders.csv created: 102 rows (100 generated + 1 duplicate + 1 missing id)")
