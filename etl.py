"""
Day 1 — Mini ETL pipeline: CSV -> clean -> validate -> partitioned Parquet.

Run:  python etl.py      (after: python generate_data.py)

Pipeline stages:
  1. EXTRACT   read orders.csv into a pandas DataFrame
  2. CLEAN     drop duplicates, drop missing order_ids, coerce types
  3. VALIDATE  quarantine bad rows (negative price, future date) and count
               them -- one bad record must not kill the whole load, but it
               must be visible
  4. LOAD      write partitioned Parquet to output/order_month=YYYY-MM/
"""

import pandas as pd

# ---------------- 1. EXTRACT ----------------
# read_csv() loads the whole CSV into a DataFrame: a table in memory with
# rows and columns you can filter, transform and aggregate.
df = pd.read_csv("orders.csv")
print(f"1. Extracted rows: {len(df)}")

# ---------------- 2. CLEAN ----------------
before = len(df)
df = df.drop_duplicates()  # removes fully identical rows
print(f"2. Removed {before - len(df)} duplicate row(s)")

# dropna() removes rows where order_id is NaN (truly missing)...
df = df.dropna(subset=["order_id"])
# ...but an empty string "" is NOT NaN, so filter those explicitly too
df = df[df["order_id"] != ""]
print(f"3. Rows after dropping missing order_ids: {len(df)}")

# Convert text into proper types: dates -> datetime objects, price -> numbers.
# to_numeric(errors="coerce") turns anything unparseable into NaN instead of
# raising an error -- the standard way to survive messy source data.
df["order_date"] = pd.to_datetime(df["order_date"])
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df = df.dropna(subset=["price"])  # drop rows whose price couldn't be converted

# ---------------- 3. VALIDATE (quarantine, don't crash) ----------------
bad_prices = df[df["price"] < 0]
df = df[df["price"] >= 0]
print(f"4. Quarantined {len(bad_prices)} row(s) with negative prices")

future = df[df["order_date"] > pd.Timestamp.now()]
df = df[df["order_date"] <= pd.Timestamp.now()]
print(f"5. Quarantined {len(future)} row(s) with future dates")

# ---------------- 4. LOAD ----------------
# New column "2026-09" used only for partitioning the output.
df["order_month"] = df["order_date"].dt.strftime("%Y-%m")

# to_parquet writes the DataFrame as Parquet files.
# Parquet is columnar: it stores data column-by-column, so analytics queries
# reading 2 of 20 columns touch far less data, and compression works better.
# partition_cols splits output into folders like output/order_month=2026-09/,
# so a query engine reading September never even opens other months' files.
df.to_parquet("output/", partition_cols=["order_month"], index=False)
print(f"6. Wrote {len(df)} clean rows to output/ partitioned by order_month")
