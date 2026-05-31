"""
Why did the flag rate jump in 2024-2025?

The dominant flag is null_pattern (RatecodeID + passenger_count +
store_and_fwd_flag all null). This script breaks null_pattern down by VendorID
and year to test one hypothesis: a new data provider (new VendorID) appeared in
the later years and simply does not populate those fields.

Reads only the columns it needs. No data is modified.
"""

from pathlib import Path
import re
import polars as pl

files = sorted(Path("data/raw/yellow_2025").glob("yellow_tripdata_*.parquet"))


def year_of(name: str) -> int:
    return int(re.search(r"(\d{4})-\d{2}\.parquet$", name).group(1))


null_pattern = (
    pl.col("RatecodeID").is_null()
    & pl.col("passenger_count").is_null()
    & pl.col("store_and_fwd_flag").is_null()
)

parts = []
for f in files:
    part = (
        pl.scan_parquet(str(f))
        .with_columns(pl.col("VendorID").cast(pl.Int64))
        .group_by("VendorID")
        .agg([
            pl.len().alias("rows"),
            null_pattern.sum().alias("null_pattern_rows"),
        ])
        .with_columns(pl.lit(year_of(f.name)).alias("year"))
        .collect()
    )
    parts.append(part)

df = pl.concat(parts)

# --- total rows per (year, vendor) and how many are null_pattern ---
by_year_vendor = (
    df.group_by(["year", "VendorID"])
    .agg([
        pl.sum("rows").alias("rows"),
        pl.sum("null_pattern_rows").alias("null_pattern_rows"),
    ])
    .with_columns(
        (pl.col("null_pattern_rows") / pl.col("rows") * 100).round(2).alias("null_pct_within_vendor")
    )
    .sort(["year", "VendorID"])
)

pl.Config.set_tbl_rows(100)
print("=== Rows and null_pattern by year x VendorID ===")
print(by_year_vendor)

# --- which vendors exist in which years (presence map) ---
print("\n=== Which VendorIDs appear each year ===")
presence = (
    df.group_by("year")
    .agg(pl.col("VendorID").unique().sort().alias("vendors"))
    .sort("year")
)
for r in presence.iter_rows(named=True):
    print(f"  {r['year']}: {r['vendors']}")

# --- share of all null_pattern rows attributable to each vendor ---
print("\n=== Share of ALL null_pattern rows, by vendor ===")
vendor_share = (
    df.group_by("VendorID")
    .agg(pl.sum("null_pattern_rows").alias("null_pattern_rows"))
    .with_columns(
        (pl.col("null_pattern_rows") / pl.col("null_pattern_rows").sum() * 100).round(2).alias("pct_of_all_null_pattern")
    )
    .sort("null_pattern_rows", descending=True)
)
print(vendor_share)
