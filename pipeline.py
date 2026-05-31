"""
NYC Yellow Taxi - Multi-Year Data Quality Pipeline

Reads raw monthly Parquet files (2021-2025), flags quality issues at the
record level (keep + flag, never delete), writes a flagged dataset plus
summary reports.

Quality rules (unchanged from the single-year audit):
  - negative_money    : fare_amount < 0  OR total_amount < 0
  - null_pattern      : RatecodeID, passenger_count, store_and_fwd_flag all null
  - equal_time        : pickup == dropoff
  - reverse_time      : pickup >  dropoff
      - dst_fallback  : reverse_time on that year's DST clock-fallback date,
                        pickup hour == 1 AND dropoff hour == 1
      - non_dst       : reverse_time that is not the DST fallback pattern

Why this version exists (schema reality across years):
  - Column `airport_fee` is spelled `airport_fee` (2021-2022) and `Airport_fee`
    (2023+). We normalise to `airport_fee`.
  - `cbd_congestion_fee` exists only from 2025 (congestion pricing). Older files
    get it added as null so the rest of the code is identical for every year.
  - Numeric types drift (Int64 vs Int32, Float64 vs Int64). We normalise the
    key columns so comparisons and writes are stable.
  - DST fallback date differs every year. We use an explicit lookup table per
    year instead of a single hardcoded date - a single hardcoded date would
    mislabel DST events in every other year.
"""

from pathlib import Path
import argparse
import re
import sys
import time

import polars as pl


# --- DST clock-fallback dates (first Sunday of November, US) ---
# Explicit on purpose: the client can read exactly which dates are treated as
# DST ambiguity. Add a new line when a new year of data arrives.
DST_FALLBACK_DATES = {
    2021: pl.date(2021, 11, 7),
    2022: pl.date(2022, 11, 6),
    2023: pl.date(2023, 11, 5),
    2024: pl.date(2024, 11, 3),
    2025: pl.date(2025, 11, 2),
}

# Flags that count toward "is this row anomalous". reverse_time already covers
# dst + non_dst, so sub-flags are NOT re-added (that would double count).
ANOMALY_FLAGS = ["flag_negative_money", "flag_null_pattern",
                 "flag_equal_time", "flag_reverse_time"]


def year_from_filename(name: str) -> int:
    m = re.search(r"(\d{4})-\d{2}\.parquet$", name)
    if not m:
        raise ValueError(f"Cannot read year from filename: {name}")
    return int(m.group(1))


def normalise_schema(lf: pl.LazyFrame, schema: dict) -> pl.LazyFrame:
    """Make every file look the same regardless of which year it came from."""
    cols = set(schema.keys())

    # 1) unify airport_fee spelling -> airport_fee
    if "Airport_fee" in cols and "airport_fee" not in cols:
        lf = lf.rename({"Airport_fee": "airport_fee"})
        cols.discard("Airport_fee")
        cols.add("airport_fee")

    # 2) add columns that are simply absent in older years, as null
    add = []
    if "airport_fee" not in cols:
        add.append(pl.lit(None).cast(pl.Float64).alias("airport_fee"))
    if "cbd_congestion_fee" not in cols:
        add.append(pl.lit(None).cast(pl.Float64).alias("cbd_congestion_fee"))
    if add:
        lf = lf.with_columns(add)

    # 3) normalise key column types so comparisons/writes are stable across years
    lf = lf.with_columns([
        pl.col("VendorID").cast(pl.Int64),
        pl.col("passenger_count").cast(pl.Float64),
        pl.col("RatecodeID").cast(pl.Float64),
        pl.col("PULocationID").cast(pl.Int64),
        pl.col("DOLocationID").cast(pl.Int64),
    ])
    return lf


def add_flags(lf: pl.LazyFrame, year: int) -> pl.LazyFrame:
    reverse_time = pl.col("tpep_pickup_datetime") > pl.col("tpep_dropoff_datetime")

    dst_date = DST_FALLBACK_DATES.get(year)
    if dst_date is None:
        # Unknown year: no DST date available -> treat no row as DST fallback,
        # so every reverse_time becomes non_dst. Safer than guessing a date.
        dst_fallback = pl.lit(False)
    else:
        dst_fallback = (
            reverse_time
            & (pl.col("tpep_pickup_datetime").dt.date() == dst_date)
            & (pl.col("tpep_dropoff_datetime").dt.date() == dst_date)
            & (pl.col("tpep_pickup_datetime").dt.hour() == 1)
            & (pl.col("tpep_dropoff_datetime").dt.hour() == 1)
        )

    lf = lf.with_columns([
        ((pl.col("fare_amount") < 0) | (pl.col("total_amount") < 0)).alias("flag_negative_money"),
        (
            pl.col("RatecodeID").is_null()
            & pl.col("passenger_count").is_null()
            & pl.col("store_and_fwd_flag").is_null()
        ).alias("flag_null_pattern"),
        (pl.col("tpep_pickup_datetime") == pl.col("tpep_dropoff_datetime")).alias("flag_equal_time"),
        reverse_time.alias("flag_reverse_time"),
        dst_fallback.alias("flag_dst_fallback"),
        (reverse_time & ~dst_fallback).alias("flag_reverse_time_non_dst"),
    ])
    any_flag = pl.any_horizontal([pl.col(c) for c in ANOMALY_FLAGS])
    return lf.with_columns(any_flag.alias("flag_any"))


def progress(i: int, n: int, name: str, t0: float) -> None:
    elapsed = time.time() - t0
    bar_len = 24
    filled = int(bar_len * i / n)
    bar = "#" * filled + "-" * (bar_len - filled)
    sys.stderr.write(f"\r[{bar}] {i}/{n}  {name:<32}  {elapsed:6.1f}s")
    sys.stderr.flush()
    if i == n:
        sys.stderr.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi multi-year data quality pipeline")
    parser.add_argument("--input", default="data/raw/yellow_2025")
    parser.add_argument("--output", default="data/processed")
    parser.add_argument("--reports", default="reports")
    parser.add_argument("--write-flagged", action="store_true")
    args = parser.parse_args()

    in_dir = Path(args.input)
    out_dir = Path(args.output)
    rep_dir = Path(args.reports)

    files = sorted(in_dir.glob("yellow_tripdata_*.parquet"))
    if not files:
        sys.stderr.write(f"ERROR: no parquet files found in {in_dir}\n")
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(files)} files in {in_dir}")

    per_file_rows = []
    t0 = time.time()

    for i, f in enumerate(files, start=1):
        year = year_from_filename(f.name)
        schema = dict(pl.scan_parquet(str(f)).collect_schema())

        lf = pl.scan_parquet(str(f))
        lf = normalise_schema(lf, schema)
        lf = add_flags(lf, year)

        counts = (
            lf.select(
                [pl.len().alias("rows")]
                + [pl.col(c).sum().alias(c) for c in
                   ["flag_negative_money", "flag_null_pattern", "flag_equal_time",
                    "flag_reverse_time", "flag_dst_fallback",
                    "flag_reverse_time_non_dst", "flag_any"]]
            )
            .collect()
            .with_columns([pl.lit(f.name).alias("file"), pl.lit(year).alias("year")])
        )
        per_file_rows.append(counts)

        if args.write_flagged:
            out_path = out_dir / f.name.replace(".parquet", "_flagged.parquet")
            lf.sink_parquet(str(out_path))

        progress(i, len(files), f.name, t0)

    per_file = pl.concat(per_file_rows).select(
        ["file", "year", "rows", "flag_negative_money", "flag_null_pattern",
         "flag_equal_time", "flag_reverse_time", "flag_dst_fallback",
         "flag_reverse_time_non_dst", "flag_any"]
    )

    totals = per_file.select(
        [pl.sum("rows").alias("rows")]
        + [pl.sum(c).alias(c) for c in
           ["flag_negative_money", "flag_null_pattern", "flag_equal_time",
            "flag_reverse_time", "flag_dst_fallback",
            "flag_reverse_time_non_dst", "flag_any"]]
    )
    t = totals.row(0, named=True)
    all_rows = t["rows"]

    def pct(x: int) -> float:
        return round(x / all_rows * 100, 4) if all_rows else 0.0

    per_year = (
        per_file.group_by("year")
        .agg([pl.sum("rows").alias("rows"), pl.sum("flag_any").alias("flagged")])
        .with_columns((pl.col("flagged") / pl.col("rows") * 100).round(4).alias("flagged_pct"))
        .sort("year")
    )

    category_summary = pl.DataFrame({
        "category": ["null_pattern", "negative_money", "equal_time",
                     "reverse_time_dst_like", "reverse_time_non_dst"],
        "rows": [t["flag_null_pattern"], t["flag_negative_money"], t["flag_equal_time"],
                 t["flag_dst_fallback"], t["flag_reverse_time_non_dst"]],
        "pct_of_dataset": [pct(t["flag_null_pattern"]), pct(t["flag_negative_money"]),
                           pct(t["flag_equal_time"]), pct(t["flag_dst_fallback"]),
                           pct(t["flag_reverse_time_non_dst"])],
        "recommended_handling": [
            "keep + flag as structured missingness",
            "keep + flag as vendor-specific monetary pattern",
            "keep + flag as structured timestamp anomaly",
            "keep + flag as DST-like time ambiguity",
            "keep + flag as structured non-DST time anomaly",
        ],
    })

    clean_vs_flagged = pl.DataFrame({
        "category": ["flagged_rows", "clean_rows"],
        "rows": [t["flag_any"], all_rows - t["flag_any"]],
        "pct_of_dataset": [pct(t["flag_any"]), pct(all_rows - t["flag_any"])],
    })

    per_file.write_csv(rep_dir / "per_file_summary.csv")
    per_year.write_csv(rep_dir / "per_year_summary.csv")
    category_summary.write_csv(rep_dir / "category_summary.csv")
    clean_vs_flagged.write_csv(rep_dir / "clean_vs_flagged.csv")

    print()
    print("=" * 56)
    print(f"Files processed : {len(files)}")
    print(f"Years covered   : {per_year['year'].min()}-{per_year['year'].max()}")
    print(f"Total rows      : {all_rows:,}")
    print(f"Flagged (any)   : {t['flag_any']:,}  ({pct(t['flag_any'])}%)")
    print(f"Clean           : {all_rows - t['flag_any']:,}  ({pct(all_rows - t['flag_any'])}%)")
    print("-" * 56)
    print(f"null_pattern    : {t['flag_null_pattern']:,}  ({pct(t['flag_null_pattern'])}%)")
    print(f"negative_money  : {t['flag_negative_money']:,}  ({pct(t['flag_negative_money'])}%)")
    print(f"equal_time      : {t['flag_equal_time']:,}  ({pct(t['flag_equal_time'])}%)")
    print(f"reverse (DST)   : {t['flag_dst_fallback']:,}")
    print(f"reverse(non-DST): {t['flag_reverse_time_non_dst']:,}")
    print("-" * 56)
    print("Per year:")
    for r in per_year.iter_rows(named=True):
        print(f"  {r['year']}: {r['rows']:>12,} rows   flagged {r['flagged_pct']:>7}%")
    print("=" * 56)
    print(f"Reports written to {rep_dir}/")
    if args.write_flagged:
        print(f"Flagged dataset written to {out_dir}/")
    else:
        print("(run with --write-flagged to also save the full per-record dataset)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
