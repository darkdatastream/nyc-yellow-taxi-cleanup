"""
NYC Yellow Taxi - Multi-Year Data Quality Pipeline

Reads raw monthly Parquet files (2021-2025), flags quality issues at the
record level (keep + flag, never delete), writes a flagged dataset plus
summary reports.

Quality rules:
  - negative_money          : fare_amount < 0  OR total_amount < 0
  - null_pattern            : RatecodeID, passenger_count, store_and_fwd_flag all null
  - invalid_vendor_id       : VendorID outside the official TLC yellow-taxi codes
  - invalid_passenger_count : passenger_count is missing, fractional, <= 0, or > 6
  - invalid_ratecode_id     : RatecodeID outside the official TLC yellow-taxi codes
  - invalid_location_id     : PULocationID or DOLocationID outside taxi_zone_lookup range
  - equal_time              : pickup == dropoff
  - reverse_time            : pickup >  dropoff
      - dst_fallback        : reverse_time on that year's DST clock-fallback date,
                              pickup hour == 1 AND dropoff hour == 1
      - non_dst             : reverse_time that is not the DST fallback pattern

Why this version exists (schema reality across years):
  - Column `airport_fee` is spelled `airport_fee` (2021-2022) and `Airport_fee`
    (2023+). We normalise to `airport_fee`.
  - `cbd_congestion_fee` exists only from 2025 (congestion pricing). Older files
    get it added as null so the rest of the code is identical for every year.
  - Numeric types drift (Int64 vs Int32, Float64 vs Int64). We validate domain
    constraints first, then narrow small coded fields to memory-light integer
    types for flagged output.
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


# --- Official/domain constraints used as data-quality checks ---
VALID_VENDOR_IDS = [1, 2, 6, 7]
VALID_RATECODE_IDS = [1, 2, 3, 4, 5, 6, 99]

MIN_PASSENGER_COUNT = 1
MAX_PASSENGER_COUNT = 6

# data/reference/taxi_zone_lookup.csv contains LocationID 1..265
MIN_TAXI_ZONE_ID = 1
MAX_TAXI_ZONE_ID = 265


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
ANOMALY_FLAGS = [
    "flag_negative_money",
    "flag_null_pattern",
    "flag_invalid_vendor_id",
    "flag_invalid_passenger_count",
    "flag_invalid_ratecode_id",
    "flag_invalid_location_id",
    "flag_equal_time",
    "flag_reverse_time",
]

REPORT_FLAGS = ANOMALY_FLAGS + [
    "flag_invalid_pu_location_id",
    "flag_invalid_do_location_id",
    "flag_dst_fallback",
    "flag_reverse_time_non_dst",
]


def year_from_filename(name: str) -> int:
    m = re.search(r"(\d{4})-\d{2}\.parquet$", name)
    if not m:
        raise ValueError(f"Cannot read year from filename: {name}")
    return int(m.group(1))


def code_is_in(expr: pl.Expr, valid_values: list[int]) -> pl.Expr:
    """Return true only for non-null, whole-number codes in the allowed set."""
    value = expr.cast(pl.Float64, strict=False)
    return (
        value.is_not_null()
        & (value == value.floor())
        & value.is_in([float(v) for v in valid_values])
    )


def whole_number_in_range(expr: pl.Expr, min_value: int, max_value: int) -> pl.Expr:
    """Return true only for non-null, whole-number values inside an inclusive range."""
    value = expr.cast(pl.Float64, strict=False)
    return (
        value.is_not_null()
        & (value == value.floor())
        & (value >= min_value)
        & (value <= max_value)
    )


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

    valid_vendor_id = code_is_in(pl.col("VendorID"), VALID_VENDOR_IDS)
    valid_passenger_count = whole_number_in_range(
        pl.col("passenger_count"),
        MIN_PASSENGER_COUNT,
        MAX_PASSENGER_COUNT,
    )
    valid_ratecode_id = code_is_in(pl.col("RatecodeID"), VALID_RATECODE_IDS)
    valid_pu_location_id = whole_number_in_range(
        pl.col("PULocationID"),
        MIN_TAXI_ZONE_ID,
        MAX_TAXI_ZONE_ID,
    )
    valid_do_location_id = whole_number_in_range(
        pl.col("DOLocationID"),
        MIN_TAXI_ZONE_ID,
        MAX_TAXI_ZONE_ID,
    )

    lf = lf.with_columns([
        ((pl.col("fare_amount") < 0) | (pl.col("total_amount") < 0))
        .fill_null(False)
        .alias("flag_negative_money"),
        (
            pl.col("RatecodeID").is_null()
            & pl.col("passenger_count").is_null()
            & pl.col("store_and_fwd_flag").is_null()
        ).alias("flag_null_pattern"),
        (~valid_vendor_id).alias("flag_invalid_vendor_id"),
        (~valid_passenger_count).alias("flag_invalid_passenger_count"),
        (~valid_ratecode_id).alias("flag_invalid_ratecode_id"),
        (~valid_pu_location_id).alias("flag_invalid_pu_location_id"),
        (~valid_do_location_id).alias("flag_invalid_do_location_id"),
        (pl.col("tpep_pickup_datetime") == pl.col("tpep_dropoff_datetime"))
        .fill_null(False)
        .alias("flag_equal_time"),
        reverse_time.fill_null(False).alias("flag_reverse_time"),
        dst_fallback.fill_null(False).alias("flag_dst_fallback"),
        (reverse_time & ~dst_fallback).fill_null(False).alias("flag_reverse_time_non_dst"),
    ])

    lf = lf.with_columns(
        (
            pl.col("flag_invalid_pu_location_id")
            | pl.col("flag_invalid_do_location_id")
        ).alias("flag_invalid_location_id")
    )

    # any_horizontal uses Kleene null logic; fill flags first so flag_any is
    # always deterministic true/false, never null.
    any_flag = pl.any_horizontal([pl.col(c).fill_null(False) for c in ANOMALY_FLAGS])
    return lf.with_columns(any_flag.alias("flag_any"))


def preserve_invalid_raw_values(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keep raw evidence for values that may become null during narrow casts."""
    return lf.with_columns([
        pl.when(pl.col("flag_invalid_vendor_id"))
        .then(pl.col("VendorID").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("raw_invalid_VendorID"),
        pl.when(pl.col("flag_invalid_passenger_count"))
        .then(pl.col("passenger_count").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("raw_invalid_passenger_count"),
        pl.when(pl.col("flag_invalid_ratecode_id"))
        .then(pl.col("RatecodeID").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("raw_invalid_RatecodeID"),
        pl.when(pl.col("flag_invalid_pu_location_id"))
        .then(pl.col("PULocationID").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("raw_invalid_PULocationID"),
        pl.when(pl.col("flag_invalid_do_location_id"))
        .then(pl.col("DOLocationID").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("raw_invalid_DOLocationID"),
    ])


def apply_storage_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Narrow small coded fields after flags are already created.

    strict=False prevents one bad value from crashing the audit. Bad values are
    already flagged, and preserve_invalid_raw_values keeps raw evidence columns
    for forensic review in the flagged output.
    """
    return lf.with_columns([
        pl.col("VendorID").cast(pl.UInt8, strict=False),
        pl.col("passenger_count").cast(pl.UInt8, strict=False),
        pl.col("RatecodeID").cast(pl.UInt8, strict=False),
        pl.col("PULocationID").cast(pl.UInt16, strict=False),
        pl.col("DOLocationID").cast(pl.UInt16, strict=False),
    ])


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

        if args.write_flagged:
            lf = preserve_invalid_raw_values(lf)
            lf = apply_storage_types(lf)

        counts = (
            lf.select(
                [pl.len().alias("rows")]
                + [pl.col(c).sum().alias(c) for c in REPORT_FLAGS + ["flag_any"]]
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
        ["file", "year", "rows"] + REPORT_FLAGS + ["flag_any"]
    )

    totals = per_file.select(
        [pl.sum("rows").alias("rows")]
        + [pl.sum(c).alias(c) for c in REPORT_FLAGS + ["flag_any"]]
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
        "category": [
            "null_pattern",
            "invalid_vendor_id",
            "invalid_passenger_count",
            "invalid_ratecode_id",
            "invalid_location_id",
            "negative_money",
            "equal_time",
            "reverse_time_dst_like",
            "reverse_time_non_dst",
        ],
        "rows": [
            t["flag_null_pattern"],
            t["flag_invalid_vendor_id"],
            t["flag_invalid_passenger_count"],
            t["flag_invalid_ratecode_id"],
            t["flag_invalid_location_id"],
            t["flag_negative_money"],
            t["flag_equal_time"],
            t["flag_dst_fallback"],
            t["flag_reverse_time_non_dst"],
        ],
        "pct_of_dataset": [
            pct(t["flag_null_pattern"]),
            pct(t["flag_invalid_vendor_id"]),
            pct(t["flag_invalid_passenger_count"]),
            pct(t["flag_invalid_ratecode_id"]),
            pct(t["flag_invalid_location_id"]),
            pct(t["flag_negative_money"]),
            pct(t["flag_equal_time"]),
            pct(t["flag_dst_fallback"]),
            pct(t["flag_reverse_time_non_dst"]),
        ],
        "recommended_handling": [
            "keep + flag as structured missingness",
            "keep + flag as invalid provider code",
            "keep + flag as invalid passenger count",
            "keep + flag as invalid rate code",
            "keep + flag as invalid taxi zone reference",
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
    print(f"invalid_vendor  : {t['flag_invalid_vendor_id']:,}  ({pct(t['flag_invalid_vendor_id'])}%)")
    print(f"invalid_passenger: {t['flag_invalid_passenger_count']:,}  ({pct(t['flag_invalid_passenger_count'])}%)")
    print(f"invalid_ratecode: {t['flag_invalid_ratecode_id']:,}  ({pct(t['flag_invalid_ratecode_id'])}%)")
    print(f"invalid_location: {t['flag_invalid_location_id']:,}  ({pct(t['flag_invalid_location_id'])}%)")
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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
