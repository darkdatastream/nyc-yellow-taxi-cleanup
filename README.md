# NYC Yellow Taxi — Multi-Year Data Quality Pipeline

A reproducible data quality pipeline for NYC Yellow Taxi trip records,
**2021–2025** — 60 monthly Parquet files, **198,762,954 records**.

The pipeline reads raw monthly files, flags quality issues **at the record
level**, and writes both a flagged dataset and summary reports. Its guiding
principle is **keep + flag, never delete**: a record with a quality issue may
still be a real, revenue-bearing trip, so it is preserved and marked rather than
dropped.

## Why five years (and what it revealed)

A single year of data is misleading. Run the audit on 2025 alone and **26.6%**
of records look anomalous — a number that, taken at face value, would suggest
the dataset is badly broken. It isn't. Five years side by side tell a different
story:

| Year | Rows | Flagged |
|------|------|---------|
| 2021 | 30,904,308 | 5.30% |
| 2022 | 39,656,098 | 4.14% |
| 2023 | 38,310,226 | 4.44% |
| 2024 | 41,169,720 | 11.44% |
| 2025 | 48,722,602 | 26.60% |

Quality was stable and improving through 2023, then the flag rate jumped sharply
in 2024 and again in 2025. That is not random noise — it has a cause.

**The dominant flag is `null_pattern`** (RatecodeID, passenger_count and
store_and_fwd_flag all null) — ~20M of the ~22.6M flagged rows. The obvious
hypothesis was a new data provider. The data rejects it: VendorID 7 first
appears in 2024, but generates **zero** null_pattern rows.

The real cause is a **reporting regression in the two dominant existing
providers (VendorID 1 and 2), starting in 2024.** For VendorID 2 — which alone
accounts for **81%** of all null_pattern rows — the rate of unpopulated fields
rose from **2.8% (2023) → 10.3% (2024) → 25.9% (2025)**. VendorID 1 shows the
same trend. Two independent providers degrading simultaneously points to a
**systemic change at the source** (e.g. a TLC reporting format change from 2024),
not a single-vendor bug.

**Recommendation:** keep the records, flag the affected fields as structurally
missing from 2024 onward, and investigate at the source — do not delete.

The analysis behind this lives in [`diagnostics/`](diagnostics/).

## Quality rules

| Flag | Rule | Handling |
|------|------|----------|
| `negative_money` | `fare_amount < 0` OR `total_amount < 0` | keep + flag (vendor-specific monetary pattern) |
| `null_pattern` | `RatecodeID`, `passenger_count`, `store_and_fwd_flag` all null | keep + flag (structured missingness) |
| `equal_time` | pickup == dropoff | keep + flag (structured timestamp anomaly) |
| `reverse_time` | pickup > dropoff | split into the two below |
| → `dst_fallback` | reverse_time on that year's DST clock-fallback date, both hour == 1 | keep + flag (DST ambiguity, not an error) |
| → `non_dst` | reverse_time outside the DST window | keep + flag (structured non-DST anomaly) |

A row counts as **flagged** if any rule fires. Overlapping issues on the same
row are counted **once** — no double counting.

DST clock-fallback dates differ every year (first Sunday of November). The
pipeline uses an explicit per-year table rather than a hardcoded date — a single
hardcoded date would mislabel DST events in every other year.

## Multi-year schema handling

The raw files are not consistent across years, and the pipeline absorbs this:

- `airport_fee` is spelled `airport_fee` (2021–2022) and `Airport_fee` (2023+) — normalised to `airport_fee`.
- `cbd_congestion_fee` exists only from 2025 (congestion pricing) — added as null for earlier years.
- Numeric types drift (Int64/Int32, Float64/Int64) — key columns normalised.
- Timestamp resolution drifts (ns/µs) — handled.

## Project layout

```
.
├── pipeline.py              # the pipeline (run this)
├── diagnostics/
│   ├── inspect_schema.py        # schema differences across years
│   └── inspect_null_pattern.py  # the vendor regression analysis
├── requirements.txt
├── reports/                 # summary CSVs (committed — lightweight)
└── data/
    ├── reference/           # taxi_zone_lookup.csv
    ├── raw/                 # raw Parquet (gitignored — download separately)
    └── processed/           # full flagged dataset (gitignored — generated locally)
```

Raw data and the full flagged output (≈3.8 GB) are **not** committed — only code
and lightweight reports live in the repo.

## Get the data

The monthly files come from the official NYC TLC trip record server. On
Debian/Ubuntu, one command downloads 2021–2025 (skipping 2020):

```bash
mkdir -p data/raw/yellow_2025 && \
for y in 2021 2022 2023 2024 2025; do \
  for m in 01 02 03 04 05 06 07 08 09 10 11 12; do \
    wget -P data/raw/yellow_2025 \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${y}-${m}.parquet"; \
  done; \
done
```

## Run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python pipeline.py                  # reports only (fast)
python pipeline.py --write-flagged  # + full per-record flagged dataset (local, ~3.8 GB)
```

The pipeline prints a per-file progress bar and a summary, and writes
`per_file_summary.csv`, `per_year_summary.csv`, `category_summary.csv` and
`clean_vs_flagged.csv` to `reports/`. All numbers are computed from the data on
every run — nothing is hardcoded.

## Data source

NYC Taxi & Limousine Commission, [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
