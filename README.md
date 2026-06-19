# NYC Yellow Taxi — Multi-Year Data Quality Pipeline

A reproducible data quality pipeline for NYC Yellow Taxi trip records,
intended for the 2021–2025 monthly Parquet files.

The pipeline reads raw monthly files, flags quality issues at the record
level, and writes summary reports. When requested, it also writes a full
per-record flagged Parquet output. Its guiding principle is **keep + flag,
never delete**: a record with a quality issue may still be a real trip, so it
is preserved and marked rather than silently removed.

## Current scope

This project focuses on data-quality validation, not business forecasting.
The pipeline checks both structural anomalies and domain constraints from the
NYC TLC Yellow Taxi data dictionary and the taxi zone reference file.

The raw Parquet files and the full flagged output are not committed to GitHub.
Only code, diagnostics, reference data, and lightweight report CSVs live in the
repository.

## Quality rules

| Flag | Rule | Handling |
|------|------|----------|
| `negative_money` | `fare_amount < 0` OR `total_amount < 0` | keep + flag |
| `null_pattern` | `RatecodeID`, `passenger_count`, `store_and_fwd_flag` all null | keep + flag as structured missingness |
| `invalid_vendor_id` | `VendorID` outside known TLC Yellow Taxi provider codes: `1`, `2`, `6`, `7` | keep + flag |
| `invalid_passenger_count` | `passenger_count` is null, fractional, `<= 0`, or `> 6` | keep + flag |
| `invalid_ratecode_id` | `RatecodeID` outside known TLC Yellow Taxi rate codes: `1`, `2`, `3`, `4`, `5`, `6`, `99` | keep + flag |
| `invalid_location_id` | `PULocationID` or `DOLocationID` outside taxi zone IDs `1–265` | keep + flag |
| `equal_time` | pickup timestamp equals dropoff timestamp | keep + flag |
| `reverse_time` | pickup timestamp is later than dropoff timestamp | split into DST-like and non-DST cases |
| `dst_fallback` | reverse-time row on that year's DST clock-fallback date, with both pickup and dropoff hour equal to `1` | keep + flag as DST-like ambiguity |
| `non_dst` | reverse-time row outside the DST-like pattern | keep + flag |

A row counts as **flagged** if any main anomaly rule fires. Overlapping issues
on the same row are counted once in `flag_any`; category summaries can overlap
because one record may have more than one quality issue.

## Schema and storage handling

The raw files are not fully consistent across years, and the pipeline handles
these known differences:

- `airport_fee` is spelled `airport_fee` in some files and `Airport_fee` in others; the pipeline normalises it to `airport_fee`.
- `cbd_congestion_fee` exists only from 2025; older files receive a null column so output schemas remain aligned.
- Small coded fields are validated first, then narrowed for flagged output: `VendorID`, `passenger_count`, and `RatecodeID` are stored as `UInt8`; `PULocationID` and `DOLocationID` are stored as `UInt16`.
- Invalid raw coded values are preserved in evidence columns before narrowing, for example `raw_invalid_VendorID` and `raw_invalid_passenger_count`.
- `flag_any` is forced to deterministic true/false logic; null comparisons are not allowed to create null anomaly flags.

## Known limitations

The pipeline does not yet validate that all expected monthly files are present.
If one of the 2021–2025 files is missing, the current code still processes the
files it finds. A future improvement should add an explicit completeness check
for the expected month set.

DST handling is intentionally conservative. The `dst_fallback` flag means
"DST-like reverse-time pattern", not proof that the row is correct. Without
additional timezone/fold metadata, the pipeline cannot prove the exact real-world
clock sequence for every row.

The committed report CSVs should be regenerated after rule changes. The source
of truth is the current `pipeline.py` plus a fresh local run against the raw TLC
Parquet files.

## Project layout

```
.
├── pipeline.py                  # main pipeline
├── diagnostics/
│   ├── inspect_schema.py        # schema differences across years
│   └── inspect_null_pattern.py  # vendor/year null-pattern diagnostic
├── requirements.txt
├── reports/                     # lightweight summary CSVs
└── data/
    ├── reference/               # taxi_zone_lookup.csv
    ├── raw/                     # raw Parquet files, gitignored
    └── processed/               # generated flagged output, gitignored
```

## Get the data

The monthly files come from the official NYC TLC trip record server. On
Debian/Ubuntu, this downloads the 2021–2025 Yellow Taxi monthly files:

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

python pipeline.py                  # summary reports only
python pipeline.py --write-flagged  # summary reports + full flagged dataset
```

The pipeline prints a per-file progress bar and writes:

- `reports/per_file_summary.csv`
- `reports/per_year_summary.csv`
- `reports/category_summary.csv`
- `reports/clean_vs_flagged.csv`

## Data source

NYC Taxi & Limousine Commission, TLC Trip Record Data and Yellow Taxi Trip
Records data dictionary.
