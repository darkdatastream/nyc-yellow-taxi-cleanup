"""
Schema inspection — read ONLY the schema of each parquet file (no data loaded),
then report which columns/types differ across files.

Run this BEFORE touching the pipeline, so we adapt the code to what the files
actually contain instead of guessing.
"""

from pathlib import Path
import polars as pl

files = sorted(Path("data/raw/yellow_2025").glob("yellow_tripdata_*.parquet"))
print(f"Inspecting {len(files)} files\n")

schemas = {}
for f in files:
    schemas[f.name] = dict(pl.scan_parquet(str(f)).collect_schema())

# --- the column set of the very first file, used as the baseline ---
baseline_name = files[0].name
baseline = schemas[baseline_name]
baseline_cols = list(baseline.keys())

print(f"Baseline = {baseline_name}: {len(baseline_cols)} columns")
for c, t in baseline.items():
    print(f"    {c}: {t}")
print()

# --- compare every file to the union of all columns seen anywhere ---
all_cols = []
for s in schemas.values():
    for c in s:
        if c not in all_cols:
            all_cols.append(c)

print(f"Union of all columns across all files: {len(all_cols)}")
print()

# --- per file: what's MISSING vs the union, and any TYPE differences ---
print("Differences per file (vs union / vs baseline types):")
print("-" * 60)
for name, s in schemas.items():
    missing = [c for c in all_cols if c not in s]
    extra = [c for c in s if c not in baseline]
    type_diffs = []
    for c, t in s.items():
        if c in baseline and baseline[c] != t:
            type_diffs.append(f"{c}: {baseline[c]} -> {t}")

    if missing or extra or type_diffs:
        print(f"\n{name}  ({len(s)} cols)")
        if missing:
            print(f"   MISSING : {missing}")
        if extra:
            print(f"   EXTRA   : {extra}")
        if type_diffs:
            print(f"   TYPES   : {type_diffs}")

print()
print("-" * 60)
print("If nothing printed above the line, all files share one schema.")
