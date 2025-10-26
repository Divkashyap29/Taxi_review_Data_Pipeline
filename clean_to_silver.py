
import argparse
from pathlib import Path
import pandas as pd

# Columns we expect to be numeric if present
NUM_COLS = [
    "trip_duration_sec","trip_duration_min","trip_duration_hr",
    "distance_traveled_km","kph",
    "wait_time_cost","distance_cost","fare_w_flag",
    "tip","miscellaneous_fees","total_fare_new",
    "num_of_passengers"
]

# Money-like columns (must be >= 0 if present)
MONEY_COLS = ["wait_time_cost","distance_cost","fare_w_flag","tip","miscellaneous_fees","total_fare_new"]

# Composite key used for de-duplication
COMPOSITE_KEY = [
    "trip_duration_sec",
    "distance_traveled_km",
    "total_fare_new",
    "num_of_passengers",
    "tip",
    "surge_applied",
]

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input",  default="Taxi_Set.csv", help="Path to raw Taxi CSV (default: Taxi_Set.csv)")
    p.add_argument("--outdir", default="step2_silver", help="Output directory (default: step2_silver)")
    return p.parse_args()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = (
        d.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return d

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    # numeric coercion
    for c in NUM_COLS:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    # boolean coercion for surge_applied
    if "surge_applied" in d.columns:
        if d["surge_applied"].dtype == object:
            d["surge_applied"] = (
                d["surge_applied"].astype(str).str.strip().str.lower()
                .map({"true": True, "false": False, "1": True, "0": False})
                .fillna(False)
            )
        else:
            d["surge_applied"] = d["surge_applied"].astype(bool)
    return d

def clean_rows(d: pd.DataFrame) -> pd.DataFrame:
    df = d.copy()
    # positive duration & distance
    if "trip_duration_sec" in df: df = df[df["trip_duration_sec"].fillna(0) > 0]
    if "trip_duration_min" in df: df = df[df["trip_duration_min"].fillna(0) > 0]
    if "distance_traveled_km" in df: df = df[df["distance_traveled_km"].fillna(0) > 0]
    # money >= 0
    for m in MONEY_COLS:
        if m in df: df = df[df[m].fillna(0) >= 0]
    # passengers >= 1
    if "num_of_passengers" in df: df = df[df["num_of_passengers"].fillna(0) >= 1]
    # speed sanity
    if "kph" in df: df = df[(df["kph"].fillna(0) > 0) & (df["kph"] <= 160)]
    return df

def recompute_duration_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure minutes exist; recompute hours from minutes (or seconds if needed)."""
    d = df.copy()
    if "trip_duration_min" not in d.columns and "trip_duration_sec" in d.columns:
        d["trip_duration_min"] = d["trip_duration_sec"] / 60.0
    # Always recompute hours from minutes to avoid display quirks
    if "trip_duration_min" in d.columns:
        d["trip_duration_hr"] = (d["trip_duration_min"] / 60.0).round(6)
    elif "trip_duration_sec" in d.columns:
        d["trip_duration_hr"] = (d["trip_duration_sec"] / 3600.0).round(6)
    return d

def drop_duplicates_by_key(df: pd.DataFrame):
    key = [c for c in COMPOSITE_KEY if c in df.columns]
    dupes = df.duplicated(subset=key).sum() if key else 0
    out = df.drop_duplicates(subset=key) if key else df
    return out, int(dupes), key

def main():
    args = parse_args()
    in_path = Path(args.input)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "taxi_silver_clean.csv"

    raw = pd.read_csv(in_path, low_memory=False)
    n_in = len(raw)
    raw_cols = list(raw.columns)

    df = normalize_columns(raw)
    df = coerce_types(df)
    df = clean_rows(df)
    df = recompute_duration_fields(df)   # <— recompute hours from minutes (rounded)
    df, dup_count, key_used = drop_duplicates_by_key(df)

    df.to_csv(out_csv, index=False)

    print("=== STEP 2 SUMMARY ===")
    print(f"Input rows:                 {n_in}")
    print(f"After cleaning & de-dupe:   {len(df)}")
    print(f"Duplicates removed (key):   {dup_count}")
    print(f"Composite key used:         {key_used}")
    print(f"Columns in:                 {raw_cols}")
    print(f"Columns out:                {list(df.columns)}")
    print(f"Output file:                {out_csv.resolve()}")

if __name__ == "__main__":
    main()
