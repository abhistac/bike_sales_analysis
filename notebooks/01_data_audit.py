# notebooks/01_data_audit.py
import pandas as pd
from pathlib import Path

RAW = Path("data/raw/bike_sales_100k.csv")
REPORTS = Path("reports")
REPORTS.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(RAW, low_memory=False)

# Basic shape
rows, cols = df.shape

# Schema summary
schema_rows = []
for col in df.columns:
    s = df[col]
    dtype = str(s.dtype)
    non_null = int(s.notna().sum())
    null_pct = round(100 * (1 - non_null / len(s)), 2)
    nunique = int(s.nunique(dropna=True))
    sample_vals = s.dropna().astype(str).unique()[:5]
    schema_rows.append(
        {
            "column": col,
            "dtype": dtype,
            "non_null": non_null,
            "null_%": null_pct,
            "unique": nunique,
            "sample_values": "; ".join(sample_vals),
        }
    )
schema = pd.DataFrame(schema_rows)

# Try to detect a date column
date_col = None
for c in df.columns:
    if "date" in c.lower():
        date_col = c
        break

date_info = {}
if date_col:
    dates = pd.to_datetime(df[date_col], errors="coerce", infer_datetime_format=True)
    if dates.notna().any():
        date_info = {
            "date_column": date_col,
            "min_date": str(dates.min()),
            "max_date": str(dates.max()),
            "non_null": int(dates.notna().sum()),
        }

# Save artifacts
schema_path = REPORTS / "data_dictionary.csv"
schema.to_csv(schema_path, index=False)

preview_path = REPORTS / "preview_head.csv"
df.head(200).to_csv(preview_path, index=False)

with open(REPORTS / "quick_profile.md", "w") as f:
    f.write("# Quick Profile\n\n")
    f.write(f"- Rows: {rows}\n- Cols: {cols}\n")
    if date_info:
        f.write(
            f"- Date column: {date_info['date_column']} "
            f"({date_info['min_date']} → {date_info['max_date']}; "
            f"non-null: {date_info['non_null']})\n"
        )

print("Saved:")
print(f" - {schema_path}")
print(f" - {preview_path}")
print(" - reports/quick_profile.md")
