import pandas as pd
from pathlib import Path

FULL = Path("data/processed/bike_sales_100k_enriched.csv")
SAMPLE = Path("data/sample/bike_sales_sample.csv")


def main():
    df = pd.read_csv(FULL)
    # Take a stratified sample: a few rows from each Warehouse/Product_Category
    sample = (
        df.groupby(["Warehouse", "Product_Category"], group_keys=False)
        .apply(lambda g: g.sample(min(len(g), 100), random_state=42))
        .reset_index(drop=True)
    )

    # Limit to max 500 rows (in case of imbalance)
    if len(sample) > 500:
        sample = sample.sample(500, random_state=42).reset_index(drop=True)

    SAMPLE.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(SAMPLE, index=False)
    print(f"Saved sample → {SAMPLE} ({len(sample)} rows)")


if __name__ == "__main__":
    main()
