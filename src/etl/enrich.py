from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from pandas.util import hash_pandas_object

from src.utils.io import read_csv, write_csv, write_parquet
from src.etl.rules import fee_rate_for, categorize_product, warehouse_region


def guess_revenue_columns(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    Returns (gross_revenue_col, unit_price_col) best guesses.
    Priority:
      - 'Total' / 'Revenue' / 'Sales' / 'Amount' as gross revenue
      - else compute Unit_Price * Quantity if both exist
    """
    lowered = {c.lower(): c for c in df.columns}
    for key in ["total", "revenue", "sales", "amount", "order_total"]:
        if key in lowered:
            return lowered[key], None
    unit = lowered.get("unit_price") or lowered.get("price") or lowered.get("unitprice")
    qty = lowered.get("quantity") or lowered.get("qty") or lowered.get("order_qty")
    if unit and qty:
        return None, unit
    return None, None


def compute_client_type(df: pd.DataFrame, target_wholesale_ratio: float = 0.30) -> pd.Series:
    """
    Deterministic Retail/Wholesale split using a hash so it stays stable across runs.
    """
    h = hash_pandas_object(df.index.to_series(), index=False).astype("uint64")
    frac = (h % 10_000) / 10_000.0
    return pd.Series(
        pd.Categorical(
            ["Wholesale" if x < target_wholesale_ratio else "Retail" for x in frac],
            categories=["Retail", "Wholesale"],
        ),
        index=df.index,
        name="Client_Type",
    )


def main(in_path: str, out_csv: str, out_parquet: str | None = None) -> None:
    src = Path(in_path)
    df = read_csv(src)

    # Standardize common columns if present
    colmap = {c.lower(): c for c in df.columns}
    date_col = colmap.get("date")
    store_col = colmap.get("store_location") or colmap.get("store") or colmap.get("location")
    model_col = colmap.get("bike_model") or colmap.get("product") or colmap.get("item_name")
    qty_col = colmap.get("quantity") or colmap.get("qty")

    # Product category
    df["Product_Category"] = (
        df[model_col].astype(str).map(categorize_product) if model_col else "Bikes"
    )

    # Warehouse region from store location
    df["Warehouse"] = df[store_col].astype(str).map(warehouse_region) if store_col else "East"

    # Client type (deterministic split 70/30)
    df["Client_Type"] = compute_client_type(df, target_wholesale_ratio=0.30)

    # Payment fee rate
    pay_col = colmap.get("payment_method") or colmap.get("payment") or colmap.get("pay_method")
    df["Payment_Fee_Rate"] = df[pay_col].astype(str).map(fee_rate_for) if pay_col else 0.0

    # Gross / Net revenue
    gross_col, unit_price_col = guess_revenue_columns(df)
    if gross_col:
        df["Gross_Revenue"] = pd.to_numeric(df[gross_col], errors="coerce")
    elif unit_price_col and qty_col:
        unit = pd.to_numeric(df[unit_price_col], errors="coerce")
        qty = pd.to_numeric(df[qty_col], errors="coerce")
        df["Gross_Revenue"] = unit * qty
    else:
        # Best-effort: look for any price * quantity combo; otherwise zeros
        df["Gross_Revenue"] = 0.0

    df["Payment_Fee"] = df["Gross_Revenue"] * df["Payment_Fee_Rate"]
    df["Net_Revenue"] = df["Gross_Revenue"] - df["Payment_Fee"]

    # Clean dates (optional but useful)
    if date_col:
        df["Date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.to_period("M").astype(str)
    else:
        df["Year"] = None
        df["Month"] = None

    # Save
    write_csv(df, out_csv)
    if out_parquet:
        write_parquet(df, out_parquet)

    print(f"Saved enriched CSV → {out_csv}")
    if out_parquet:
        print(f"Saved enriched Parquet → {out_parquet}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="data/raw/bike_sales_100k.csv")
    ap.add_argument("--out_csv", default="data/processed/bike_sales_100k_enriched.csv")
    ap.add_argument("--out_parquet", default="data/processed/bike_sales_100k_enriched.parquet")
    args = ap.parse_args()
    main(args.in_path, args.out_csv, args.out_parquet)
