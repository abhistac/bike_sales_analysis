# tests/test_enrich.py
from pathlib import Path
import pandas as pd

# Try to import the public enrichment function.
# Your module might expose it as enrich_dataframe(...) or enrich(...).
try:
    from src.etl.enrich import enrich_dataframe as enrich_df  # type: ignore
except ImportError:  # pragma: no cover
    from src.etl.enrich import enrich as enrich_df  # type: ignore


PROCESSED_PARQUET = Path("data/processed/bike_sales_100k_enriched.parquet")
SAMPLE_CSV = Path("data/sample/bike_sales_sample.csv")


def _get_enriched_df() -> pd.DataFrame:
    """
    Load the processed Parquet if it exists (developer machine),
    otherwise build an enriched frame from the repo-tracked sample CSV (CI).
    """
    if PROCESSED_PARQUET.exists():
        return pd.read_parquet(PROCESSED_PARQUET)

    # CI path: build from sample
    assert SAMPLE_CSV.exists(), "Sample CSV is missing from the repo."
    raw = pd.read_csv(SAMPLE_CSV)
    df = enrich_df(raw)
    # basic sanity that enrichment ran
    assert len(df) > 0
    return df


def test_enriched_columns_exist():
    df = _get_enriched_df()
    required = {
        "Warehouse",
        "Client_Type",
        "Product_Category",
        "Payment_Fee",
        "Net_Revenue",
    }
    missing = required.difference(df.columns)
    assert not missing, f"Missing expected columns: {missing}"


def test_net_revenue_not_null():
    df = _get_enriched_df()
    assert df["Net_Revenue"].notna().all(), "Net_Revenue contains nulls"
    assert (df["Net_Revenue"] >= 0).all(), "Net_Revenue contains negatives"
