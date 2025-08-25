# tests/test_enrich.py
from __future__ import annotations

from pathlib import Path
import importlib
import inspect
import pandas as pd

PROCESSED_PARQUET = Path("data/processed/bike_sales_100k_enriched.parquet")
SAMPLE_CSV = Path("data/sample/bike_sales_sample.csv")


def _find_enricher():
    """
    Import src.etl.enrich and find a callable that takes a single argument
    (a DataFrame) and returns a DataFrame. Works regardless of the function name.
    """
    mod = importlib.import_module("src.etl.enrich")

    # Prefer conventional names first
    preferred = [
        "enrich_dataframe",
        "enrich_df",
        "enrich",
        "transform",
        "apply_enrichment",
        "process",
        "run",
        "build",
    ]
    for name in preferred:
        fn = getattr(mod, name, None)
        if callable(fn):
            try:
                sig = inspect.signature(fn)
                if len(sig.parameters) == 1:
                    return fn
            except (ValueError, TypeError):
                pass

    # Fallback: any single-arg callable returning a DataFrame
    for name, fn in vars(mod).items():
        if callable(fn):
            try:
                sig = inspect.signature(fn)
                if len(sig.parameters) == 1:
                    return fn
            except (ValueError, TypeError):
                continue

    raise AssertionError(
        "Could not find an enrichment function in src.etl.enrich. "
        "Expose a single-argument function that accepts a DataFrame and returns an enriched DataFrame."
    )


def _get_enriched_df() -> pd.DataFrame:
    # Use processed parquet when it exists locally; otherwise build from the sample.
    if PROCESSED_PARQUET.exists():
        return pd.read_parquet(PROCESSED_PARQUET)

    assert SAMPLE_CSV.exists(), "Sample CSV is missing from the repo."
    raw = pd.read_csv(SAMPLE_CSV)
    enricher = _find_enricher()
    df = enricher(raw)
    assert isinstance(df, pd.DataFrame)
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
