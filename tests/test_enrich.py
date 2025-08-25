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
    Import src.etl.enrich and find a callable that takes one argument and
    returns a DataFrame. Name-agnostic (enrich_dataframe, enrich, transform, etc.).
    """
    mod = importlib.import_module("src.etl.enrich")

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
            except (TypeError, ValueError):
                pass

    for name, fn in vars(mod).items():
        if callable(fn):
            try:
                sig = inspect.signature(fn)
                if len(sig.parameters) == 1:
                    return fn
            except (TypeError, ValueError):
                continue

    raise AssertionError(
        "Could not find a single-argument enrichment function in src.etl.enrich "
        "(e.g., enrich_dataframe(df) or enrich(path))."
    )


def _call_enricher(enricher, raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Try calling enricher with a DataFrame; if it raises TypeError (expects a path),
    call it with the sample CSV path instead.
    """
    # Attempt: DataFrame signature
    try:
        out = enricher(raw_df)
        if isinstance(out, pd.DataFrame):
            return out
    except TypeError:
        pass

    # Fallback: Path signature
    out = enricher(str(SAMPLE_CSV))
    assert isinstance(out, pd.DataFrame), "Enricher must return a DataFrame"
    return out


def _get_enriched_df() -> pd.DataFrame:
    # Use processed parquet when present (dev machines)
    if PROCESSED_PARQUET.exists():
        return pd.read_parquet(PROCESSED_PARQUET)

    # CI path: build from tracked sample CSV
    assert SAMPLE_CSV.exists(), "Sample CSV is missing from the repo"
    raw = pd.read_csv(SAMPLE_CSV)
    enricher = _find_enricher()
    df = _call_enricher(enricher, raw)
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
