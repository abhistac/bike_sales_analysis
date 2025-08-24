import pandas as pd


def test_enriched_columns_exist():
    df = pd.read_parquet("data/processed/bike_sales_100k_enriched.parquet")
    expected = {
        "Warehouse",
        "Client_Type",
        "Product_Category",
        "Payment_Fee_Rate",
        "Gross_Revenue",
        "Payment_Fee",
        "Net_Revenue",
    }
    assert expected.issubset(df.columns), f"Missing columns: {expected - set(df.columns)}"


def test_net_revenue_not_null():
    df = pd.read_parquet("data/processed/bike_sales_100k_enriched.parquet")
    assert df["Net_Revenue"].notna().mean() > 0.95  # at least 95% non-null
