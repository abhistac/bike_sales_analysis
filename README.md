# Motorcycle Parts Sales Analysis

A portfolio project analyzing sales performance, customer segments, payment methods, warehouse performance, and product line profitability.

**Stack:** Python (pandas), DuckDB (SQL), Plotly/Matplotlib, optional Streamlit dashboard.

## Goals
- Compute **Net Revenue** after payment fees
- Compare **Retail vs Wholesale**
- Analyze **Payment Method** impact and **Warehouse** performance
- Visualize **time-series revenue trends**

## Data

- Full dataset (`data/raw/` and `data/processed/`) is ignored in GitHub to keep the repo lightweight.
- A **sample dataset** (`data/sample/bike_sales_sample.csv`) with ~500 rows is included for demo/reproducibility.
- To reproduce full results:
  1. Download the [100k Bike Sales dataset from Kaggle](https://www.kaggle.com/)
  2. Place it in `data/raw/`
  3. Run the ETL script:
     ```bash
     python -m src.etl.enrich
     ```
