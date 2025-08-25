# app/streamlit_app.py
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import duckdb

# ---- Color palette (consistent across charts)
COLOR_GROSS = "#F39C12"  # orange
COLOR_NET = "#2E8B57"  # sea green
COLOR_PRODUCT = "#0E7C86"  # teal
COLOR_CITY = "#6A5ACD"  # slate blue
COLOR_WAREHOUSE = "#8E44AD"  # purple
COLOR_RETAIL = "#1E90FF"  # dodger blue
COLOR_WHOLESALE = "#B22222"  # firebrick
COLOR_FEES = "#E74C3C"  # fees (red)


# ---- Helpers: human-readable numbers
def human_currency(x: float) -> str:
    if pd.isna(x):
        return "—"
    ax = float(abs(x))
    if ax >= 1e9:
        return f"${x/1e9:.2f}B"
    if ax >= 1e6:
        return f"${x/1e6:.2f}M"
    if ax >= 1e3:
        return f"${x/1e3:.2f}K"
    return f"${x:,.0f}"


# ---- Helpers: nicer money ticks & hover
def money_tickformat(fig):
    fig.update_yaxes(tickprefix="$", separatethousands=True, matches=None)
    fig.update_xaxes(tickprefix="$", separatethousands=True, matches=None)


def month_coverage_text(df):
    if "_MonthDT" not in df.columns or df["_MonthDT"].dropna().empty:
        return "No month coverage (missing Month/Date)"
    mmin = pd.to_datetime(df["_MonthDT"].min()).strftime("%Y-%m")
    mmax = pd.to_datetime(df["_MonthDT"].max()).strftime("%Y-%m")
    return f"{mmin} → {mmax}"


DB_PATH = Path("data/processed/sales.duckdb")

DATA_FULL = Path("data/processed/bike_sales_100k_enriched.parquet")
DATA_SAMPLE = Path("data/sample/bike_sales_sample.csv")


@st.cache_data(show_spinner=False)
def load_data():
    if DB_PATH.exists():
        con = duckdb.connect(DB_PATH.as_posix())
        df = con.execute("SELECT * FROM sales").df()
        con.close()
        src = "duckdb (sales table)"
    elif DATA_FULL.exists():
        df = pd.read_parquet(DATA_FULL)
        src = "full (parquet)"
    elif DATA_SAMPLE.exists():
        df = pd.read_csv(DATA_SAMPLE)
        src = "sample (csv)"
    else:
        st.error(
            "No data store found. Run enrichment to create DuckDB and/or Parquet: `python -m src.etl.enrich`"
        )
        st.stop()

    # types
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Month normalization: build a robust month index from Month and/or Date
    month_from_col = None
    if "Month" in df.columns:
        # first try from existing Month string
        month_from_col = pd.to_datetime(df["Month"].astype(str) + "-01", errors="coerce")

    month_from_date = None
    if "Date" in df.columns:
        # fallback from Date column
        month_from_date = (
            pd.to_datetime(df["Date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        )

    # combine: prefer Month, else Date; if both NaT, stays NaT
    if month_from_col is not None and month_from_date is not None:
        df["_MonthDT"] = month_from_col.fillna(month_from_date)
    elif month_from_col is not None:
        df["_MonthDT"] = month_from_col
    elif month_from_date is not None:
        df["_MonthDT"] = month_from_date
    else:
        df["_MonthDT"] = pd.NaT

    # Create/standardize Month string from _MonthDT when available
    if "_MonthDT" in df.columns:
        month_str = df["_MonthDT"].dt.strftime("%Y-%m")
        if "Month" in df.columns:
            df.loc[month_str.notna(), "Month"] = month_str[month_str.notna()]
        else:
            df["Month"] = month_str

    # Do NOT drop rows aggressively; only later filters will handle NaT

    # Safe numeric
    for c in ["Gross_Revenue", "Net_Revenue", "Payment_Fee", "Quantity"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df, src


def compute_kpis(df):
    total_orders = len(df)
    total_gross = df["Gross_Revenue"].sum() if "Gross_Revenue" in df else np.nan
    total_net = df["Net_Revenue"].sum() if "Net_Revenue" in df else np.nan
    total_fees = df["Payment_Fee"].sum() if "Payment_Fee" in df else np.nan
    fee_pct = (
        (total_fees / total_gross * 100.0) if (total_gross and np.isfinite(total_gross)) else np.nan
    )
    return total_gross, total_net, total_orders, fee_pct


def kpi_strip(df):
    tg, tn, n, fee_pct = compute_kpis(df)
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1], gap="large")
    c1.metric("Total Gross Revenue", human_currency(tg))
    c2.metric("Total Net Revenue", human_currency(tn))
    c3.metric("Orders", f"{n:,}")
    c4.metric("Payment Fees % of Gross", "—" if pd.isna(fee_pct) else f"{fee_pct:.2f}%")

    # Second row: Top model & its revenue
    if {"Bike_Model", "Net_Revenue"}.issubset(df.columns) and len(df):
        t = (
            df.groupby("Bike_Model", as_index=False)["Net_Revenue"]
            .sum()
            .sort_values("Net_Revenue", ascending=False)
        )
        if len(t):
            top_model = str(t.iloc[0]["Bike_Model"]) if pd.notna(t.iloc[0]["Bike_Model"]) else "—"
            top_rev = (
                float(t.iloc[0]["Net_Revenue"])
                if pd.notna(t.iloc[0]["Net_Revenue"])
                else float("nan")
            )
            r1, r2 = st.columns([2, 1])
            r1.metric("Top Bike Model", top_model)
            r2.metric("Top Model Revenue", human_currency(top_rev))


st.set_page_config(page_title="Motorcycle Sales EDA", layout="wide")
st.title("🏍️ Motorcycle Sales — EDA Dashboard")

df, src = load_data()
if len(df) < 90000:
    st.warning(
        f"Loaded {len(df):,} rows from {src}. If you expected ~100k+, ensure enrichment has run on the full raw data."
    )
else:
    st.success(f"Using FULL dataset from {src} (rows: {len(df):,})")
st.caption(f"Data source: **{src}** | Rows: {len(df):,}")

if str(src).startswith("sample"):
    st.info(
        "Running in **Demo mode** (sample CSV). For full 100k analysis, run enrichment locally to build DuckDB/Parquet.",
        icon="ℹ️",
    )

# ---------------- Sidebar filters ----------------
with st.sidebar:
    import subprocess

    if st.button("🔄 Refresh data (ingest from raw)"):
        with st.spinner("Re-ingesting raw CSV via enrichment..."):
            subprocess.run(
                [
                    "python",
                    "-m",
                    "src.etl.enrich",
                    "--in",
                    "data/raw/bike_sales_100k.csv",
                    "--out_csv",
                    "data/processed/bike_sales_100k_enriched.csv",
                    "--out_parquet",
                    "data/processed/bike_sales_100k_enriched.parquet",
                ],
                check=False,
            )
        st.cache_data.clear()
        st.rerun()

    st.header("Filters")

    # Date range on Month
    mind, maxd = df["_MonthDT"].min(), df["_MonthDT"].max()
    d1, d2 = st.slider(
        "Date range (by month)",
        min_value=mind.to_pydatetime() if pd.notna(mind) else None,
        max_value=maxd.to_pydatetime() if pd.notna(maxd) else None,
        value=(
            (mind.to_pydatetime(), maxd.to_pydatetime())
            if pd.notna(mind) and pd.notna(maxd)
            else (None, None)
        ),
        format="YYYY-MM",
    )

    warehouses = sorted(df["Warehouse"].dropna().unique()) if "Warehouse" in df else []
    clients = sorted(df["Client_Type"].dropna().unique()) if "Client_Type" in df else []
    stores = sorted(df["Store_Location"].dropna().unique()) if "Store_Location" in df else []
    models = sorted(df["Bike_Model"].dropna().unique()) if "Bike_Model" in df else []

    sel_wh = st.multiselect("Warehouse", warehouses, default=warehouses)
    sel_ct = st.multiselect("Client Type", clients, default=clients)
    sel_st = st.multiselect("Store Location", stores, default=stores)
    sel_mod = st.multiselect("Bike Model", models, default=models)

    lock_axis = st.checkbox("🔒 Lock axis range for Product & City charts (42–45M)", value=True)

# Apply filters
f = df.copy()
if pd.notna(d1) and pd.notna(d2):
    f = f[(f["_MonthDT"] >= pd.to_datetime(d1)) & (f["_MonthDT"] <= pd.to_datetime(d2))]
if "Warehouse" in f.columns and sel_wh:
    f = f[f["Warehouse"].isin(sel_wh)]
if "Client_Type" in f.columns and sel_ct:
    f = f[f["Client_Type"].isin(sel_ct)]
if "Store_Location" in f.columns and sel_st:
    f = f[f["Store_Location"].isin(sel_st)]
if "Bike_Model" in f.columns and sel_mod:
    f = f[f["Bike_Model"].isin(sel_mod)]

# ---------------- Export filtered data ----------------
with st.expander("Export"):
    csv_bytes = f.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download filtered data (CSV)",
        data=csv_bytes,
        file_name="filtered_sales.csv",
        mime="text/csv",
    )

# ---------------- KPIs ----------------
st.subheader("Key Metrics")
kpi_strip(f)

st.subheader("About this view")
st.markdown(
    f"""
- **Rows (after filters):** {len(f):,}
- **Month coverage:** {month_coverage_text(f)}
- **Filters applied:** Warehouse={', '.join(sel_wh) if sel_wh else 'All'}, Client_Type={', '.join(sel_ct) if sel_ct else 'All'}, Stores={', '.join(sel_st) if sel_st else 'All'}, Models={', '.join(sel_mod) if sel_mod else 'All'}
"""
)

# ---------------- Monthly Sales Trends ----------------
st.subheader("Monthly Sales Trends (Gross vs Net)")
if {"Gross_Revenue", "Net_Revenue", "_MonthDT", "Month"}.issubset(f.columns):
    m = (
        f.groupby(["Month", "_MonthDT"], as_index=False)[["Gross_Revenue", "Net_Revenue"]]
        .sum()
        .sort_values("_MonthDT")
    )
    if len(m):
        fig = px.line(
            m,
            x="Month",
            y=["Gross_Revenue", "Net_Revenue"],
            markers=True,
            color_discrete_map={
                "Gross_Revenue": COLOR_GROSS,
                "Net_Revenue": COLOR_NET,
            },
        )
        fig.update_layout(xaxis_title=None, legend_title=None)
        fig.update_traces(hovertemplate="%{x}<br>%{fullData.name}: $%{y:,.0f}<extra></extra>")
        money_tickformat(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for the selected filters.")
else:
    st.info("Required columns not found for monthly trend.")

# ---------------- Product Analysis ----------------
st.subheader("Product Analysis — Bike Models by Net Revenue (Quantity labels)")
if {"Bike_Model", "Net_Revenue", "Quantity"}.issubset(f.columns):
    prod = (
        f.groupby("Bike_Model", as_index=False)
        .agg(Net_Revenue=("Net_Revenue", "sum"), Quantity=("Quantity", "sum"))
        .sort_values("Net_Revenue", ascending=False)
    )
    if len(prod):
        fig = px.bar(
            prod,
            x="Bike_Model",
            y="Net_Revenue",
            text="Quantity",
            color_discrete_sequence=[COLOR_PRODUCT],
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside", cliponaxis=False)
        fig.update_layout(xaxis_title=None, yaxis_title="Net Revenue")
        money_tickformat(fig)
        if lock_axis:
            fig.update_yaxes(range=[42_000_000, 45_000_000])
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>Net Revenue: $%{y:,.0f}<br>Qty: %{text:,}<extra></extra>"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product data after filtering.")
else:
    st.info("Product columns not available.")

# ---------------- Geographic & Warehouse Performance ----------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by City")
    if {"Store_Location", "Net_Revenue"}.issubset(f.columns):
        geo = (
            f.groupby("Store_Location", as_index=False)["Net_Revenue"]
            .sum()
            .sort_values("Net_Revenue", ascending=False)
        )
        if len(geo):
            fig = px.bar(
                geo,
                x="Net_Revenue",
                y="Store_Location",
                orientation="h",
                color_discrete_sequence=[COLOR_CITY],
            )
            fig.update_layout(xaxis_title="Net Revenue", yaxis_title=None)
            money_tickformat(fig)
            if lock_axis:
                fig.update_xaxes(range=[42_000_000, 45_000_000])
            fig.update_traces(hovertemplate="<b>%{y}</b><br>Net Revenue: $%{x:,.0f}<extra></extra>")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No city data after filtering.")
    else:
        st.info("Store_Location / Net_Revenue not found.")

with col2:
    st.subheader("Revenue by Warehouse")
    if {"Warehouse", "Net_Revenue"}.issubset(f.columns):
        wh = (
            f.groupby("Warehouse", as_index=False)["Net_Revenue"]
            .sum()
            .sort_values("Net_Revenue", ascending=False)
        )
        if len(wh):
            fig = px.bar(
                wh,
                x="Warehouse",
                y="Net_Revenue",
                color_discrete_sequence=[COLOR_WAREHOUSE],
            )
            fig.update_layout(xaxis_title=None, yaxis_title="Net Revenue")
            money_tickformat(fig)
            fig.update_traces(hovertemplate="<b>%{x}</b><br>Net Revenue: $%{y:,.0f}<extra></extra>")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No warehouse data after filtering.")
    else:
        st.info("Warehouse / Net_Revenue not found.")

# ---------------- Retail vs Wholesale by Payment Method ----------------
st.subheader("Retail vs Wholesale by Payment Method (Net Revenue)")
if {"Payment_Method", "Client_Type", "Net_Revenue"}.issubset(f.columns):
    pm = f.groupby(["Payment_Method", "Client_Type"], as_index=False, observed=False)[
        "Net_Revenue"
    ].sum()
    if len(pm):
        fig = px.bar(
            pm,
            x="Payment_Method",
            y="Net_Revenue",
            color="Client_Type",
            barmode="group",
            color_discrete_map={
                "Retail": COLOR_RETAIL,
                "Wholesale": COLOR_WHOLESALE,
            },
        )
        fig.update_layout(xaxis_title=None, yaxis_title="Net Revenue")
        money_tickformat(fig)
        fig.update_traces(hovertemplate="<b>%{x}</b><br>%{legendgroup}: $%{y:,.0f}<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No payment method data after filtering.")
else:
    st.info("Payment_Method / Client_Type / Net_Revenue not found.")
