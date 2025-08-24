import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

df = pd.read_parquet("data/processed/bike_sales_100k_enriched.parquet")

# Monthly net revenue
m = (
    df.dropna(subset=["Month"])
    .groupby(["Month"], as_index=False)["Net_Revenue"]
    .sum()
    .sort_values("Month")
)

plt.figure()
plt.plot(m["Month"], m["Net_Revenue"])
plt.xticks(rotation=45, ha="right")
plt.title("Monthly Net Revenue")
plt.tight_layout()
Path("reports/figs").mkdir(parents=True, exist_ok=True)
plt.savefig("reports/figs/monthly_net_revenue.png", dpi=160)

# Warehouse bar
w = (
    df.groupby("Warehouse", as_index=False)["Net_Revenue"]
    .sum()
    .sort_values("Net_Revenue", ascending=False)
)
plt.figure()
plt.bar(w["Warehouse"], w["Net_Revenue"])
plt.title("Net Revenue by Warehouse")
plt.tight_layout()
plt.savefig("reports/figs/warehouse_net_revenue.png", dpi=160)

print("Saved plots to reports/figs/")
