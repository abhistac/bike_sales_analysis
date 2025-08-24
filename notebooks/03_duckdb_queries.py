import duckdb

con = duckdb.connect()
for path in [
    "sql/monthly_net_revenue.sql",
    "sql/warehouse_performance.sql",
    "sql/client_type.sql",
    "sql/payment_method_profitability.sql",
    "sql/product_line_profitability.sql",
]:
    print(f"\n--- {path} ---")
    with open(path, "r") as f:
        q = f.read()
    print(con.sql(q).df().head(20).to_string(index=False))
