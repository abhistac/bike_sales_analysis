SELECT
  Client_Type,
  SUM(Net_Revenue) AS net_revenue,
  COUNT(*) AS orders
FROM 'data/processed/bike_sales_100k_enriched.parquet'
GROUP BY 1
ORDER BY net_revenue DESC;
