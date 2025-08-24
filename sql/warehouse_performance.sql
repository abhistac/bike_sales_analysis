SELECT
  Warehouse,
  SUM(Net_Revenue) AS net_revenue,
  SUM(Gross_Revenue) AS gross_revenue,
  SUM(Payment_Fee) AS payment_fees
FROM 'data/processed/bike_sales_100k_enriched.parquet'
GROUP BY 1
ORDER BY net_revenue DESC;
