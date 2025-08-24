SELECT
  COALESCE(Payment_Method, 'Unknown') AS Payment_Method,
  AVG(Payment_Fee_Rate) AS avg_fee_rate,
  SUM(Payment_Fee) AS fees_collected,
  SUM(Gross_Revenue) AS gross_revenue,
  SUM(Net_Revenue) AS net_revenue
FROM 'data/processed/bike_sales_100k_enriched.parquet'
GROUP BY 1
ORDER BY net_revenue DESC;
