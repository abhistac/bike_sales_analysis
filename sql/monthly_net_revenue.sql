-- Monthly Net Revenue overall and by Product_Category
WITH m AS (
  SELECT
    Month,
    Product_Category,
    SUM(Net_Revenue) AS net_revenue
  FROM 'data/processed/bike_sales_100k_enriched.parquet'
  GROUP BY 1,2
)
SELECT * FROM m ORDER BY Month, Product_Category;
