-- Check for NULLs in key columns
SELECT 
  COUNT(*) as total_rows,
  COUNT(O_ORDERKEY) as order_key_count,
  COUNT(O_CUSTKEY) as cust_key_count,
  COUNT(O_TOTALPRICE) as price_count,
  COUNT(O_ORDERDATE) as date_count,
  SUM(CASE WHEN O_TOTALPRICE <= 0 THEN 1 ELSE 0 END) as negative_prices
FROM FMCG_ANALYTICS.RAW.ORDERS;

-- Check date range of data
SELECT 
  MIN(O_ORDERDATE) as earliest_order,
  MAX(O_ORDERDATE) as latest_order,
  COUNT(DISTINCT YEAR(O_ORDERDATE)) as years_of_data
FROM FMCG_ANALYTICS.RAW.ORDERS;

-- Check for duplicate order keys
SELECT O_ORDERKEY, COUNT(*) as cnt
FROM FMCG_ANALYTICS.RAW.ORDERS
GROUP BY O_ORDERKEY
HAVING COUNT(*) > 1;03_
