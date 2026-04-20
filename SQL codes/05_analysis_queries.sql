-- Q1: Month over month revenue growth by market segment
WITH monthly AS (
  SELECT 
    order_month,
    market_segment,
    SUM(net_revenue) AS total_revenue
  FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
  GROUP BY order_month, market_segment
)
SELECT 
  order_month,
  market_segment,
  total_revenue,
  LAG(total_revenue) OVER (
    PARTITION BY market_segment 
    ORDER BY order_month
  ) AS prev_month_revenue,
  ROUND(
    (total_revenue - LAG(total_revenue) OVER (
      PARTITION BY market_segment ORDER BY order_month)
    ) / NULLIF(LAG(total_revenue) OVER (
      PARTITION BY market_segment ORDER BY order_month), 0) * 100, 2
  ) AS mom_growth_pct
FROM monthly
ORDER BY market_segment, order_month;


-- Q2: Promotion effectiveness analysis
SELECT 
  promotion_tier,
  COUNT(DISTINCT order_id)             AS total_orders,
  ROUND(AVG(quantity), 2)              AS avg_quantity,
  ROUND(SUM(gross_revenue), 2)         AS total_gross_revenue,
  ROUND(SUM(discount_amount), 2)       AS total_discount_given,
  ROUND(SUM(net_revenue), 2)           AS total_net_revenue,
  ROUND(SUM(discount_amount) / 
    NULLIF(SUM(gross_revenue), 0) * 100, 2) AS discount_pct_of_revenue
FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
GROUP BY promotion_tier
ORDER BY total_net_revenue DESC;

-- Q3: Product performance ranking
SELECT 
  product_name,
  manufacturer,
  product_type,
  SUM(net_revenue)    AS total_revenue,
  SUM(quantity)       AS total_units,
  COUNT(DISTINCT order_id) AS order_count,
  DENSE_RANK() OVER (ORDER BY SUM(net_revenue) DESC) AS revenue_rank
FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
GROUP BY product_name, manufacturer, product_type
QUALIFY DENSE_RANK() OVER (ORDER BY SUM(net_revenue) DESC) <= 10
ORDER BY revenue_rank;

-- Q4: Customer lifetime value segmentation
WITH customer_ltv AS (
  SELECT 
    customer_id,
    customer_name,
    market_segment,
    SUM(net_revenue)         AS lifetime_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    MIN(order_date)          AS first_order,
    MAX(order_date)          AS last_order,
    DATEDIFF('day', MIN(order_date), MAX(order_date)) AS customer_tenure_days
  FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
  GROUP BY customer_id, customer_name, market_segment
)
SELECT *,
  CASE 
    WHEN lifetime_revenue > 500000 THEN 'Platinum'
    WHEN lifetime_revenue > 200000 THEN 'Gold'
    WHEN lifetime_revenue > 100000 THEN 'Silver'
    ELSE 'Bronze'
  END AS customer_tier,
  NTILE(4) OVER (ORDER BY lifetime_revenue DESC) AS revenue_quartile
FROM customer_ltv
ORDER BY lifetime_revenue DESC;


-- Q5: Detect months where revenue deviates significantly from average
WITH monthly_revenue AS (
  SELECT 
    order_month,
    SUM(net_revenue) AS monthly_revenue
  FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
  GROUP BY order_month
),
stats AS (
  SELECT 
    AVG(monthly_revenue)    AS avg_revenue,
    STDDEV(monthly_revenue) AS stddev_revenue
  FROM monthly_revenue
)
SELECT 
  m.order_month,
  m.monthly_revenue,
  s.avg_revenue,
  ROUND((m.monthly_revenue - s.avg_revenue) / 
    NULLIF(s.stddev_revenue, 0), 2) AS z_score,
  CASE 
    WHEN ABS((m.monthly_revenue - s.avg_revenue) / 
      NULLIF(s.stddev_revenue, 0)) > 2 
    THEN 'ANOMALY - Investigate'
    WHEN ABS((m.monthly_revenue - s.avg_revenue) / 
      NULLIF(s.stddev_revenue, 0)) > 1 
    THEN 'WARNING - Monitor'
    ELSE 'Normal'
  END AS kpi_status
FROM monthly_revenue m, stats s
ORDER BY m.order_month;