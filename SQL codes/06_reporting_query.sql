-- Revenue summary view for Power BI
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.VW_REVENUE_MONTHLY AS
SELECT 
  order_month,
  order_year,
  market_segment,
  promotion_tier,
  SUM(net_revenue)    AS total_revenue,
  SUM(discount_amount) AS total_discount,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(quantity)       AS total_units
FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
GROUP BY order_month, order_year, market_segment, promotion_tier;

-- Product performance view
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.VW_PRODUCT_PERFORMANCE AS
SELECT 
  product_id,
  product_name,
  manufacturer,
  product_type,
  promotion_tier,
  SUM(net_revenue)    AS total_revenue,
  SUM(quantity)       AS total_units,
  ROUND(AVG(discount_rate) * 100, 2) AS avg_discount_pct
FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
GROUP BY product_id, product_name, manufacturer, product_type, promotion_tier;

-- Customer KPI view
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.VW_CUSTOMER_KPI AS
SELECT 
  customer_id,
  customer_name,
  market_segment,
  SUM(net_revenue)         AS lifetime_revenue,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(AVG(net_revenue), 2) AS avg_order_value,
  MAX(order_date)          AS last_order_date
FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
GROUP BY customer_id, customer_name, market_segment;

-- Add clustering key on staging table for performance
ALTER TABLE FMCG_ANALYTICS.STAGING.SALES_ENRICHED
CLUSTER BY (order_year, market_segment);
