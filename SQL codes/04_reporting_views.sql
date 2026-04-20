-- Create staging table with business logic applied
CREATE OR REPLACE TABLE FMCG_ANALYTICS.STAGING.SALES_ENRICHED AS
SELECT 
  o.O_ORDERKEY                          AS order_id,
  o.O_CUSTKEY                           AS customer_id,
  c.C_NAME                              AS customer_name,
  c.C_MKTSEGMENT                        AS market_segment,
  c.C_NATIONKEY                         AS nation_id,
  l.L_PARTKEY                           AS product_id,
  p.P_NAME                              AS product_name,
  p.P_MFGR                              AS manufacturer,
  p.P_TYPE                              AS product_type,
  p.P_SIZE                              AS product_size,
  l.L_QUANTITY                          AS quantity,
  l.L_EXTENDEDPRICE                     AS gross_revenue,
  l.L_DISCOUNT                          AS discount_rate,
  l.L_TAX                               AS tax_rate,
  ROUND(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT), 2)    AS net_revenue,
  ROUND(l.L_EXTENDEDPRICE * l.L_DISCOUNT, 2)           AS discount_amount,
  o.O_ORDERDATE                         AS order_date,
  DATE_TRUNC('month', o.O_ORDERDATE)    AS order_month,
  DATE_TRUNC('quarter', o.O_ORDERDATE)  AS order_quarter,
  YEAR(o.O_ORDERDATE)                   AS order_year,
  o.O_ORDERSTATUS                       AS order_status,
  l.L_SHIPMODE                          AS ship_mode,
  CASE 
    WHEN l.L_DISCOUNT = 0 THEN 'No Promotion'
    WHEN l.L_DISCOUNT < 0.05 THEN 'Low Discount'
    WHEN l.L_DISCOUNT < 0.08 THEN 'Medium Discount'
    ELSE 'High Discount'
  END                                   AS promotion_tier
FROM FMCG_ANALYTICS.RAW.ORDERS o
JOIN FMCG_ANALYTICS.RAW.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN FMCG_ANALYTICS.RAW.CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN FMCG_ANALYTICS.RAW.PART p ON l.L_PARTKEY = p.P_PARTKEY
WHERE o.O_ORDERSTATUS != 'P';  -- exclude pending orders


-- FACT TABLE: one row per order line item
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.FACT_SALES AS
SELECT 
    l.L_ORDERKEY        AS order_id,
    l.L_PARTKEY         AS product_id,
    o.O_CUSTKEY         AS customer_id,
    o.O_ORDERDATE       AS order_date,
    l.L_QUANTITY        AS quantity,
    l.L_EXTENDEDPRICE   AS gross_revenue,
    l.L_DISCOUNT        AS discount_rate,
    ROUND(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT), 2) AS net_revenue,
    ROUND(l.L_EXTENDEDPRICE * l.L_DISCOUNT, 2)        AS discount_amount,
    l.L_SHIPMODE        AS ship_mode,
    CASE 
        WHEN l.L_DISCOUNT = 0     THEN 'No Promotion'
        WHEN l.L_DISCOUNT < 0.05  THEN 'Low Discount'
        WHEN l.L_DISCOUNT < 0.08  THEN 'Medium Discount'
        ELSE 'High Discount'
    END AS promotion_tier
FROM FMCG_ANALYTICS.RAW.ORDERS o
JOIN FMCG_ANALYTICS.RAW.LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERSTATUS != 'P';

-- DIMENSION 1: Customer
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.DIM_CUSTOMER AS
SELECT
    C_CUSTKEY       AS customer_id,
    C_NAME          AS customer_name,
    C_MKTSEGMENT    AS market_segment,
    C_NATIONKEY     AS nation_id
FROM FMCG_ANALYTICS.RAW.CUSTOMER;

-- DIMENSION 2: Product
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.DIM_PRODUCT AS
SELECT
    P_PARTKEY   AS product_id,
    P_NAME      AS product_name,
    P_MFGR      AS manufacturer,
    P_TYPE      AS product_type,
    P_SIZE      AS product_size,
    P_RETAILPRICE AS retail_price
FROM FMCG_ANALYTICS.RAW.PART;

-- DIMENSION 3: Date
CREATE OR REPLACE VIEW FMCG_ANALYTICS.REPORTING.DIM_DATE AS
SELECT DISTINCT
    O_ORDERDATE                             AS order_date,
    YEAR(O_ORDERDATE)                       AS year,
    QUARTER(O_ORDERDATE)                    AS quarter,
    MONTH(O_ORDERDATE)                      AS month,
    MONTHNAME(O_ORDERDATE)                  AS month_name,
    DAY(O_ORDERDATE)                        AS day,
    DATE_TRUNC('month', O_ORDERDATE)        AS month_start,
    DATE_TRUNC('quarter', O_ORDERDATE)      AS quarter_start,
    CONCAT('Q', QUARTER(O_ORDERDATE))       AS quarter_label
FROM FMCG_ANALYTICS.RAW.ORDERS
ORDER BY order_date;