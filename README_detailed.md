# FMCG Sales & Pricing Intelligence — Snowflake BI Project

A end-to-end Business Intelligence project built on **Snowflake** using the TPC-H sample dataset, simulating a real-world FMCG (Fast Moving Consumer Goods) pricing and promotion analytics use case. The project covers the full data analyst workflow — from raw data ingestion through SQL transformation to a Power BI reporting layer — including KPI anomaly detection using statistical analysis.

---

## Project Overview

| Item | Detail |
|---|---|
| **Data Source** | Snowflake Sample Data — TPC-H SF1 (1GB scale factor) |
| **Warehouse** | Snowflake (XS Virtual Warehouse — FMCG_WH) |
| **BI Tool** | Microsoft Power BI Desktop |
| **Schema Architecture** | Three-layer: RAW → STAGING → REPORTING |
| **Key Techniques** | Window functions, CTEs, clustering keys, materialized views, star schema, KPI anomaly detection |
| **Business Domain** | FMCG pricing, promotion analysis, customer segmentation, revenue intelligence |

---

## Business Problem

In the FMCG industry, pricing and promotional decisions are often made on gut feeling rather than data. The questions this project answers are:

- Which promotional discount tiers are actually driving volume — and which are just giving away margin?
- How is revenue trending month over month across different market segments?
- Which customers represent the highest lifetime value and how are they segmented?
- When a KPI drops unexpectedly — how do we know automatically, before a stakeholder notices?

---

## Dataset — TPC-H Sample Data

The TPC-H dataset is an industry-standard benchmark dataset provided free in every Snowflake account under `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`. It simulates a product ordering and supply chain environment and is used here to represent an FMCG business with customers, products, orders, and line-item transactions.

### Tables Used

| Table | Rows (SF1) | Description |
|---|---|---|
| `ORDERS` | 1,500,000 | Order header — customer, date, status, total price |
| `LINEITEM` | 6,001,215 | Order line items — product, quantity, price, discount, ship mode |
| `CUSTOMER` | 150,000 | Customer master — name, market segment, nation |
| `PART` | 200,000 | Product master — name, manufacturer, type, size, retail price |

### Column Mapping to FMCG Business Context

| TPC-H Column | FMCG Interpretation |
|---|---|
| `O_ORDERDATE` | Transaction date |
| `L_EXTENDEDPRICE` | Gross revenue |
| `L_DISCOUNT` | Promotional discount rate |
| `L_QUANTITY` | Units sold |
| `C_MKTSEGMENT` | Market segment (Automobile, Building, Furniture, Machinery, Household) |
| `P_MFGR` | Manufacturer / Brand |
| `P_TYPE` | Product category |
| `L_SHIPMODE` | Fulfilment channel |

---

## Architecture — Three Layer Schema

```
SNOWFLAKE_SAMPLE_DATA
        │
        │  (CTAS — full copy)
        ▼
FMCG_ANALYTICS.RAW          ← Source data, untouched
        │
        │  (SQL transformation + business logic)
        ▼
FMCG_ANALYTICS.STAGING      ← Cleaned, enriched, joined data
        │
        │  (Aggregated views for BI)
        ▼
FMCG_ANALYTICS.REPORTING    ← Star schema views → Power BI
```

**Why three layers?**

- **RAW** keeps source data untouched — if anything breaks downstream, raw data is always safe to reprocess
- **STAGING** is where all the analytical heavy lifting happens — joins, transformations, business logic
- **REPORTING** exposes only clean, business-ready views to Power BI — no raw IDs, no internal columns

---

## Project Structure

```
snowflake-fmcg-analytics/
│
├── 01_setup_database_schema.sql       # Database, schema, warehouse creation
├── 02_raw_load_source_data.sql        # Copy sample data into RAW schema + validation
├── 03_staging_transformation.sql      # Data cleaning, enrichment, star schema staging
├── 04_reporting_views.sql             # REPORTING layer views for Power BI
├── 05_analysis_queries.sql            # Business analysis SQL queries
│
└── README.md
```

---

## SQL Files — What Each Does

### `01_setup_database_schema.sql`
Creates the project database `FMCG_ANALYTICS`, three schemas (RAW, STAGING, REPORTING), and a dedicated XS Virtual Warehouse with auto-suspend set to 60 seconds to minimise cost.

```sql
CREATE DATABASE FMCG_ANALYTICS;
CREATE SCHEMA FMCG_ANALYTICS.RAW;
CREATE SCHEMA FMCG_ANALYTICS.STAGING;
CREATE SCHEMA FMCG_ANALYTICS.REPORTING;

CREATE WAREHOUSE FMCG_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

---

### `02_raw_load_source_data.sql`
Copies four TPC-H tables into the RAW schema using `CREATE TABLE AS SELECT`. Includes a row count reconciliation query to validate source vs destination match — data integrity check before any transformation begins.

```sql
-- Source vs destination validation
WITH source AS (
    SELECT 'ORDERS' as tbl, COUNT(*) as cnt
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    UNION ALL ...
),
destination AS (
    SELECT 'ORDERS' as tbl, COUNT(*) as cnt
    FROM FMCG_ANALYTICS.RAW.ORDERS
    UNION ALL ...
)
SELECT s.tbl, s.cnt AS source_rows, d.cnt AS raw_rows,
    CASE WHEN s.cnt = d.cnt THEN 'MATCH - OK'
         ELSE 'MISMATCH - INVESTIGATE' END AS validation_status
FROM source s JOIN destination d ON s.tbl = d.tbl;
```

---

### `03_staging_transformation.sql`
The core transformation layer. Joins all four RAW tables into a single enriched staging table with business logic applied:

- Calculates `net_revenue` after discount: `L_EXTENDEDPRICE * (1 - L_DISCOUNT)`
- Calculates `discount_amount`: `L_EXTENDEDPRICE * L_DISCOUNT`
- Derives `promotion_tier` using CASE WHEN on discount rate:
  - No Promotion (0%)
  - Low Discount (< 5%)
  - Medium Discount (5–8%)
  - High Discount (> 8%)
- Extracts date dimensions: `order_month`, `order_quarter`, `order_year` using `DATE_TRUNC`
- Excludes pending orders (`O_ORDERSTATUS != 'P'`)
- Adds a **clustering key** on `(order_year, market_segment)` for query optimisation

```sql
ALTER TABLE FMCG_ANALYTICS.STAGING.SALES_ENRICHED
CLUSTER BY (order_year, market_segment);
```

Also includes data quality checks: null validation, negative price detection, duplicate order key detection.

---

### `04_reporting_views.sql`
Creates four views in the REPORTING schema for Power BI — a star schema structure:

| View | Type | Description |
|---|---|---|
| `FACT_SALES` | Fact | Grain-level order line data — measures only |
| `DIM_CUSTOMER` | Dimension | Customer attributes — name, segment, nation |
| `DIM_PRODUCT` | Dimension | Product attributes — name, manufacturer, type |
| `DIM_DATE` | Dimension | Date attributes — year, quarter, month, day |

Power BI connects only to these REPORTING views — never to RAW or STAGING directly.

---

### `05_analysis_queries.sql`
Five business analysis queries demonstrating advanced SQL techniques:

**Q1 — Month-over-month revenue growth by market segment**
Uses `LAG()` window function partitioned by market segment with `NULLIF` for safe division.

**Q2 — Promotion ROI analysis**
Aggregates gross revenue, discount given, and net revenue by promotion tier to identify which discount level is most profitable.

**Q3 — Top 10 products by revenue**
Uses `DENSE_RANK()` window function with Snowflake's `QUALIFY` clause to filter ranked results cleanly.

**Q4 — Customer lifetime value segmentation**
Segments customers into Platinum / Gold / Silver / Bronze tiers by lifetime revenue. Uses `NTILE(4)` for revenue quartile assignment.

**Q5 — KPI anomaly detection using Z-score**
Statistically identifies months where revenue deviates significantly from the historical mean. Flags months with Z-score above 2 as anomalies requiring investigation — the foundation of automated KPI monitoring.

```sql
-- Z-score based KPI anomaly detection
WITH monthly_revenue AS (
    SELECT order_month, SUM(net_revenue) AS monthly_revenue
    FROM FMCG_ANALYTICS.STAGING.SALES_ENRICHED
    GROUP BY order_month
),
stats AS (
    SELECT AVG(monthly_revenue) AS avg_revenue,
           STDDEV(monthly_revenue) AS stddev_revenue
    FROM monthly_revenue
)
SELECT m.order_month, m.monthly_revenue,
    ROUND((m.monthly_revenue - s.avg_revenue) /
        NULLIF(s.stddev_revenue, 0), 2) AS z_score,
    CASE
        WHEN ABS((m.monthly_revenue - s.avg_revenue) /
            NULLIF(s.stddev_revenue, 0)) > 2 THEN 'ANOMALY - Investigate'
        WHEN ABS((m.monthly_revenue - s.avg_revenue) /
            NULLIF(s.stddev_revenue, 0)) > 1 THEN 'WARNING - Monitor'
        ELSE 'Normal'
    END AS kpi_status
FROM monthly_revenue m, stats s
ORDER BY m.order_month;
```

---

## Power BI Integration

The REPORTING schema views are connected to Power BI Desktop using the native Snowflake connector in **Import mode** with a star schema data model:

```
         [DIM_DATE]
              |
              | order_date
              |
[DIM_CUSTOMER] ── [FACT_SALES] ── [DIM_PRODUCT]
  customer_id          │            product_id
  market_segment       │            manufacturer
                  net_revenue       product_type
                  quantity
                  discount_rate
                  promotion_tier
```

### Dashboard KPIs
- Total Net Revenue
- Total Orders
- Average Discount %
- Month-over-month revenue trend by market segment
- Promotion tier revenue comparison
- Top 10 products by net revenue
- Customer segment breakdown

---

## Key Snowflake Concepts Demonstrated

| Concept | Where Used |
|---|---|
| Three-layer schema architecture | Full project structure |
| Virtual Warehouse sizing & auto-suspend | `01_setup_database_schema.sql` |
| CTAS (Create Table As Select) | `02_raw_load_source_data.sql` |
| Row count reconciliation | `02_raw_load_source_data.sql` |
| DATE_TRUNC for date grain | `03_staging_transformation.sql` |
| CASE WHEN business logic | `03_staging_transformation.sql` |
| Clustering Keys | `03_staging_transformation.sql` |
| Window functions — LAG, DENSE_RANK, NTILE | `05_analysis_queries.sql` |
| QUALIFY clause (Snowflake-specific) | `05_analysis_queries.sql` |
| NULLIF for safe division | `05_analysis_queries.sql` |
| Z-score anomaly detection | `05_analysis_queries.sql` |
| Star schema views for BI | `04_reporting_views.sql` |
| Power BI native Snowflake connector | Power BI model |

---

## How to Reproduce This Project

**Prerequisites:**
- Snowflake account (free trial at snowflake.com)
- Power BI Desktop (free download from Microsoft)

**Steps:**
1. Log into Snowflake — open a new SQL Worksheet
2. Run `01_setup_database_schema.sql`
3. Run `02_raw_load_source_data.sql` — verify all four tables show `MATCH - OK`
4. Run `03_staging_transformation.sql`
5. Run `04_reporting_views.sql`
6. Run `05_analysis_queries.sql` to explore the analysis
7. Open Power BI Desktop → Get Data → Snowflake → connect to `FMCG_ANALYTICS.REPORTING` schema → load the four views → build star schema model

---

## Author

**Anjali Soni**
Senior BI & Analytics Professional | Power BI | Snowflake | SQL
[LinkedIn](https://www.linkedin.com/in/anjali-soni0986/) | anjalisoni86@gmail.com

---

*Built using Snowflake's free TPC-H sample dataset. No proprietary data used.*
