# snowflake-etl-powerbi-dashboard
snowflake-etl-powerbi-dashboard/
│
├── README.md
├── sql/
│   ├── 01_setup_database_schema.sql
│   ├── 02_raw_load_source_data.sql
│   ├── 03_staging_transformation.sql
│   ├── 04_reporting_views.sql
│   ├── 05_analysis_queries.sql
│   └── 06_reporting_query.sql

├── data/
│   └── RAW           ←  small sample only, not full data  
│        └── CUSTOMER.CSV
│        └── LINEITEM.CSV
│        └── ORDERS.CSV  
│        └── PART.CSV     
│   └── STAGING   
│        └── SALES_ENRICHED.CSV      
│   └── REPORTING  
│        └── DIM_CUSTOMER.CSV
│        └── DIM_DATE.CSV
│        └── DIM_PRODUCT.CSV  
│        └── FACT_SALES.CSV  
│        └── VW_CUSTOMER_KPI.CSV
│        └── VW_PRODUCT_PERFORMANCE.CSV  
│        └── VW_REVENUE_MONTHLY.CSV  
├── screenshots/
│   ├── snowflake_FMCG_Analytics.png
└── pbix/
    └── snowflake_KPI_sample_dashboard.pbix         ← optional, if file size is small

## Project Overview
| Item | Detail |
|---|---|
| **Data Source** | Snowflake Sample Data — TPC-H SF1 (1GB scale factor) |
| **Warehouse** | Snowflake (XS Virtual Warehouse — FMCG_WH) |
| **BI Tool** | Microsoft Power BI Desktop |
| **Schema Architecture** | Three-layer: RAW → STAGING → REPORTING |
| **Key Techniques** | Window functions, CTEs, clustering keys, materialized views, star schema, KPI anomaly detection |
| **Business Domain** | FMCG pricing, promotion analysis, customer segmentation, revenue intelligence |

## Tools Used
- Snowflake (data ingestion, storage, transformation)
- SQL (cleaning, deduplication, joins, aggregations)
- Power BI Desktop (dashboard, DAX measures, data model)

## Pipeline Steps
1. Ingested raw CSV data into Snowflake staging table
2. Cleaned data — handled nulls, duplicates, data type mismatches
3. Transformed into final reporting table using SQL
4. Connected Power BI to Snowflake via ODBC connector
5. Built dashboard with [X] KPIs tracking [what the data is about]

## Key SQL Transformations
- Removed duplicate records using ROW_NUMBER() window function
- Standardised date formats across source files
- Joined fact and dimension tables for final reporting model

## Dashboard Preview
![Dashboard](screenshots/dashboard_overview.png)

**Anjali Soni**
Senior BI & Analytics Professional | Power BI | Snowflake | SQL
[LinkedIn](https://www.linkedin.com/in/anjali-soni0986/) | anjalisoni86@gmail.com# snowflake-etl-powerbi-dashboard
snowflake-etl-powerbi-dashboard
