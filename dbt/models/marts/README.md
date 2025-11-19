# Data Marts

Business-focused aggregated models optimized for analytics and dashboards.

## Overview

Marts are pre-aggregated tables that combine data from multiple warehouse models to answer specific business questions. They improve dashboard performance by reducing query complexity and execution time.

## Available Marts

### Sales Analytics

#### `mart_sales_daily`

- **Grain:** One row per day, product category, and customer state
- **Purpose:** Daily sales metrics for operational dashboards
- **Materialization:** Table (partitioned by date)
- **Refresh:** Daily at 3 AM (after warehouse build)

**Key Metrics:**

- Total orders and revenue
- Average order value
- Delivery performance
- Review scores
- Payment metrics
- Data quality indicators

**Use Cases:**

- Daily sales monitoring dashboards
- Operational performance tracking
- Category and geography analysis

---

#### `mart_sales_monthly`

- **Grain:** One row per month, product category, and customer state
- **Purpose:** Monthly trends and growth analysis
- **Materialization:** Table
- **Refresh:** Daily (aggregates from daily mart)

**Key Metrics:**

- Monthly revenue and orders
- Month-over-month growth
- Year-over-year comparisons
- Daily averages
- Delivery and review trends

**Use Cases:**

- Executive dashboards
- Business reviews
- Trend analysis
- Forecasting

---

### Customer Analytics

#### `mart_customer_cohorts`

- **Grain:** One row per cohort month and customer state
- **Purpose:** Customer cohort analysis and lifetime value
- **Materialization:** Table
- **Refresh:** Daily

**Key Metrics:**

- Cohort size and composition
- Customer lifetime value (LTV)
- Segment distribution (loyal/repeat/one-time)
- First month vs lifetime metrics
- Delivery and review performance by cohort

**Use Cases:**

- Customer acquisition analysis
- LTV calculations
- Cohort comparison
- Retention strategy planning

---

#### `mart_customer_retention`

- **Grain:** One row per cohort month, activity month, and customer state
- **Purpose:** Retention rate tracking and churn analysis
- **Materialization:** Table
- **Refresh:** Daily

**Key Metrics:**

- Retention rates by month
- Month-over-month retention
- Cumulative revenue per cohort
- Active customers per month
- Churn indicators

**Use Cases:**

- Retention dashboards
- Churn prediction
- Customer lifecycle analysis
- Marketing campaign effectiveness

---

### Product Analytics

#### `mart_product_performance`

- **Grain:** One row per product and month
- **Purpose:** Product sales performance and rankings
- **Materialization:** Table
- **Refresh:** Daily

**Key Metrics:**

- Revenue and order volume
- Product rankings (overall and by category)
- Month-over-month growth
- Review scores and sentiment
- Delivery performance
- Freight costs

**Use Cases:**

- Product analytics dashboards
- Inventory planning
- Category management
- Pricing optimization
- Supplier performance

---

## Mart Architecture

```
┌─────────────────────────────────────────────────┐
│                   MARTS LAYER                   │
│        (Business-focused aggregations)          │
└─────────────────────────────────────────────────┘
                        ▲
                        │
┌─────────────────────────────────────────────────┐
│                WAREHOUSE LAYER                  │
│                                                 │
│  ┌─────────────────┐      ┌─────────────────┐  │
│  │   Dimensions    │      │     Facts       │  │
│  │                 │      │                 │  │
│  │ • dim_customer  │      │ • fact_orders   │  │
│  │ • dim_product   │◄─────┤                 │  │
│  │ • dim_seller    │      │                 │  │
│  │ • dim_date      │      │                 │  │
│  └─────────────────┘      └─────────────────┘  │
└─────────────────────────────────────────────────┘
                        ▲
                        │
┌─────────────────────────────────────────────────┐
│                STAGING LAYER                    │
│         (Cleaned and standardized)              │
└─────────────────────────────────────────────────┘
```

## Query Performance

### Before Marts (Direct Warehouse Query)

```sql
-- Complex joins across multiple tables
-- 5-10 second execution time
-- Scans entire fact table

SELECT
  DATE_TRUNC(f.order_purchase_date, MONTH) as month,
  p.product_category_name_en,
  c.customer_state,
  COUNT(DISTINCT f.order_id) as orders,
  SUM(f.total_order_value) as revenue,
  AVG(f.review_score) as avg_review
FROM fact_orders f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN stg_order_items oi ON f.order_id = oi.order_id
JOIN dim_product p ON oi.product_id = p.product_id
WHERE f.order_status = 'delivered'
GROUP BY 1, 2, 3;
```

### After Marts (Pre-aggregated)

```sql
-- Simple SELECT from mart
-- <1 second execution time
-- Uses partitioned table

SELECT
  month_date,
  product_category,
  customer_state,
  total_orders,
  total_revenue,
  avg_review_score
FROM mart_sales_monthly
WHERE month_date >= '2017-01-01';
```

**Performance Improvement:** 5-10x faster queries

---

## Maintenance

### Refresh Strategy

- **Daily Marts:** Rebuild completely each day (small tables)
- **Incremental Option:** Can be converted to incremental if size grows

### Data Quality

- All marts have comprehensive tests in `schema.yml`
- Tests run after each mart build
- Alerts on test failures

### Monitoring

```bash
# Check mart freshness
dbt source freshness --select marts_only

# Run mart tests
dbt test --select marts_only

# Rebuild specific mart
dbt run --select mart_sales_daily
```

---

## Usage Examples

### Streamlit Dashboard

```python
# Fast query for dashboard
query = """
SELECT
  month_date,
  SUM(total_revenue) as revenue,
  SUM(total_orders) as orders
FROM `project-samba-insight.prod_warehouse.mart_sales_monthly`
WHERE month_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY month_date
ORDER BY month_date
"""

df = client.query(query).to_dataframe()
```

### Cohort Analysis

```python
# Customer retention curve
query = """
SELECT
  cohort_month_name,
  months_since_cohort,
  retention_rate_pct
FROM `project-samba-insight.prod_warehouse.mart_customer_retention`
WHERE cohort_month >= '2017-01-01'
ORDER BY cohort_month, months_since_cohort
"""

df = client.query(query).to_dataframe()
```

### Product Rankings

```python
# Top products by category
query = """
SELECT
  product_category,
  product_id,
  total_revenue,
  category_revenue_rank
FROM `project-samba-insight.prod_warehouse.mart_product_performance`
WHERE month_date = '2018-08-01'
  AND category_revenue_rank <= 10
ORDER BY product_category, category_revenue_rank
"""

df = client.query(query).to_dataframe()
```

---

## Best Practices

### When to Create a Mart

✅ **Do create a mart when:**

- The same aggregation is queried repeatedly
- Dashboard queries are slow (>3 seconds)
- Complex joins are needed across multiple tables
- You need month-over-month or year-over-year calculations

❌ **Don't create a mart when:**

- The query is rarely used
- The aggregation is simple (single table)
- Data needs to be real-time (marts are daily)
- The underlying data changes frequently

### Naming Conventions

- Prefix: `mart_`
- Category: `sales_`, `customer_`, `product_`
- Grain: `_daily`, `_monthly`, `_weekly`

Examples:

- `mart_sales_daily`
- `mart_customer_cohorts`
- `mart_product_performance`

---

## Extending Marts

To add a new mart:

1. **Create SQL file:** `models/marts/mart_new_analysis.sql`
2. **Add configuration:**
   ```sql
   {{
       config(
           materialized='table',
           tags=['mart', 'category']
       )
   }}
   ```
3. **Document in schema.yml:** Add model, columns, and tests
4. **Add to selector:** Update `selectors.yml` if needed
5. **Test locally:** `dbt run --select mart_new_analysis`
6. **Deploy:** Push to git, dbt Cloud will auto-deploy

---

## Troubleshooting

For issue troubleshooting:

- Check dbt logs: `dbt run --select marts_only --debug`
- Review test results: `dbt test --select marts_only`
- See main README: `/dbt/README.md`
- dbt Cloud docs: `https://[account].getdbt.com/docs`

---

**Last Updated:** 2025-11-18
**Models:** 5 marts
**Total Tests:** 35+
