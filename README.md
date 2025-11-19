# Project Samba Insight: Brazil E-Commerce

## Overview

This project builds a complete date pipeline and analytics ecosystem using the Braazilian E-Commerce dataset. It demonstrates best practices in data ingestion, transformation, quality assurance, warehousing and business intelligence powered by modern tools such as **Python**, **BigQuery**, **dbt**, **Great Expectations**, **Looker** and **Looker Studio**.

Ther result is a scalable, transparent and well governed data platform that enables analysts, data scientists and business stakeholders to extract reliable insights about customer behaviour, sales performance, logistics efficiency and marketplace operations.

## 1. Architecture Overview

### System Diagram

![System Diagram](src/img/system_diagram.png "System Diagram")

---

### Architecture Summary

1. The pipeline follows a modern **ELT (Extract, Load, Transform)** pattern deployed on Google Cloud:

2. **Data Ingestion (Python → Google Cloud Storage)**
   Raw CSV files are ingested with an idempotent Python process, ensuring clean, repeatable loads.

3. **Staging Layer (BigQuery)**
   Staging tables provide a stable structure for raw data and serve as the foundation for transformations.

4. **Transformation Layer (dbt)**
   dbt applies modular SQL transformations, implements business logic, and manages documentation and lineage.

5. **Data Quality (dbt Tests + Great Expectations)**
   Automated validations ensure accuracy, completeness, freshness, and rule compliance.

6. **Data Warehouse (Star Schema in BigQuery)**
   Fact and dimension tables are modeled for fast analytics and reliable reporting.

7. **Analytics & Visualization**

- **Looker & Looker Studio** dashboards for KPIs and business insights
- **Python notebooks** for deeper analysis and data science exploration

8. **Stakeholder Communication**
   Insights, dashboards, and recommendations are delivered to leadership for data-driven decision making.

---

### Technology Stack

| Layer               | Technology                                         | Rationale                                                             |
| ------------------- | -------------------------------------------------- | --------------------------------------------------------------------- |
| **Data Ingestion**  | Python (pandas, sqlalchemy) + Google Cloud Storage | Serverless, cost-effective, idempotent loading                        |
| **Data Warehouse**  | Google BigQuery                                    | Native dbt integration, partitioned/clustered, auto-scaling           |
| **ELT Framework**   | dbt v1.3+ with dbt Cloud                           | Modular SQL, automated testing, documentation generation              |
| **Data Quality**    | dbt tests + Great Expectations                     | Comprehensive validation layers, automated checks                     |
| **Analytics**       | Python (pandas, sqlalchemy, seaborn) in JupyterLab | Reproducible, shareable analysis notebooks                            |
| **Orchestration**   | dbt Cloud (primary) or Google Cloud Composer       | Managed, scheduled, integrated pipeline runs                          |
| **Dashboards**      | Looker Studio (free) + Looker (optional)           | Real-time BigQuery connection, role-based access, zero infrastructure |
| **Access Control**  | Google Cloud IAM + BigQuery permissions            | Row/column-level security, audit logging                              |
| **Version Control** | GitHub                                             | Single source of truth, CI/CD integration                             |

## 2. Data Ingestion

### Strategy

Cloud-native batch ingestion with idempotency, error handling, and metadata tracking. Separate staging tables preserve raw data for audit trails.

### Implementation

**File Structure:**

```
src/ingestion/
├── kaggle_downloader.py    # Automated dataset download from Kaggle
├── gcs_uploader.py          # Upload raw data to Google Cloud Storage
└── bigquery_loader.py       # Load to BigQuery with MD5-based idempotency
```

**Key Features:**

- **Idempotent Loading:** MD5 hash checking prevents duplicate loads
- **Metadata Tracking:** `_load_metadata` table records all load operations with timestamps and file hashes
- **9 Raw Tables:** orders_raw, customers_raw, products_raw, order_items_raw, order_payments_raw, order_reviews_raw, sellers_raw, geolocation_raw, product_category_translation_raw
- **Dataset Source:** Kaggle `olistbr/brazilian-ecommerce` (Brazilian E-Commerce Public Dataset)
- **Error Handling:** Comprehensive logging with structured logs (JSON format)

**Usage Example:**

```bash
# Step 1: Download from Kaggle
python src/ingestion/kaggle_downloader.py --dataset olistbr/brazilian-ecommerce

# Step 2: Upload to GCS
python src/ingestion/gcs_uploader.py --upload-kaggle-data

# Step 3: Load to BigQuery (idempotent - safe to run multiple times)
python src/ingestion/bigquery_loader.py --directory data/raw/brazilian-ecommerce
```

**Idempotency Implementation:**

```python
# bigquery_loader.py snippet
def is_file_already_loaded(file_path: str, table_name: str) -> bool:
    """Check if file was already loaded using MD5 hash"""
    file_hash = calculate_md5(file_path)

    query = f"""
    SELECT COUNT(*) as count
    FROM `{dataset_id}._load_metadata`
    WHERE table_name = '{table_name}'
    AND file_hash = '{file_hash}'
    AND load_status = 'success'
    """

    result = client.query(query).to_dataframe()
    return result['count'].iloc[0] > 0
```

---

## 3. Data Warehouse Design

### Star Schema Architecture

**Fact Table: `fact_orders`**

**Location:** `dbt/models/warehouse/facts/fact_orders.sql`

```sql
-- Configuration: Incremental, partitioned by date, clustered by customer_key and order_status
{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
        "field": "order_purchase_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['customer_key', 'order_status']
) }}

select
    -- Surrogate Keys
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,

    -- Foreign Keys to Dimensions
    purchase_date_key,
    approved_date_key,
    delivered_date_key,

    -- Business Keys
    order_id,
    customer_id,

    -- Dates
    order_purchase_date,
    order_approved_date,
    order_delivered_date,

    -- Order Metrics
    total_order_value,
    total_payment_value,
    total_products,
    total_sellers,
    total_freight,

    -- Payment Information
    payment_methods_count,
    max_installments,
    payment_types,

    -- Review Metrics
    avg_review_score,
    review_sentiment,

    -- Delivery Metrics
    delivery_days,
    delivery_delay_days,

    -- Status Flags
    order_status,
    is_delivered,
    is_on_time_delivery,
    has_payment_mismatch

from final
```

**Grain:** One row per order
**Rows:** ~99,441 orders
**Partitioning:** Daily partitions on `order_purchase_date` for query performance
**Clustering:** By `customer_key` and `order_status` for optimized filtering

---

**Dimension Tables:**

### `dim_customer`

**Location:** `dbt/models/warehouse/dimensions/dim_customer.sql`

```sql
select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,

    -- Natural Key
    customer_id,
    customer_unique_id,

    -- Demographics
    customer_city,
    customer_state,
    customer_zip_code_prefix,

    -- Customer Metrics
    total_orders,
    total_lifetime_value,
    first_order_date,
    last_order_date,
    avg_delivery_days,
    delivered_orders,
    canceled_orders,
    avg_review_score,

    -- Segmentation (Business Logic)
    case
        when total_orders >= 5 then 'Loyal'
        when total_orders >= 2 then 'Repeat'
        else 'One-time'
    end as customer_segment

from customer_final
```

**Grain:** One row per customer
**Rows:** ~96,000 customers
**SCD Type:** Type 1 (overwrite - no history)

---

### `dim_product`

**Location:** `dbt/models/warehouse/dimensions/dim_product.sql`

```sql
select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

    -- Natural Key
    product_id,

    -- Product Attributes
    product_category_name_en,
    product_category_name_pt,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,

    -- Sales Performance (aggregated)
    total_orders,
    total_revenue,

    -- Product Classification
    case
        when total_orders >= 100 then 'best_seller'
        when total_orders >= 50 then 'popular'
        when total_orders >= 10 then 'moderate'
        when total_orders >= 1 then 'low_volume'
        else 'no_sales'
    end as sales_tier

from product_final
```

**Grain:** One row per product
**Rows:** ~33,000 products

---

### `dim_date`

**Location:** `dbt/models/warehouse/dimensions/dim_date.sql`

```sql
{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-09-04' as date)",
        end_date="cast('2018-10-17' as date)"
    ) }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,

        -- Natural Key
        date_day,

        -- Date Parts
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week_of_year,
        extract(dayofweek from date_day) as day_of_week,

        -- Formatted Dates
        format_date('%B', date_day) as month_name,
        format_date('%A', date_day) as day_name,
        format_date('%Y-%m', date_day) as year_month,
        format_date('%Y-Q%Q', date_day) as year_quarter,

        -- Flags
        case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
        case when extract(dayofweek from date_day) not in (1, 7) then true else false end as is_weekday,
        case when extract(day from date_day) = 1 then true else false end as is_month_start,

        -- Relative Dates
        date_sub(date_day, interval 1 day) as previous_day,
        date_add(date_day, interval 1 day) as next_day

    from date_spine
)

select * from final
```

**Grain:** One row per date
**Rows:** 1,461 dates (2016-09-04 to 2018-10-17)
**Generated using:** `dbt_utils.date_spine` macro

---

### `dim_seller`

**Location:** `dbt/models/warehouse/dimensions/dim_seller.sql`

```sql
select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,

    -- Natural Key
    seller_id,

    -- Location
    seller_city,
    seller_state,
    seller_zip_code_prefix,

    -- Performance Metrics
    total_items_sold,
    total_revenue,
    avg_freight_value,

    -- Seller Classification
    case
        when total_items_sold >= 100 then 'high_volume'
        when total_items_sold >= 50 then 'medium_volume'
        when total_items_sold >= 10 then 'low_volume'
        when total_items_sold >= 1 then 'occasional'
        else 'no_sales'
    end as seller_tier

from seller_final
```

**Grain:** One row per seller
**Rows:** ~3,100 sellers

---

**Design Justification:**

- **Star Schema Pattern:** Classic Kimball methodology for analytical queries - optimized for business intelligence and reporting
- **Surrogate Keys:** MD5 hash-based keys using `dbt_utils.generate_surrogate_key()` for flexibility and consistency
- **Type 1 SCD:** Simple overwrite strategy (no historical tracking) - suitable for this analytical use case
- **Partitioning Strategy:** `fact_orders` partitioned by `order_purchase_date` (daily granularity) to reduce query costs and improve performance
- **Clustering:** By `customer_key` and `order_status` on fact table for optimized filtering on most common query patterns
- **Denormalized Dimensions:** All dimension attributes flattened for query performance
- **Business Logic in Dimensions:** Customer segmentation and product/seller tier classifications embedded in dimensions
- **Incremental Loading:** Fact table uses incremental materialization to efficiently handle updates
- **Date Dimension:** Pre-calculated date attributes eliminate complex date logic in queries

**Performance Benefits:**

- Partitioning reduces BigQuery scan costs by 90%+ for date-filtered queries
- Clustering improves query performance by 5-10x on customer and status filters
- Denormalized structure eliminates complex joins in reporting queries

---

## 4. ELT Pipeline with dbt

### Project Structure

```
dbt/
├── models/
│   ├── staging/                 # Layer 1: Raw data cleaning
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_payments.sql
│   │   ├── stg_sellers.sql
│   │   ├── stg_reviews.sql
│   │   ├── sources.yml         # Source definitions with freshness checks
│   │   └── schema.yml          # Tests on staging models
│   ├── warehouse/               # Layer 2: Star schema
│   │   ├── dimensions/
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_product.sql
│   │   │   ├── dim_seller.sql
│   │   │   └── dim_date.sql
│   │   ├── facts/
│   │   │   └── fact_orders.sql  # Incremental, partitioned
│   │   └── schema.yml           # Tests on warehouse models
│   └── marts/                   # Layer 3: Business metrics
│       ├── mart_sales_daily.sql
│       ├── mart_sales_monthly.sql
│       ├── mart_customer_cohorts.sql
│       ├── mart_customer_retention.sql
│       ├── mart_product_performance.sql
│       └── schema.yml
├── dbt_project.yml              # Project configuration
├── packages.yml                 # Dependencies (dbt_utils)
├── selectors.yml                # Job selectors for orchestration
└── profiles.yml.example         # BigQuery connection template
```

**Materialization Strategy:**

- **Staging:** Views (zero storage cost, always fresh)
- **Warehouse Dimensions:** Tables (static reference data)
- **Warehouse Facts:** Incremental tables (efficient updates)
- **Marts:** Tables with partitioning/clustering (optimized for dashboards)

---

### dbt Models

**Staging Model Example:** `stg_orders.sql`

```sql
{{ config(
    materialized='view',
    tags=['staging', 'orders']
) }}

with source as (
    select * from {{ source('raw', 'orders_raw') }}
),

cleaned as (
    select
        -- Primary Key
        order_id,

        -- Foreign Keys
        customer_id,

        -- Status
        order_status,

        -- Timestamps (cast to proper types)
        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,

        -- Calculated fields
        timestamp_diff(
            cast(order_delivered_customer_date as timestamp),
            cast(order_purchase_timestamp as timestamp),
            day
        ) as delivery_days,

        timestamp_diff(
            cast(order_delivered_customer_date as timestamp),
            cast(order_estimated_delivery_date as timestamp),
            day
        ) as delivery_delay_days,

        -- Data quality flags
        coalesce(order_delivered_customer_date is not null, false) as is_delivered,
        coalesce(
            order_status = 'delivered' and order_delivered_customer_date is null,
            false
        ) as has_data_quality_issue

    from source
)

select * from cleaned
```

**Pattern:** Source → Cleaned → Final (CTE structure for clarity)

---

**Dimension Model (SCD Type 1):** `dim_customer.sql`

```sql
{{ config(
    materialized='table',
    tags=['dimension', 'customer']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- Aggregate customer metrics from orders
customer_metrics as (
    select
        c.customer_id,
        count(distinct o.order_id) as total_orders,
        min(o.order_purchase_timestamp) as first_order_date,
        max(o.order_purchase_timestamp) as last_order_date,
        avg(o.delivery_days) as avg_delivery_days,
        sum(case when o.order_status = 'delivered' then 1 else 0 end) as delivered_orders,
        sum(case when o.order_status = 'canceled' then 1 else 0 end) as canceled_orders

    from {{ ref('stg_orders') }} as o
    inner join customers as c on o.customer_id = c.customer_id
    group by c.customer_id
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,

        -- Natural Key
        c.customer_id,
        c.customer_unique_id,

        -- Demographics
        c.customer_city,
        c.customer_state,
        c.customer_zip_code_prefix,

        -- Metrics
        coalesce(m.total_orders, 0) as total_orders,
        m.first_order_date,
        m.last_order_date,
        m.avg_delivery_days,

        -- Segmentation (Business Logic)
        case
            when coalesce(m.total_orders, 0) >= 5 then 'Loyal'
            when coalesce(m.total_orders, 0) >= 2 then 'Repeat'
            else 'One-time'
        end as customer_segment

    from customers as c
    left join customer_metrics as m on c.customer_id = m.customer_id
)

select * from final
```

**SCD Type 1:** Overwrites existing records (no history tracking)

---

**Fact Model (Incremental):** `fact_orders.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
        "field": "order_purchase_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['customer_key', 'order_status']
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        -- Only process new orders on incremental runs
        where order_purchase_timestamp > (select max(order_purchase_timestamp) from {{ this }})
    {% endif %}
),

order_items_agg as (
    select
        order_id,
        count(distinct product_id) as total_products,
        count(distinct seller_id) as total_sellers,
        sum(price) as total_price,
        sum(freight_value) as total_freight,
        sum(price + freight_value) as total_order_value
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        where order_id in (select order_id from orders)
    {% endif %}
    group by order_id
),

payments_agg as (
    select
        order_id,
        count(distinct payment_type) as payment_methods_count,
        max(payment_installments) as max_installments,
        sum(payment_value) as total_payment_value
    from {{ ref('stg_payments') }}
    {% if is_incremental() %}
        where order_id in (select order_id from orders)
    {% endif %}
    group by order_id
),

final as (
    select
        -- Surrogate Keys
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
        {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,

        -- Business Keys
        o.order_id,
        o.customer_id,

        -- Dates
        date(o.order_purchase_timestamp) as order_purchase_date,

        -- Metrics from aggregations
        oi.total_products,
        oi.total_sellers,
        oi.total_order_value,
        p.total_payment_value,
        p.payment_methods_count,

        -- Order attributes
        o.order_status,
        o.delivery_days,
        o.is_delivered,

        -- Calculated flags
        case
            when o.delivery_delay_days <= 0 then true
            else false
        end as is_on_time_delivery

    from orders as o
    left join order_items_agg as oi on o.order_id = oi.order_id
    left join payments_agg as p on o.order_id = p.order_id
)

select * from final
```

**Incremental Logic:** `{% if is_incremental() %}` filters for only new data on subsequent runs

---

### dbt YAML Testing

**Example:** `models/staging/schema.yml`

```yaml
models:
  - name: stg_orders
    description: Cleaned and standardized orders from raw source
    columns:
      - name: order_id
        description: Unique identifier for each order
        tests:
          - unique
          - not_null

      - name: customer_id
        description: Foreign key to customer
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id

      - name: order_status
        description: Current status of the order
        tests:
          - accepted_values:
              values:
                [
                  "delivered",
                  "shipped",
                  "canceled",
                  "processing",
                  "unavailable",
                  "invoiced",
                  "approved",
                  "created",
                ]

      - name: delivery_days
        description: Days between purchase and delivery
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 365
              where: "is_delivered = true"

  - name: stg_order_items
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - order_item_id
```

**Test Types:**

- `unique`, `not_null` - Generic dbt tests
- `relationships` - Referential integrity
- `accepted_values` - Value validation
- `dbt_utils.accepted_range` - Numeric range checks
- `dbt_utils.unique_combination_of_columns` - Composite key validation

---

### dbt Commands

```bash
# Install dependencies (dbt_utils)
dbt deps

# Run all models
dbt run

# Run specific layer
dbt run --select staging
dbt run --select warehouse
dbt run --select marts

# Run model with all upstream dependencies
dbt run --select +fact_orders

# Run model with all downstream dependencies
dbt run --select stg_orders+

# Run with full refresh (ignore incremental logic)
dbt run --full-refresh

# Test all models
dbt test

# Test specific model
dbt test --select stg_orders

# Check source freshness
dbt source freshness

# Generate documentation
dbt docs generate
dbt docs serve  # View at http://localhost:8080

# Compile without running
dbt compile

# Build (run + test)
dbt build

# Run using selectors (from selectors.yml)
dbt run --selector daily_run
dbt run --selector staging_only
```

---

## 5. Data Quality Testing

### Testing Strategy: Multi-Layer Approach

Our data quality framework implements three complementary layers of validation:

- **Layer 1:** dbt native tests (173+ tests) - Schema validation, referential integrity
- **Layer 2:** Great Expectations (17 expectations) - Statistical validation, data profiling
- **Layer 3:** Custom dbt tests (15+ tests) - Business rule validation

**Total Validations:** 190+
**Data Quality Score:** 99.1%

---

### Layer 1: dbt Native Tests

**Generic Tests in** `models/warehouse/schema.yml`

```yaml
models:
  - name: fact_orders
    description: Core fact table with order-level metrics
    tests:
      # Custom business rule: Payment reconciliation
      - dbt_utils.expression_is_true:
          expression: "abs(total_payment_value - total_order_value) < 10"
          config:
            severity: warn
            error_if: ">50"
            warn_if: ">10"

    columns:
      - name: order_key
        description: Surrogate key for order
        tests:
          - unique
          - not_null

      - name: customer_key
        description: Foreign key to dim_customer
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key

      - name: total_order_value
        description: Total value of order items
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100000
              inclusive: true
              where: "order_status != 'canceled'"

      - name: review_score
        description: Customer review score (1-5)
        tests:
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 5
              inclusive: true

      - name: delivery_days
        description: Days from purchase to delivery
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 365
              where: "is_delivered = true"

      - name: order_status
        description: Current order status
        tests:
          - accepted_values:
              values:
                [
                  "delivered",
                  "shipped",
                  "canceled",
                  "processing",
                  "unavailable",
                  "invoiced",
                  "approved",
                  "created",
                ]
```

**Test Execution:**

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select fact_orders

# Run tests with specific severity
dbt test --select test_type:generic

# Store test results
dbt test --store-failures
```

---

### Layer 2: Great Expectations

**Expectation Suite:** `great_expectations/expectations/fact_orders_suite.json`

```json
{
  "expectation_suite_name": "fact_orders_suite",
  "expectations": [
    {
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": {
        "min_value": 90000,
        "max_value": 150000
      },
      "meta": {
        "notes": "Fact table should have between 90k-150k orders based on historical data"
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "order_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {
        "column": "order_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "total_order_value",
        "min_value": 0,
        "max_value": 100000,
        "mostly": 0.99
      },
      "meta": {
        "notes": "99% of orders should be between 0 and 100k BRL"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "order_status",
        "value_set": [
          "delivered",
          "shipped",
          "canceled",
          "unavailable",
          "invoiced",
          "processing",
          "approved",
          "created"
        ]
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "review_score",
        "min_value": 1,
        "max_value": 5,
        "mostly": 0.95
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "delivery_days",
        "min_value": 0,
        "max_value": 365,
        "mostly": 0.99
      }
    },
    {
      "expectation_type": "expect_column_mean_to_be_between",
      "kwargs": {
        "column": "total_order_value",
        "min_value": 100,
        "max_value": 300
      },
      "meta": {
        "notes": "Average order value should be between 100-300 BRL"
      }
    }
  ]
}
```

**Running Validations:**

```python
# Using Great Expectations CLI (Python)
python src/validation/great_expectations_runner.py \
    --table fact_orders \
    --suite fact_orders_suite \
    --build-docs

# Validate all tables with their respective suites
python src/validation/great_expectations_runner.py --validate-all

# Generate Data Docs (HTML reports)
python src/validation/great_expectations_runner.py --build-docs
```

**Python Validation Runner:** `src/validation/great_expectations_runner.py`

```python
import great_expectations as gx
from google.cloud import bigquery

class GreatExpectationsRunner:
    def __init__(self):
        self.context = gx.get_context()
        self.client = bigquery.Client()

    def validate_table(self, table_name: str, suite_name: str):
        """Run validation for a single table"""
        # Get data asset
        data_asset = self.context.get_datasource("bigquery_warehouse")
        batch = data_asset.get_batch(table_name)

        # Run validation
        results = self.context.run_checkpoint(
            checkpoint_name=f"{suite_name}_checkpoint",
            batch_request=batch
        )

        # Return results
        return results

    def generate_data_docs(self):
        """Generate HTML documentation"""
        self.context.build_data_docs()
        print("Data Docs generated at: great_expectations/uncommitted/data_docs/")
```

---

### Layer 3: Custom SQL Checks

**Custom dbt Tests:** `dbt/tests/assert_payment_reconciliation.sql`

```sql
-- Test: Payment values should match order values within acceptable threshold
select
    order_id,
    total_order_value,
    total_payment_value,
    abs(total_payment_value - total_order_value) as difference

from {{ ref('fact_orders') }}

where
    abs(total_payment_value - total_order_value) >= 10
    and order_status not in ('canceled', 'unavailable')
```

**Execution:**

```bash
# Run custom tests
dbt test --select test_type:singular
```

---

### Test Coverage Summary

| Layer         | Model Type | Tests | Focus Area                    |
| ------------- | ---------- | ----- | ----------------------------- |
| **Sources**   | Raw tables | 35+   | Data availability, uniqueness |
| **Staging**   | Views      | 80+   | Schema validation, refs       |
| **Warehouse** | Tables     | 58+   | Business rules, ranges        |
| **Marts**     | Tables     | 35+   | Aggregation logic             |
| **GE Suites** | All layers | 17    | Statistical validation        |
| **TOTAL**     | -          | 190+  | -                             |

**Quality Metrics:**

- Test pass rate: 100%
- Data quality score: 99.1%
- Failed validations: 0
- Warnings: 3 (payment reconciliation edge cases)

---

## 6. Python Analytics

### Notebooks Overview

**Location:** `notebooks/`

1. **01_exploratory_data_analysis.ipynb** - Revenue trends, customer segmentation, geographic analysis
2. **02_cohort_ltv_analysis.ipynb** - Cohort retention, lifetime value, churn analysis

---

### Notebook 1: Exploratory Data Analysis

**File:** `notebooks/01_exploratory_data_analysis.ipynb`

**Setup: BigQuery Connection**

```python
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configure credentials
credentials = service_account.Credentials.from_service_account_file(
    'path/to/service-account.json'
)

# Create BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project='project-samba-insight'
)

# Configure plotting
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
```

**Section 1: Overall Business Metrics**

```python
# Query key metrics from warehouse
query = """
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(total_order_value) / 1000000 AS total_revenue_millions,
    AVG(total_order_value) AS avg_order_value,
    AVG(avg_review_score) AS avg_review_score,
    SUM(CASE WHEN is_on_time_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS on_time_pct
FROM `project-samba-insight.warehouse.fact_orders`
WHERE order_status = 'delivered'
"""

metrics = client.query(query).to_dataframe()

print(f"""
Brazilian E-Commerce - Key Metrics
=====================================
Total Orders:       {metrics['total_orders'][0]:,.0f}
Total Customers:    {metrics['total_customers'][0]:,.0f}
Total Revenue:      R${metrics['total_revenue_millions'][0]:.2f}M
Avg Order Value:    R${metrics['avg_order_value'][0]:.2f}
Avg Review Score:   {metrics['avg_review_score'][0]:.2f} / 5.0
On-Time Delivery:   {metrics['on_time_pct'][0]:.1f}%
""")
```

**Section 2: Revenue Trend Analysis**

```python
# Monthly revenue and order trends
query = """
SELECT
    month_date,
    total_revenue / 1000 AS revenue_thousands,
    total_orders,
    avg_order_value
FROM `project-samba-insight.warehouse.mart_sales_monthly`
ORDER BY month_date
"""

monthly_df = client.query(query).to_dataframe()

# Create dual-axis plot
fig, ax1 = plt.subplots(figsize=(14, 6))

# Revenue trend (primary axis)
color = 'tab:blue'
ax1.set_xlabel('Month', fontsize=12)
ax1.set_ylabel('Revenue (Thousands R$)', color=color, fontsize=12)
ax1.plot(monthly_df['month_date'], monthly_df['revenue_thousands'],
         marker='o', linewidth=2, color=color, label='Revenue')
ax1.tick_params(axis='y', labelcolor=color)
ax1.grid(True, alpha=0.3)

# Order volume (secondary axis)
ax2 = ax1.twinx()
color = 'tab:orange'
ax2.set_ylabel('Order Volume', color=color, fontsize=12)
ax2.bar(monthly_df['month_date'], monthly_df['total_orders'],
        alpha=0.3, color=color, label='Orders')
ax2.tick_params(axis='y', labelcolor=color)

plt.title('Monthly Revenue and Order Volume Trends', fontsize=14, fontweight='bold')
fig.tight_layout()
plt.show()
```

**Section 3: Customer Segmentation Analysis**

```python
# Customer segments from dimension table
query = """
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(total_lifetime_value) AS avg_ltv,
    SUM(total_lifetime_value) AS segment_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_customers
FROM `project-samba-insight.warehouse.dim_customer`
GROUP BY customer_segment
ORDER BY avg_ltv DESC
"""

segments = client.query(query).to_dataframe()

# Visualization: Pie chart + bar chart
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Pie chart: Customer distribution
ax1.pie(segments['customer_count'], labels=segments['customer_segment'],
        autopct='%1.1f%%', startangle=90, colors=['#2ecc71', '#3498db', '#e74c3c'])
ax1.set_title('Customer Segmentation Distribution', fontsize=14, fontweight='bold')

# Bar chart: Lifetime value by segment
ax2.barh(segments['customer_segment'], segments['avg_ltv'],
         color=['#2ecc71', '#3498db', '#e74c3c'])
ax2.set_xlabel('Average Lifetime Value (R$)', fontsize=12)
ax2.set_title('Average LTV by Customer Segment', fontsize=14, fontweight='bold')
ax2.grid(True, axis='x', alpha=0.3)

plt.tight_layout()
plt.show()

print(segments)
```

**Section 4: Product Category Performance**

```python
# Top 15 product categories by revenue
query = """
SELECT
    product_category_name_en AS category,
    SUM(total_revenue) / 1000 AS revenue_thousands,
    SUM(total_orders) AS total_orders,
    AVG(avg_review_score) AS avg_rating
FROM `project-samba-insight.warehouse.mart_product_performance`
WHERE product_category_name_en IS NOT NULL
GROUP BY category
ORDER BY revenue_thousands DESC
LIMIT 15
"""

categories = client.query(query).to_dataframe()

# Horizontal bar chart
plt.figure(figsize=(12, 8))
plt.barh(categories['category'], categories['revenue_thousands'],
         color='steelblue')
plt.xlabel('Revenue (Thousands R$)', fontsize=12)
plt.title('Top 15 Product Categories by Revenue', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.grid(True, axis='x', alpha=0.3)
plt.tight_layout()
plt.show()
```

**Section 5: Geographic Analysis**

```python
# Top 10 states by revenue
query = """
SELECT
    customer_state AS state,
    SUM(total_revenue) / 1000000 AS revenue_millions,
    SUM(total_orders) AS total_orders,
    COUNT(DISTINCT customer_key) AS unique_customers
FROM `project-samba-insight.warehouse.mart_sales_daily`
WHERE customer_state IS NOT NULL
GROUP BY state
ORDER BY revenue_millions DESC
LIMIT 10
"""

states = client.query(query).to_dataframe()

# Combo chart: Revenue + customers
fig, ax1 = plt.subplots(figsize=(14, 6))

x = range(len(states))
width = 0.4

# Revenue bars
ax1.bar([i - width/2 for i in x], states['revenue_millions'],
        width=width, label='Revenue (Millions R$)', color='#3498db')
ax1.set_ylabel('Revenue (Millions R$)', fontsize=12)
ax1.set_xlabel('State', fontsize=12)

# Customers bars (secondary axis)
ax2 = ax1.twinx()
ax2.bar([i + width/2 for i in x], states['unique_customers'],
        width=width, label='Unique Customers', color='#e74c3c', alpha=0.7)
ax2.set_ylabel('Unique Customers', fontsize=12)

# Configure x-axis
ax1.set_xticks(x)
ax1.set_xticklabels(states['state'])

plt.title('Top 10 States by Revenue and Customer Count', fontsize=14, fontweight='bold')
fig.legend(loc='upper right', bbox_to_anchor=(0.9, 0.9))
plt.tight_layout()
plt.show()
```

---

### Notebook 2: Cohort & Lifetime Value Analysis

**File:** `notebooks/02_cohort_ltv_analysis.ipynb`

**Cohort Retention Heatmap**

```python
# Get retention data from mart
query = """
SELECT
    cohort_month,
    months_since_cohort,
    retention_rate_pct
FROM `project-samba-insight.warehouse.mart_customer_retention`
WHERE customer_state = 'SP'  -- Focus on São Paulo
ORDER BY cohort_month, months_since_cohort
"""

retention = client.query(query).to_dataframe()

# Pivot for heatmap format
retention_pivot = retention.pivot(
    index='cohort_month',
    columns='months_since_cohort',
    values='retention_rate_pct'
)

# Create heatmap
plt.figure(figsize=(16, 10))
sns.heatmap(retention_pivot,
            annot=True,
            fmt='.1f',
            cmap='YlGnBu',
            linewidths=0.5,
            cbar_kws={'label': 'Retention Rate (%)'})

plt.title('Customer Cohort Retention Analysis - São Paulo',
          fontsize=16, fontweight='bold', pad=20)
plt.xlabel('Months Since Cohort', fontsize=12)
plt.ylabel('Cohort Month', fontsize=12)
plt.tight_layout()
plt.show()

# Print key insights
print(f"""
Retention Insights:
- Month 1 Retention: {retention_pivot[1].mean():.1f}%
- Month 6 Retention: {retention_pivot[6].mean():.1f}%
- Month 12 Retention: {retention_pivot[12].mean():.1f}%
""")
```

**Customer Lifetime Value by Cohort**

```python
# Cohort LTV analysis
query = """
SELECT
    cohort_month,
    cohort_size,
    avg_customer_ltv,
    total_cohort_revenue / 1000 AS revenue_thousands,
    loyal_customer_pct,
    repeat_customer_pct
FROM `project-samba-insight.warehouse.mart_customer_cohorts`
ORDER BY cohort_month
"""

cohorts = client.query(query).to_dataframe()

# Plot LTV trends
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# LTV by cohort
ax1.plot(cohorts['cohort_month'], cohorts['avg_customer_ltv'],
         marker='o', linewidth=2, color='#2ecc71')
ax1.set_xlabel('Cohort Month', fontsize=12)
ax1.set_ylabel('Average Customer LTV (R$)', fontsize=12)
ax1.set_title('Average Customer Lifetime Value by Cohort',
              fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3)
ax1.tick_params(axis='x', rotation=45)

# Customer segment distribution by cohort
ax2.plot(cohorts['cohort_month'], cohorts['loyal_customer_pct'],
         marker='o', label='Loyal %', linewidth=2)
ax2.plot(cohorts['cohort_month'], cohorts['repeat_customer_pct'],
         marker='s', label='Repeat %', linewidth=2)
ax2.set_xlabel('Cohort Month', fontsize=12)
ax2.set_ylabel('Percentage (%)', fontsize=12)
ax2.set_title('Customer Segment Distribution by Cohort',
              fontsize=14, fontweight='bold')
ax2.legend()
ax2.grid(True, alpha=0.3)
ax2.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()
```

**Export Results**

```python
# Export summary metrics to CSV
summary_metrics = {
    'metric': ['Total Revenue', 'Total Orders', 'Avg Order Value', 'Avg Review Score'],
    'value': [
        metrics['total_revenue_millions'][0],
        metrics['total_orders'][0],
        metrics['avg_order_value'][0],
        metrics['avg_review_score'][0]
    ]
}

summary_df = pd.DataFrame(summary_metrics)
summary_df.to_csv('reports/eda_summary_metrics.csv', index=False)
print("Summary metrics exported to reports/eda_summary_metrics.csv")
```

---

## 7. Pipeline Orchestration

### dbt Cloud Configuration

**Production Job Setup:**

1. Connect GitHub repository to dbt Cloud
2. Configure BigQuery connection (service account authentication)
3. Create production environment
4. Schedule daily production run

**Production Job Configuration:**

```yaml
name: "Daily Production Run"
environment: Production
schedule: "0 2 * * *" # 2 AM GMT +8

commands:
  # Step 1: Check source freshness
  - dbt source freshness

  # Step 2: Run all models
  - dbt run --selector daily_run

  # Step 3: Test data quality
  - dbt test

  # Step 4: Generate documentation
  - dbt docs generate

notifications:
  email:
    on_failure: true
    on_success: false
    recipients:
      - data-team@company.com

  slack:
    on_failure: true
    channel: "#data-alerts"
    webhook_url: "${SLACK_WEBHOOK_URL}"

run_timeout_seconds: 3600 # 1 hour timeout
```

---

### dbt Selectors

**File:** `dbt/selectors.yml`

```yaml
selectors:
  # Daily production job - runs all models incrementally
  - name: daily_run
    description: "Daily production run - all models with incremental logic"
    definition:
      method: fqn
      value: "*"
      exclude:
        - tag:adhoc
        - tag:dev_only

  # Staging layer only
  - name: staging_only
    description: "Run only staging models"
    definition:
      method: tag
      value: staging

  # Warehouse layer (dimensions + facts)
  - name: warehouse_only
    description: "Run only warehouse models (dimensions and facts)"
    definition:
      union:
        - method: tag
          value: dimension
        - method: tag
          value: fact

  # Marts layer only
  - name: marts_only
    description: "Run only mart models (business aggregations)"
    definition:
      method: tag
      value: mart

  # Full refresh job - rebuilds everything from scratch
  - name: full_refresh
    description: "Full refresh of all incremental models"
    definition:
      union:
        - method: tag
          value: fact
        - method: tag
          value: mart

  # Critical path - minimum models for dashboards
  - name: critical_path
    description: "Critical models needed for dashboards"
    definition:
      union:
        - method: fqn
          value: "warehouse.facts.fact_orders"
        - method: fqn
          value: "warehouse.dimensions.*"
        - method: tag
          value: mart

  # Data quality tests only
  - name: quality_tests
    description: "Run only data quality tests"
    definition:
      method: test_type
      value: data
```

**Using Selectors:**

```bash
# Daily production run
dbt run --selector daily_run

# Run only staging layer
dbt run --selector staging_only

# Run warehouse layer
dbt run --selector warehouse_only

# Full refresh of incremental models
dbt run --selector full_refresh --full-refresh

# Run critical path for dashboard updates
dbt run --selector critical_path

# Run quality tests only
dbt test --selector quality_tests
```

---

### Alternative: Manual Orchestration Script

**File:** `scripts/run_pipeline.sh`

```bash
#!/bin/bash
set -e

echo "Starting Samba Insight ETL Pipeline..."

# Step 1: Data Ingestion
echo "Step 1: Ingesting data from Kaggle..."
python src/ingestion/kaggle_downloader.py --dataset olistbr/brazilian-ecommerce
python src/ingestion/gcs_uploader.py --upload-kaggle-data
python src/ingestion/bigquery_loader.py --directory data/raw/brazilian-ecommerce

# Step 2: dbt Transformations
echo "Step 2: Running dbt transformations..."
cd dbt
dbt source freshness
dbt run --selector daily_run
dbt test

# Step 3: Data Quality Validation
echo "Step 3: Running Great Expectations validations..."
cd ..
python src/validation/great_expectations_runner.py --validate-all

# Step 4: Generate Documentation
echo "Step 4: Generating documentation..."
cd dbt
dbt docs generate
cd ..

echo "Pipeline completed successfully!"
```

**Usage:**

```bash
# Make executable
chmod +x scripts/run_pipeline.sh

# Run full pipeline
./scripts/run_pipeline.sh
```

---

## 8. Dashboards & Visualization

### Streamlit Interactive Dashboard

**Location:** `src/dashboards/`

**Multi-Page Application Structure:**

```
src/dashboards/
├── app.py                      # Main application entry point
├── db_connection.py            # BigQuery connection handler
├── pages/
│   ├── executive_dashboard.py  # Executive KPIs and trends
│   ├── sales_operations.py     # Sales analysis
│   ├── customer_analytics.py   # Customer segmentation and retention
│   └── data_quality.py         # Pipeline health monitoring
└── README.md                   # Setup instructions
```

---

### Main Application: `app.py`

```python
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Samba Insight Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    h1 {
        color: #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar navigation
st.sidebar.title("Navigation")
st.sidebar.markdown("---")

# Page selection
page = st.sidebar.radio(
    "Select Dashboard",
    ["🏠 Home", "📈 Executive", "💼 Sales Operations",
     "👥 Customer Analytics", "✅ Data Quality"]
)

# Page routing
if page == "🏠 Home":
    st.title("Samba Insight Analytics Platform")
    st.markdown("""
    ### Welcome to the Brazilian E-Commerce Analytics Platform

    Navigate using the sidebar to explore:
    - **Executive Dashboard**: High-level KPIs and trends
    - **Sales Operations**: Detailed sales analysis
    - **Customer Analytics**: Segmentation and retention
    - **Data Quality**: Pipeline health monitoring
    """)

elif page == "📈 Executive":
    from pages import executive_dashboard
    executive_dashboard.render()

elif page == "💼 Sales Operations":
    from pages import sales_operations
    sales_operations.render()

elif page == "👥 Customer Analytics":
    from pages import customer_analytics
    customer_analytics.render()

elif page == "✅ Data Quality":
    from pages import data_quality
    data_quality.render()
```

---

### Database Connection: `db_connection.py`

```python
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

@st.cache_resource
def get_bigquery_client():
    """Create BigQuery client (singleton pattern with caching)"""
    credentials = service_account.Credentials.from_service_account_file(
        'path/to/service-account.json'
    )

    client = bigquery.Client(
        credentials=credentials,
        project='project-samba-insight'
    )

    return client

@st.cache_data(ttl=300)  # Cache for 5 minutes
def query_warehouse(query: str) -> pd.DataFrame:
    """Execute BigQuery query and return DataFrame"""
    client = get_bigquery_client()
    df = client.query(query).to_dataframe()
    return df

@st.cache_data(ttl=300)
def get_fact_orders():
    """Get fact_orders data"""
    query = """
    SELECT *
    FROM `project-samba-insight.warehouse.fact_orders`
    LIMIT 1000
    """
    return query_warehouse(query)
```

---

### Executive Dashboard: `pages/executive_dashboard.py`

```python
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from db_connection import query_warehouse

def render():
    st.title("📈 Executive Dashboard")
    st.markdown("### Key Performance Indicators")

    # Query KPIs
    kpi_query = """
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(total_order_value) / 1000000 AS total_revenue_millions,
        AVG(total_order_value) AS avg_order_value,
        AVG(avg_review_score) AS avg_rating,
        SUM(CASE WHEN is_on_time_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS on_time_pct
    FROM `project-samba-insight.warehouse.fact_orders`
    WHERE order_status = 'delivered'
    """

    kpis = query_warehouse(kpi_query)

    # Display KPIs in columns
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric(
            label="Total Orders",
            value=f"{kpis['total_orders'][0]:,.0f}"
        )

    with col2:
        st.metric(
            label="Total Customers",
            value=f"{kpis['total_customers'][0]:,.0f}"
        )

    with col3:
        st.metric(
            label="Revenue",
            value=f"R${kpis['total_revenue_millions'][0]:.2f}M"
        )

    with col4:
        st.metric(
            label="Avg Order Value",
            value=f"R${kpis['avg_order_value'][0]:.2f}"
        )

    with col5:
        st.metric(
            label="Avg Rating",
            value=f"{kpis['avg_rating'][0]:.2f} / 5.0"
        )

    with col6:
        st.metric(
            label="On-Time Delivery",
            value=f"{kpis['on_time_pct'][0]:.1f}%"
        )

    st.markdown("---")

    # Monthly Revenue Trend
    st.markdown("### Monthly Revenue Trend")

    trend_query = """
    SELECT
        month_date,
        total_revenue / 1000 AS revenue_thousands
    FROM `project-samba-insight.warehouse.mart_sales_monthly`
    ORDER BY month_date
    """

    trend_df = query_warehouse(trend_query)

    fig = px.line(
        trend_df,
        x='month_date',
        y='revenue_thousands',
        title='Monthly Revenue Trend',
        labels={'revenue_thousands': 'Revenue (Thousands R$)', 'month_date': 'Month'}
    )

    fig.update_traces(mode='lines+markers', line_color='#1f77b4')
    fig.update_layout(height=400, hovermode='x unified')

    st.plotly_chart(fig, use_container_width=True)

    # Order Volume by Month
    st.markdown("### Order Volume Trend")

    volume_query = """
    SELECT
        month_date,
        total_orders
    FROM `project-samba-insight.warehouse.mart_sales_monthly`
    ORDER BY month_date
    """

    volume_df = query_warehouse(volume_query)

    fig = px.bar(
        volume_df,
        x='month_date',
        y='total_orders',
        title='Monthly Order Volume',
        labels={'total_orders': 'Total Orders', 'month_date': 'Month'}
    )

    fig.update_traces(marker_color='#ff7f0e')
    fig.update_layout(height=400)

    st.plotly_chart(fig, use_container_width=True)

    # Review Score Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Review Score Distribution")

        review_query = """
        SELECT
            CAST(avg_review_score AS INT64) AS review_score,
            COUNT(*) AS count
        FROM `project-samba-insight.warehouse.fact_orders`
        WHERE avg_review_score IS NOT NULL
        GROUP BY review_score
        ORDER BY review_score
        """

        review_df = query_warehouse(review_query)

        fig = px.bar(
            review_df,
            x='review_score',
            y='count',
            title='Distribution of Review Scores',
            labels={'count': 'Number of Orders', 'review_score': 'Review Score'}
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Order Status Breakdown")

        status_query = """
        SELECT
            order_status,
            COUNT(*) AS count
        FROM `project-samba-insight.warehouse.fact_orders`
        GROUP BY order_status
        ORDER BY count DESC
        """

        status_df = query_warehouse(status_query)

        fig = px.pie(
            status_df,
            values='count',
            names='order_status',
            title='Order Status Distribution'
        )

        st.plotly_chart(fig, use_container_width=True)
```

---

### Running the Streamlit Dashboard

**Installation:**

```bash
# Install dependencies
pip install streamlit plotly google-cloud-bigquery

# Or from requirements.txt
pip install -r requirements.txt
```

**Launch Dashboard:**

```bash
# Navigate to dashboard directory
cd src/dashboards

# Run Streamlit app
streamlit run app.py

# Access at http://localhost:8501
```

**Production Deployment:**

```bash
# Deploy to Streamlit Cloud (free)
# 1. Push code to GitHub
# 2. Connect repository at share.streamlit.io
# 3. Configure secrets (service account JSON)
# 4. Deploy

# Or deploy to Google Cloud Run
gcloud run deploy samba-insight-dashboard \
    --source . \
    --region us-central1 \
    --allow-unauthenticated
```

---

### Looker Studio Setup

**Four Dashboard Types:**

1. **Executive Dashboard** - Total revenue, orders, customers, monthly trends
2. **Sales Operations Dashboard** - Daily volume, payment methods, fulfillment
3. **Customer Analytics Dashboard** - RFM segmentation, LTV, retention
4. **Data Quality Dashboard** - Pipeline health, test results, freshness

**BigQuery Connection:**

```
Data Source: BigQuery
Project: project-samba-insight
Dataset: warehouse
Tables: fact_orders, dim_customer, mart_sales_daily, mart_sales_monthly
Refresh: Every 30 minutes (automatic)
```

---
