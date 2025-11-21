# Project Samba Insight - Implementation Runbook

**Version:** 1.0
**Last Updated:** 2025-11-20
**Project:** Brazil E-Commerce Analytics Platform

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Environment Setup](#3-environment-setup)
4. [Data Ingestion](#4-data-ingestion)
5. [dbt Project Configuration](#5-dbt-project-configuration)
6. [Building the Data Pipeline](#6-building-the-data-pipeline)
7. [Testing & Validation](#7-testing--validation)
8. [Documentation](#8-documentation)
9. [Deployment](#9-deployment)
10. [Monitoring & Maintenance](#10-monitoring--maintenance)
11. [Troubleshooting](#11-troubleshooting)
12. [Appendices](#12-appendices)

---

## 1. Overview

### 1.1 Project Summary

**Project Samba Insight** is a complete ELT (Extract, Load, Transform) analytics platform for Brazilian e-commerce data, implementing:

- **3-Layer Architecture:** Staging → Warehouse (Star Schema) → Marts
- **6 New Executive Marts:** Retention, Geographic Performance, Unit Economics, Churn Prediction, Category Performance, Executive Summary
- **190+ Data Quality Tests:** dbt tests + Great Expectations
- **~99K Orders** from 2016-2018 across 96K customers and 33K products

### 1.2 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    RAW DATA SOURCES                     │
│              (Kaggle → GCS → BigQuery)                  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                   STAGING LAYER                         │
│   (stg_orders, stg_customers, stg_products, etc.)      │
│                   Materialized: VIEWS                   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                  WAREHOUSE LAYER                        │
│                                                         │
│   DIMENSIONS (Tables)        FACTS (Incremental)       │
│   • dim_customer            • fact_orders              │
│   • dim_product             (Partitioned by date)      │
│   • dim_seller                                         │
│   • dim_date                                           │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                    MARTS LAYER                          │
│              (Business Analytics)                       │
│                                                         │
│  EXISTING (5):                                          │
│  • mart_sales_daily         • mart_sales_monthly       │
│  • mart_customer_cohorts    • mart_customer_retention  │
│  • mart_product_performance                            │
│                                                         │
│  NEW EXECUTIVE ANALYTICS (6):                          │
│  • mart_customer_retention_quarterly                   │
│  • mart_geographic_performance                         │
│  • mart_unit_economics                                 │
│  • mart_churn_prediction                               │
│  • mart_category_performance                           │
│  • mart_executive_summary                              │
└─────────────────────────────────────────────────────────┘
```

### 1.3 Technology Stack

| Component            | Technology                     | Purpose              |
| -------------------- | ------------------------------ | -------------------- |
| **Data Warehouse**   | Google BigQuery                | Cloud data warehouse |
| **Transformation**   | dbt v1.3+                      | ELT framework        |
| **Data Quality**     | dbt tests + Great Expectations | Validation           |
| **Orchestration**    | dbt Cloud / Airflow            | Scheduling           |
| **Storage**          | Google Cloud Storage           | Raw data staging     |
| **Version Control**  | Git / GitHub                   | Code management      |
| **Documentation**    | dbt docs                       | Auto-generated docs  |
| **BI/Visualization** | Looker Studio / Looker         | Dashboards           |

---

## 2. Prerequisites

### 2.1 Access Requirements

- ✅ Google Cloud Platform (GCP) project with billing enabled
- ✅ BigQuery API enabled
- ✅ Cloud Storage API enabled
- ✅ IAM permissions:
  - `BigQuery Admin` or `BigQuery Data Editor`
  - `BigQuery Job User`
  - `Storage Object Admin` (for GCS)
- ✅ GitHub account (for version control)
- ✅ dbt Cloud account (recommended) OR local dbt installation

### 2.2 Software Requirements

**Local Development:**

```bash
# Python 3.8+
python --version  # Should be 3.8 or higher

# pip (Python package manager)
pip --version

# Git
git --version

# Google Cloud SDK (gcloud CLI)
gcloud --version

# dbt (if running locally)
dbt --version  # Should be 1.3.0 or higher
```

**Python Packages:**

```bash
# Core packages
pip install dbt-bigquery==1.3.0
pip install pandas==1.5.0
pip install google-cloud-bigquery==3.3.0
pip install google-cloud-storage==2.5.0
pip install great-expectations==0.15.0

# Optional - for data ingestion
pip install kaggle==1.5.12
```

### 2.3 GCP Setup Checklist

```bash
# 1. Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# 2. Set your project
gcloud config set project YOUR_PROJECT_ID

# 3. Enable required APIs
gcloud services enable bigquery.googleapis.com
gcloud services enable storage-api.googleapis.com

# 4. Create BigQuery datasets
bq mk --dataset --location=US YOUR_PROJECT_ID:raw
bq mk --dataset --location=US YOUR_PROJECT_ID:staging
bq mk --dataset --location=US YOUR_PROJECT_ID:warehouse

# 5. Create GCS bucket for raw data
gsutil mb -l US gs://YOUR_PROJECT_ID-raw-data/
```

---

## 3. Environment Setup

### 3.1 Clone the Repository

```bash
# Clone the project
cd ~/Projects/NTU
git clone https://github.com/azniosman/project-samba-insight.git
cd project-samba-insight
```

### 3.2 Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Navigate to dbt directory
cd dbt

# Install dbt packages (dbt_utils)
dbt deps
```

**Verify Installation:**

```bash
dbt --version

# Expected output:
# installed version: 1.3.0
# latest version: 1.3.0
```

### 3.3 Configure dbt Profile

**Option A: Local Development**

Create `~/.dbt/profiles.yml`:

```yaml
samba_insight:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: YOUR_PROJECT_ID
      dataset: warehouse
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive

    prod:
      type: bigquery
      method: service-account
      project: YOUR_PROJECT_ID
      dataset: warehouse
      threads: 8
      timeout_seconds: 600
      location: US
      priority: batch
      keyfile: /path/to/service-account-key.json
```

**Option B: dbt Cloud**

1. Log in to [dbt Cloud](https://cloud.getdbt.com)
2. Create new project: "Samba Insight"
3. Connect to BigQuery:
   - Upload service account JSON key
   - Set dataset to `warehouse`
   - Set location to `US`
4. Connect to GitHub repository

**Test Connection:**

```bash
dbt debug

# Expected output:
# All checks passed!
```

---

## 4. Data Ingestion

### 4.1 Download Raw Data from Kaggle

**Prerequisites:**

- Kaggle account
- Kaggle API credentials (`~/.kaggle/kaggle.json`)

```bash
# Navigate to ingestion directory
cd src/ingestion

# Download dataset
python kaggle_downloader.py --dataset olistbr/brazilian-ecommerce

# Expected output:
# ✓ Downloaded: olist_customers_dataset.csv (199 KB)
# ✓ Downloaded: olist_orders_dataset.csv (3.6 MB)
# ✓ Downloaded: olist_order_items_dataset.csv (13.9 MB)
# ... (9 files total)
```

**Files Downloaded:**

- `olist_customers_dataset.csv` (~96K customers)
- `olist_orders_dataset.csv` (~99K orders)
- `olist_order_items_dataset.csv` (~112K items)
- `olist_order_payments_dataset.csv` (~103K payments)
- `olist_products_dataset.csv` (~33K products)
- `olist_sellers_dataset.csv` (~3K sellers)
- `olist_order_reviews_dataset.csv` (~99K reviews)
- `olist_geolocation_dataset.csv` (~1M geolocation records)
- `product_category_name_translation.csv` (~71 categories)

### 4.2 Upload to Google Cloud Storage

```bash
# Upload raw CSVs to GCS
python gcs_uploader.py \
  --bucket YOUR_PROJECT_ID-raw-data \
  --source-dir ../../data/raw/brazilian-ecommerce \
  --destination-folder raw

# Expected output:
# ✓ Uploaded: olist_customers_dataset.csv → gs://bucket/raw/
# ✓ Uploaded: olist_orders_dataset.csv → gs://bucket/raw/
# ... (9 files uploaded)
```

**Verify Upload:**

```bash
gsutil ls gs://YOUR_PROJECT_ID-raw-data/raw/

# Expected output:
# gs://YOUR_PROJECT_ID-raw-data/raw/olist_customers_dataset.csv
# gs://YOUR_PROJECT_ID-raw-data/raw/olist_orders_dataset.csv
# ...
```

### 4.3 Load to BigQuery (Idempotent)

```bash
# Load all CSVs to BigQuery raw dataset
python bigquery_loader.py --directory ../../data/raw/brazilian-ecommerce --dataset raw

# Expected output:
# ✓ Loaded: olist_customers_dataset.csv → raw.customers_raw (96,096 rows)
# ✓ Loaded: olist_orders_dataset.csv → raw.orders_raw (99,441 rows)
# ... (9 tables created)
#
# Idempotency: All files tracked in raw._load_metadata
```

**IMPORTANT:** This process is **idempotent** - safe to run multiple times. Files are tracked by MD5 hash in `_load_metadata` table.

**Verify Loading:**

```bash
# Check row counts
bq query --use_legacy_sql=false \
  "SELECT 'customers' AS table_name, COUNT(*) AS \`rows\` FROM \`raw.customers_raw\`
  UNION ALL SELECT 'orders', COUNT(*) FROM \`raw.orders_raw\`
  UNION ALL SELECT 'order_items', COUNT(*) FROM \`raw.order_items_raw\`
  UNION ALL SELECT 'products', COUNT(*) FROM \`raw.products_raw\`
  UNION ALL SELECT 'sellers', COUNT(*) FROM \`raw.sellers_raw\`
  UNION ALL SELECT 'payments', COUNT(*) FROM \`raw.order_payments_raw\`
  UNION ALL SELECT 'reviews', COUNT(*) FROM \`raw.order_reviews_raw\`"

# Expected output:
# +--------------+--------+
# | table_name   | rows   |
# +--------------+--------+
# | customers    | 96096  |
# | orders       | 99441  |
# | order_items  | 112650 |
# | products     | 32951  |
# | sellers      | 3095   |
# | payments     | 103886 |
# | reviews      | 99224  |
# +--------------+--------+
```

---

## 5. dbt Project Configuration

### 5.1 Project Structure Overview

```
dbt/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt_utils dependency
├── profiles.yml.example     # Connection template
├── selectors.yml            # Job selectors
├── models/
│   ├── staging/
│   │   ├── sources.yml      # Source definitions
│   │   ├── schema.yml       # Staging tests
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_payments.sql
│   │   ├── stg_sellers.sql
│   │   └── stg_reviews.sql
│   ├── warehouse/
│   │   ├── schema.yml       # Warehouse tests
│   │   ├── dimensions/
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_product.sql
│   │   │   ├── dim_seller.sql
│   │   │   └── dim_date.sql
│   │   └── facts/
│   │       └── fact_orders.sql
│   └── marts/
│       ├── schema.yml       # Mart tests & documentation
│       ├── README.md        # Mart documentation
│       │
│       # Existing marts
│       ├── mart_sales_daily.sql
│       ├── mart_sales_monthly.sql
│       ├── mart_customer_cohorts.sql
│       ├── mart_customer_retention.sql
│       ├── mart_product_performance.sql
│       │
│       # NEW executive marts
│       ├── mart_customer_retention_quarterly.sql
│       ├── mart_geographic_performance.sql
│       ├── mart_unit_economics.sql
│       ├── mart_churn_prediction.sql
│       ├── mart_category_performance.sql
│       └── mart_executive_summary.sql
├── tests/
│   └── data_quality/
│       └── assert_data_freshness_30_days.sql
├── macros/
│   └── (custom macros if needed)
└── docs/
    └── (additional documentation)
```

### 5.2 Key Configuration Files

**dbt_project.yml** - Already configured with:

- Materialization strategies (views for staging, tables/incremental for warehouse)
- Schema naming (`staging`, `warehouse`)
- Tags for selective runs
- Variables for date ranges and thresholds

**packages.yml** - Dependency on dbt_utils:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.0.0
```

### 5.3 Source Configuration

Review `models/staging/sources.yml` to ensure source tables are correctly defined:

```yaml
version: 2

sources:
  - name: raw
    database: YOUR_PROJECT_ID
    schema: raw

    # Freshness check - data should be loaded recently
    freshness:
      warn_after: { count: 7, period: day }
      error_after: { count: 30, period: day }

    tables:
      - name: orders_raw
        identifier: orders_raw
        columns:
          - name: order_id
            tests:
              - unique
              - not_null

      - name: customers_raw
        identifier: customers_raw

      - name: products_raw
        identifier: products_raw

      # ... (additional source tables)
```

---

## 6. Building the Data Pipeline

### 6.1 Build Strategy

The pipeline builds in **3 sequential layers**:

1. **Staging** (5-10 seconds) - Clean and standardize raw data
2. **Warehouse** (30-60 seconds) - Build star schema (dimensions + facts)
3. **Marts** (60-120 seconds) - Build business analytics models

**Total Build Time:** ~2-3 minutes for full refresh

### 6.2 Step-by-Step Build Process

#### Step 1: Build Staging Layer

```bash
cd dbt

# Build all staging models
dbt run --select staging

# Expected output:
# Running with dbt=1.3.0
# Found 18 models, 173 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 9 sources
#
# Completed successfully
#
# Done. PASS=7 WARN=0 ERROR=0 SKIP=0 TOTAL=7
```

**Verify Staging Models:**

```bash
# Check staging models were created as views
bq ls --project_id=YOUR_PROJECT_ID staging

# Expected: stg_orders, stg_customers, stg_products, etc. (all views)
```

#### Step 2: Build Warehouse Layer

```bash
# Build dimensions first
dbt run --select warehouse.dimensions

# Expected output:
# Completed successfully
# Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4
#
# • dim_customer (96,096 rows)
# • dim_product (32,951 rows)
# • dim_seller (3,095 rows)
# • dim_date (1,461 rows)

# Build facts (incremental)
dbt run --select warehouse.facts

# Expected output:
# Completed successfully
# Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
#
# • fact_orders (99,441 rows)
```

**Verify Warehouse Tables:**

```bash
# Check table sizes and types
bq ls --format=pretty --project_id=YOUR_PROJECT_ID warehouse

# Expected:
# +--------------------+-------+----------------+-------------+
# | tableId           | Type  | Labels         | Time Parti  |
# +--------------------+-------+----------------+-------------+
# | dim_customer       | TABLE |                |             |
# | dim_product        | TABLE |                |             |
# | dim_seller         | TABLE |                |             |
# | dim_date           | TABLE |                |             |
# | fact_orders        | TABLE | dimension:date | DAY         |
# +--------------------+-------+----------------+-------------+
```

#### Step 3: Build Existing Marts

```bash
# Build existing marts (5 models)
dbt run --select marts --exclude tag:executive

# Expected output:
# Completed successfully
# Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
#
# • mart_sales_daily
# • mart_sales_monthly
# • mart_customer_cohorts
# • mart_customer_retention
# • mart_product_performance
```

#### Step 4: Build NEW Executive Marts

```bash
# Build the 6 new executive analytics marts
dbt run --select tag:executive

# Expected output:
# Completed successfully
# Done. PASS=6 WARN=0 ERROR=0 SKIP=0 TOTAL=6
#
# NEW MODELS CREATED:
# • mart_customer_retention_quarterly
# • mart_geographic_performance
# • mart_unit_economics
# • mart_churn_prediction
# • mart_category_performance
# • mart_executive_summary
```

**Verify Mart Creation:**

```bash
# Check all marts exist
bq ls warehouse | grep mart_

# Expected output (11 total marts):
# mart_sales_daily
# mart_sales_monthly
# mart_customer_cohorts
# mart_customer_retention
# mart_product_performance
# mart_customer_retention_quarterly     ← NEW
# mart_geographic_performance           ← NEW
# mart_unit_economics                   ← NEW
# mart_churn_prediction                 ← NEW
# mart_category_performance             ← NEW
# mart_executive_summary                ← NEW
```

### 6.3 Alternative Build Commands

**Full Build (All Layers):**

```bash
# Build everything in dependency order
dbt build

# Or just run models (no tests)
dbt run
```

**Selective Builds:**

```bash
# Build a specific model and its dependencies
dbt run --select +mart_executive_summary

# Build a specific model and its children
dbt run --select fact_orders+

# Build by tag
dbt run --select tag:executive
dbt run --select tag:customer

# Build with full refresh (ignore incremental logic)
dbt run --select fact_orders --full-refresh
```

**Using Selectors (from selectors.yml):**

```bash
# Run daily production job
dbt run --selector daily_run

# Run only staging layer
dbt run --selector staging_only

# Run warehouse layer
dbt run --selector warehouse_only
```

### 6.4 Monitoring Build Progress

**Check dbt Logs:**

```bash
# View logs in real-time
tail -f logs/dbt.log

# View last run summary
cat logs/dbt.log | grep "Completed"
```

**BigQuery Console:**

- Navigate to BigQuery in GCP Console
- Check **Job History** for query execution times
- Monitor **slot usage** and costs

---

## 7. Testing & Validation

### 7.1 Testing Strategy

```
┌─────────────────────────────────────────────────┐
│            DATA QUALITY FRAMEWORK               │
│                                                 │
│  Layer 1: dbt Generic Tests (173+)            │
│  → Schema validation, referential integrity    │
│                                                 │
│  Layer 2: dbt Singular Tests (15+)            │
│  → Business rule validation                    │
│                                                 │
│  Layer 3: Great Expectations (17+)            │
│  → Statistical validation, data profiling      │
└─────────────────────────────────────────────────┘
```

### 7.2 Run dbt Tests

#### Test All Models

```bash
# Run all tests (generic + singular)
dbt test

# Expected output:
# Running with dbt=1.3.0
# Found 18 models, 173 tests, 0 snapshots
#
# Completed successfully
# Done. PASS=173 WARN=0 ERROR=0 SKIP=0 TOTAL=173
```

#### Test by Layer

```bash
# Test staging models
dbt test --select staging
# Expected: ~80 tests pass

# Test warehouse models
dbt test --select warehouse
# Expected: ~58 tests pass

# Test marts
dbt test --select marts
# Expected: ~35 tests pass

# Test ONLY new executive marts
dbt test --select tag:executive
# Expected: ~30 tests pass
```

#### Test Specific Models

```bash
# Test a single model
dbt test --select mart_executive_summary

# Test relationships and foreign keys
dbt test --select test_type:relationships

# Test unique constraints
dbt test --select test_type:unique

# Test not null constraints
dbt test --select test_type:not_null
```

### 7.3 Critical Tests to Verify

**Fact Table Integrity:**

```bash
dbt test --select fact_orders

# Verify:
# ✓ order_id is unique
# ✓ customer_key references dim_customer
# ✓ total_order_value >= 0
# ✓ delivery_days within acceptable range
# ✓ order_status in accepted values
```

**Executive Mart Tests:**

```bash
# Retention metrics
dbt test --select mart_customer_retention_quarterly

# Verify:
# ✓ Unique combination of cohort/state/city/quarters
# ✓ retention_rate_pct between 0-100
# ✓ ltv_to_date >= 0

# Churn prediction
dbt test --select mart_churn_prediction

# Verify:
# ✓ customer_key is unique
# ✓ churn_risk_score between 0-100
# ✓ churn_status in accepted values
# ✓ churn_probability between 0-1

# Executive summary
dbt test --select mart_executive_summary

# Verify:
# ✓ quarter_start is unique
# ✓ total_revenue >= 0
# ✓ strategic_health_score between 0-100
# ✓ retention rates within bounds
```

### 7.4 Store Test Failures

```bash
# Store failures for investigation
dbt test --store-failures

# Check failures in BigQuery
bq query --use_legacy_sql=false \
"SELECT * FROM \`YOUR_PROJECT_ID.warehouse.test_failures\` LIMIT 10"
```

### 7.5 Source Freshness Check

```bash
# Check if raw data is recent
dbt source freshness

# Expected output:
# Running with dbt=1.3.0
#
# Freshness check results:
#
# ✓ PASS - raw.orders_raw (loaded 2 days ago)
# ✓ PASS - raw.customers_raw (loaded 2 days ago)
# ⚠ WARN - raw.reviews_raw (loaded 10 days ago)
```

### 7.6 Data Quality Assertions

**Run Custom Tests:**

```bash
# Run all singular tests
dbt test --select test_type:singular

# Example tests in tests/data_quality/:
# • assert_data_freshness_30_days.sql
# • assert_payment_reconciliation.sql (if exists)
# • assert_no_future_dates.sql (if exists)
```

### 7.7 Great Expectations (Optional Advanced)

**Setup:**

```bash
# Navigate to project root
cd /Users/azni/Projects/NTU/project-samba-insight

# Initialize Great Expectations
great_expectations init

# Create expectation suite for executive summary
great_expectations suite new
```

**Run Validations:**

```python
# Python script: src/validation/validate_executive_marts.py
import great_expectations as gx

context = gx.get_context()

# Validate mart_executive_summary
results = context.run_checkpoint(
    checkpoint_name="executive_summary_checkpoint"
)

# Generate data docs
context.build_data_docs()

print(f"Validation Success: {results.success}")
```

---

## 8. Documentation

### 8.1 Generate dbt Documentation

```bash
# Generate documentation
dbt docs generate

# Expected output:
# Running with dbt=1.3.0
# Building catalog
# Catalog written to /Users/.../target/catalog.json
#
# Done. Generated documentation for 18 models, 173 tests, 0 snapshots
```

### 8.2 View Documentation Locally

```bash
# Serve documentation on localhost:8080
dbt docs serve

# Expected output:
# Serving docs at http://127.0.0.1:8080
# Press Ctrl+C to exit
```

**Navigate to:** http://localhost:8080

**Documentation Includes:**

- **Project Overview:** Architecture and layer descriptions
- **Model Lineage:** DAG visualization showing dependencies
- **Column Descriptions:** Detailed column-level documentation
- **Test Coverage:** All tests documented per model
- **Source Freshness:** Raw data staleness indicators

### 8.3 Model Documentation Best Practices

All models should have documentation in `schema.yml`:

```yaml
models:
  - name: mart_executive_summary
    description: |
      Executive KPI summary consolidating all critical metrics.
      Single source of truth for C-suite dashboard.

      **Update Frequency:** Daily at 3 AM
      **Owner:** Data Analytics Team
      **Consumers:** Executive Leadership, Finance

    columns:
      - name: quarter_start
        description: First day of quarter

      - name: strategic_health_score
        description: |
          Overall business health score (0-100) calculated from:
          - Revenue growth (20 points)
          - Retention rate (25 points)
          - Customer satisfaction (20 points)
          - Geographic diversity (15 points)
          - Category diversity (10 points)
          - Operational excellence (10 points)
```

### 8.4 Deploy Documentation to dbt Cloud

If using **dbt Cloud**:

1. Documentation auto-generates on each run
2. Access via: `https://YOUR_ACCOUNT.getdbt.com/docs`
3. Share link with stakeholders

If using **dbt Core + Cloud Storage**:

```bash
# Upload docs to GCS for team access
gsutil -m cp -r target/catalog.json gs://YOUR-BUCKET/dbt-docs/
gsutil -m cp -r target/manifest.json gs://YOUR-BUCKET/dbt-docs/
```

---

## 9. Deployment

### 9.1 Deployment Strategy

**Recommended Approach: dbt Cloud**

```
Development → Pull Request → Review → Merge → Prod Deployment
     ↓              ↓            ↓        ↓          ↓
  Feature      CI Tests     Approval   Main    dbt Cloud Job
   Branch                              Branch   (Scheduled)
```

### 9.2 Git Workflow

#### Commit Current Changes

```bash
cd /Users/azni/Projects/NTU/project-samba-insight/dbt

# Check current status
git status

# Expected output:
# On branch feature
# Changes not staged for commit:
#   modified:   models/marts/schema.yml
#
# Untracked files:
#   models/marts/mart_category_performance.sql
#   models/marts/mart_churn_prediction.sql
#   models/marts/mart_customer_retention_quarterly.sql
#   models/marts/mart_executive_summary.sql
#   models/marts/mart_geographic_performance.sql
#   models/marts/mart_unit_economics.sql
#   tests/

# Add new executive mart models
git add models/marts/mart_*.sql
git add models/marts/schema.yml
git add tests/

# Commit with descriptive message
git commit -m "Add 6 executive analytics marts

New models:
- mart_customer_retention_quarterly: Q1 retention tracking
- mart_geographic_performance: State/city expansion analysis
- mart_unit_economics: CAC payback and cohort health
- mart_churn_prediction: Rule-based churn risk scoring
- mart_category_performance: Category diversification strategy
- mart_executive_summary: Consolidated C-suite KPI dashboard

Includes:
- 30+ new data quality tests
- Comprehensive schema.yml documentation
- Custom data quality assertions in tests/

Closes #TICKET_NUMBER"
```

#### Push to GitHub

```bash
# Push feature branch
git push origin feature

# Expected output:
# Enumerating objects: 15, done.
# Counting objects: 100% (15/15), done.
# Delta compression using up to 8 threads
# Writing objects: 100% (12/12), 8.5 KiB | 8.5 MiB/s, done.
# Total 12 (delta 6), reused 0 (delta 0)
# To github.com:YOUR_USERNAME/project-samba-insight.git
#  * [new branch]      feature -> feature
```

### 9.3 Create Pull Request

**GitHub Web UI:**

1. Navigate to repository
2. Click "Compare & pull request"
3. Add description:

```markdown
## Executive Analytics Implementation

### Summary

Implements 6 new executive analytics marts for C-suite dashboard.

### New Models

1. **mart_customer_retention_quarterly** - Quarterly cohort retention tracking
2. **mart_geographic_performance** - State/city expansion strategy
3. **mart_unit_economics** - CAC payback and profitability
4. **mart_churn_prediction** - Customer churn risk segmentation
5. **mart_category_performance** - Product diversification analysis
6. **mart_executive_summary** - Consolidated KPI dashboard

### Testing

- ✅ 30+ new data quality tests passing
- ✅ Full build successful (staging → warehouse → marts)
- ✅ Documentation generated and reviewed

### Deployment Checklist

- [ ] Code review approved
- [ ] Tests passing in CI
- [ ] Documentation complete
- [ ] dbt Cloud job configured
```

4. Request review from team members
5. Wait for CI checks to pass

### 9.4 Merge to Main

After approval:

```bash
# Merge via GitHub UI (recommended)
# OR via command line:

git checkout main
git pull origin main
git merge feature
git push origin main

# Clean up feature branch
git branch -d feature
git push origin --delete feature
```

### 9.5 Production Deployment

#### Option A: dbt Cloud (Recommended)

**Setup dbt Cloud Job:**

1. **Navigate to Jobs** → "Create Job"

2. **Job Configuration:**

   ```yaml
   Name: Daily Production Build
   Environment: Production
   Commands:
     - dbt source freshness
     - dbt run
     - dbt test
   Schedule: Daily at 3:00 AM UTC
   ```

3. **Advanced Settings:**

   - ✅ Generate docs on run
   - ✅ Run on source freshness
   - ✅ Defer to previous run state (for incremental models)
   - ✅ Send notifications on failure

4. **Run Triggers:**
   - On schedule (daily)
   - On merge to main (CI/CD)
   - Manual trigger

**Trigger First Production Run:**

```bash
# Via dbt Cloud UI: Click "Run Now"
# OR via API:
curl -X POST \
  https://cloud.getdbt.com/api/v2/accounts/ACCOUNT_ID/jobs/JOB_ID/run/ \
  -H "Authorization: Token YOUR_API_TOKEN"
```

#### Option B: Local Scheduled Deployment

**Using Cron (Linux/Mac):**

```bash
# Create deployment script: deploy_prod.sh
#!/bin/bash
cd /Users/azni/Projects/NTU/project-samba-insight/dbt

# Pull latest code
git checkout main
git pull origin main

# Run pipeline
dbt run --target prod
dbt test --target prod

# Log results
echo "Production deployment completed at $(date)" >> logs/production.log

# Add to crontab
crontab -e

# Schedule daily at 3 AM
0 3 * * * /path/to/deploy_prod.sh
```

#### Option C: Google Cloud Composer (Airflow)

**Create DAG:** `dags/samba_insight_daily.py`

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'samba_insight_daily_build',
    default_args=default_args,
    description='Daily dbt build for Samba Insight',
    schedule_interval='0 3 * * *',  # 3 AM daily
    start_date=datetime(2025, 11, 20),
    catchup=False,
    tags=['dbt', 'analytics', 'daily'],
) as dag:

    # Task 1: Check source freshness
    freshness_check = BashOperator(
        task_id='check_source_freshness',
        bash_command='cd /dbt && dbt source freshness --target prod',
    )

    # Task 2: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt && dbt run --target prod',
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /dbt && dbt test --target prod --store-failures',
    )

    # Task 4: Generate documentation
    dbt_docs = BashOperator(
        task_id='generate_docs',
        bash_command='cd /dbt && dbt docs generate --target prod',
    )

    # Define dependencies
    freshness_check >> dbt_run >> dbt_test >> dbt_docs
```

### 9.6 Verify Production Deployment

```bash
# Check production tables exist
bq ls YOUR_PROJECT_ID:warehouse | grep mart_

# Expected output:
# mart_customer_retention_quarterly
# mart_geographic_performance
# mart_unit_economics
# mart_churn_prediction
# mart_category_performance
# mart_executive_summary

# Check row counts
bq query --use_legacy_sql=false \
"SELECT
  'retention_quarterly' as mart, COUNT(*) as rows
  FROM \`warehouse.mart_customer_retention_quarterly\`
UNION ALL
SELECT 'geographic', COUNT(*)
  FROM \`warehouse.mart_geographic_performance\`
UNION ALL
SELECT 'unit_economics', COUNT(*)
  FROM \`warehouse.mart_unit_economics\`
UNION ALL
SELECT 'churn_prediction', COUNT(*)
  FROM \`warehouse.mart_churn_prediction\`
UNION ALL
SELECT 'category_performance', COUNT(*)
  FROM \`warehouse.mart_category_performance\`
UNION ALL
SELECT 'executive_summary', COUNT(*)
  FROM \`warehouse.mart_executive_summary\`"

# Check last updated timestamps
bq query --use_legacy_sql=false \
"SELECT MAX(_updated_at) as last_updated
FROM \`warehouse.mart_executive_summary\`"
```

---

## 10. Monitoring & Maintenance

### 10.1 Daily Monitoring Checklist

**Automated Checks:**

```bash
# 1. Check dbt Cloud job status (if using dbt Cloud)
# Navigate to: https://cloud.getdbt.com/jobs

# 2. Check source freshness
dbt source freshness --target prod

# 3. Run quality tests
dbt test --target prod

# 4. Check for test failures
bq query --use_legacy_sql=false \
"SELECT * FROM \`warehouse.test_failures\`
WHERE created_at >= CURRENT_DATE()"
```

**Manual Spot Checks:**

```sql
-- Query 1: Check executive summary latest data
SELECT
  quarter_start,
  total_revenue,
  quarterly_retention_rate_pct,
  strategic_health_score
FROM `warehouse.mart_executive_summary`
ORDER BY quarter_start DESC
LIMIT 1;

-- Expected: Latest quarter with reasonable values

-- Query 2: Check churn prediction distribution
SELECT
  churn_risk_segment,
  COUNT(*) as customers,
  AVG(churn_risk_score) as avg_score
FROM `warehouse.mart_churn_prediction`
GROUP BY churn_risk_segment
ORDER BY avg_score DESC;

-- Expected: Distribution across all risk segments

-- Query 3: Verify geographic coverage
SELECT
  COUNT(DISTINCT customer_state) as states,
  COUNT(DISTINCT customer_city) as cities,
  SUM(total_revenue) as total_revenue
FROM `warehouse.mart_geographic_performance`;

-- Expected: ~27 states, ~4000+ cities
```

### 10.2 Performance Monitoring

**BigQuery Job Monitoring:**

```bash
# Check query costs for last 7 days
bq query --use_legacy_sql=false \
"SELECT
  DATE(creation_time) as date,
  user_email,
  COUNT(*) as queries,
  SUM(total_bytes_processed) / POW(10, 12) as tb_processed,
  SUM(total_slot_ms) / (1000 * 60 * 60) as slot_hours
FROM \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND statement_type = 'SELECT'
GROUP BY date, user_email
ORDER BY date DESC, tb_processed DESC"
```

**dbt Run Performance:**

```bash
# Check run times
cat logs/dbt.log | grep "Completed after" | tail -20

# Expected output:
# Completed after 0.23s
# Completed after 1.45s
# Completed after 12.34s

# View slowest models
dbt run --profile prod | grep "ERROR\|WARN\|in"
```

### 10.3 Data Quality Monitoring

**Key Metrics to Track:**

```sql
-- Metric 1: Test Pass Rate (Target: 100%)
SELECT
  COUNT(*) as total_tests,
  COUNTIF(status = 'pass') as passed,
  COUNTIF(status = 'fail') as failed,
  ROUND(COUNTIF(status = 'pass') * 100.0 / COUNT(*), 2) as pass_rate_pct
FROM `warehouse.test_results`
WHERE test_date = CURRENT_DATE();

-- Metric 2: Data Freshness (Target: < 24 hours)
SELECT
  table_name,
  MAX(_updated_at) as last_updated,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_updated_at), HOUR) as hours_stale
FROM `warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'mart_%'
GROUP BY table_name
HAVING hours_stale > 24;

-- Expected: Zero rows (all marts fresh)

-- Metric 3: Row Count Anomalies (Target: < 20% variance)
SELECT
  table_name,
  row_count,
  LAG(row_count) OVER (PARTITION BY table_name ORDER BY snapshot_date) as prev_row_count,
  ROUND((row_count - LAG(row_count) OVER (PARTITION BY table_name ORDER BY snapshot_date))
    * 100.0 / LAG(row_count) OVER (PARTITION BY table_name ORDER BY snapshot_date), 2) as pct_change
FROM `warehouse.row_count_history`
WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND ABS(pct_change) > 20;

-- Expected: Zero rows (no sudden spikes/drops)
```

### 10.4 Alerting & Notifications

**dbt Cloud Alerts:**

- Configure email/Slack notifications for job failures
- Set up alerts for test failures
- Monitor job duration (alert if >2x baseline)

**Custom BigQuery Alerts:**

```python
# Script: src/monitoring/check_data_quality.py
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText

client = bigquery.Client()

# Check for test failures
query = """
SELECT COUNT(*) as failures
FROM `warehouse.test_failures`
WHERE created_at >= CURRENT_DATE()
"""

results = client.query(query).to_dataframe()
failure_count = results['failures'].iloc[0]

if failure_count > 0:
    # Send alert email
    msg = MIMEText(f"ALERT: {failure_count} dbt tests failed today")
    msg['Subject'] = '[Samba Insight] Data Quality Alert'
    msg['From'] = 'alerts@company.com'
    msg['To'] = 'data-team@company.com'

    # Send email (configure SMTP)
    # smtp.send_message(msg)

    print(f"🚨 ALERT: {failure_count} tests failed")
else:
    print("✅ All tests passing")
```

### 10.5 Incremental Maintenance

**Weekly Tasks:**

```bash
# Week 1: Review test failures and fix
dbt test --store-failures
bq query --use_legacy_sql=false "SELECT * FROM warehouse.test_failures WHERE test_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"

# Week 2: Review slow queries
# Check BigQuery audit logs for queries > 30 seconds

# Week 3: Review and optimize costs
# Analyze slot usage and partitioning effectiveness

# Week 4: Update documentation
dbt docs generate
# Review and update model descriptions
```

**Monthly Tasks:**

```bash
# Full refresh of incremental models
dbt run --full-refresh --select fact_orders

# Review and update data quality thresholds
# Update accepted_range tests if business logic changed

# Backup and archive old partition data (if needed)

# Review and optimize table clustering
# Analyze query patterns and adjust clustering keys
```

---

## 11. Troubleshooting

### 11.1 Common Issues & Solutions

#### Issue 1: dbt Connection Errors

**Error:**

```
Database Error in model staging.stg_orders
  Credentials do not authorize this request.
```

**Solution:**

```bash
# Re-authenticate with GCP
gcloud auth application-default login

# Verify project
gcloud config get-value project

# Test connection
dbt debug

# If still failing, recreate profiles.yml
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit with correct project_id
```

#### Issue 2: Model Build Failures

**Error:**

```
Compilation Error in model mart_executive_summary
  depends on a node named 'mart_customer_retention_quarterly' which was not found
```

**Solution:**

```bash
# Build dependencies first
dbt run --select +mart_executive_summary

# Or build full DAG
dbt run
```

#### Issue 3: Test Failures

**Error:**

```
Failure in test unique_fact_orders_order_id
  Got 5 duplicate order_ids
```

**Solution:**

```bash
# Investigate duplicates
bq query --use_legacy_sql=false \
"SELECT order_id, COUNT(*) as cnt
FROM warehouse.fact_orders
GROUP BY order_id
HAVING cnt > 1"

# Fix: Full refresh to rebuild table
dbt run --full-refresh --select fact_orders

# Verify fix
dbt test --select fact_orders
```

#### Issue 4: Incremental Model Issues

**Error:**

```
Incremental model fact_orders failed to update
  No new records found
```

**Solution:**

```bash
# Check lookback window (might be too short)
# Edit dbt_project.yml:
vars:
  lookback_days: 7  # Increase from 3 to 7

# Run with full refresh to reset state
dbt run --full-refresh --select fact_orders

# Subsequent runs will be incremental
dbt run --select fact_orders
```

#### Issue 5: BigQuery Quota Exceeded

**Error:**

```
Quota exceeded: Your project exceeded quota for concurrent query bytes scanned
```

**Solution:**

```bash
# Option 1: Run models sequentially (reduce threads)
dbt run --threads 1

# Option 2: Use selectors to batch runs
dbt run --selector staging_only
# Wait 5 minutes
dbt run --selector warehouse_only
# Wait 5 minutes
dbt run --selector marts_only

# Option 3: Request quota increase from GCP
gcloud alpha billing quotas update \
  --service=bigquery.googleapis.com \
  --consumer=projects/YOUR_PROJECT_ID \
  --metric=query_usage
```

### 11.2 Debugging Queries

**Compile SQL without running:**

```bash
# Compile models to see generated SQL
dbt compile --select mart_executive_summary

# View compiled SQL
cat target/compiled/samba_insight/models/marts/mart_executive_summary.sql

# Copy SQL and run in BigQuery console for debugging
```

**Run single model with verbose logging:**

```bash
# Debug a specific model
dbt run --select mart_executive_summary --debug

# View detailed logs
tail -f logs/dbt.log
```

### 11.3 Data Quality Investigation

**Investigate test failure:**

```bash
# Store failures for inspection
dbt test --store-failures --select mart_churn_prediction

# Query failures
bq query --use_legacy_sql=false \
"SELECT *
FROM warehouse.test__audit__churn_risk_score_range
LIMIT 100"
```

### 11.4 Performance Troubleshooting

**Slow model query:**

```sql
-- Check query execution plan
EXPLAIN
SELECT * FROM `warehouse.mart_executive_summary`;

-- Check table size
SELECT
  table_name,
  row_count,
  size_bytes / POW(10, 9) as size_gb
FROM `warehouse.__TABLES__`
WHERE table_name = 'mart_executive_summary';

-- Check partitioning effectiveness
SELECT
  partition_id,
  row_count,
  size_bytes / POW(10, 6) as size_mb
FROM `warehouse.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'fact_orders'
ORDER BY partition_id DESC
LIMIT 10;
```

### 11.5 Getting Help

**Resources:**

1. **dbt Docs:** https://docs.getdbt.com
2. **dbt Slack Community:** https://getdbt.slack.com
3. **BigQuery Docs:** https://cloud.google.com/bigquery/docs
4. **Project README:** `/Users/azni/Projects/NTU/project-samba-insight/README.md`
5. **Marts Documentation:** `/Users/azni/Projects/NTU/project-samba-insight/dbt/models/marts/README.md`

**Internal Escalation:**

- Data Team Lead: data-team-lead@company.com
- Data Engineering: data-eng@company.com
- DevOps/Platform: platform-team@company.com

---

## 12. Appendices

### 12.1 Model Dependency Graph

```
┌─────────────┐
│ Raw Sources │
└──────┬──────┘
       │
┌──────▼──────────────────────────────────┐
│         Staging Models (Views)          │
│ • stg_orders    • stg_order_items      │
│ • stg_customers • stg_payments         │
│ • stg_products  • stg_sellers          │
│ • stg_reviews                          │
└──────┬──────────────────────────────────┘
       │
       ├──────────────────┬─────────────────┐
       │                  │                 │
┌──────▼───────┐  ┌──────▼─────┐  ┌────────▼──────┐
│ Dimensions   │  │ dim_date   │  │  fact_orders  │
│ • dim_customer│  │            │  │  (Incremental)│
│ • dim_product │  │            │  │               │
│ • dim_seller  │  │            │  │               │
└──────┬────────┘  └────────────┘  └───────┬───────┘
       │                                    │
       └────────────────┬───────────────────┘
                        │
       ┌────────────────▼──────────────────────┐
       │          Business Marts              │
       │                                      │
       │  EXISTING (5):                       │
       │  • mart_sales_daily                  │
       │  • mart_sales_monthly                │
       │  • mart_customer_cohorts             │
       │  • mart_customer_retention           │
       │  • mart_product_performance          │
       │                                      │
       │  NEW EXECUTIVE (6):                  │
       │  • mart_customer_retention_quarterly │
       │  • mart_geographic_performance       │
       │  • mart_unit_economics               │
       │  • mart_churn_prediction             │
       │  • mart_category_performance         │
       │  • mart_executive_summary ◄──── INTEGRATES ALL
       └──────────────────────────────────────┘
```

### 12.2 Key SQL Patterns

**Pattern 1: Date Truncation for Cohorts**

```sql
-- Quarterly cohorts
DATE_TRUNC(first_order_date, QUARTER) as cohort_quarter_start

-- Monthly cohorts
DATE_TRUNC(first_order_date, MONTH) as cohort_month
```

**Pattern 2: Retention Calculation**

```sql
-- Quarter-over-quarter retention
LAG(active_customers) OVER (
  PARTITION BY cohort_quarter
  ORDER BY activity_quarter
) as prev_quarter_active

-- Retention rate
active_customers * 100.0 / cohort_size as retention_rate_pct
```

**Pattern 3: Window Functions for Ranking**

```sql
-- Category revenue rank
ROW_NUMBER() OVER (
  ORDER BY total_revenue DESC
) as overall_rank

-- Within-category rank
ROW_NUMBER() OVER (
  PARTITION BY category
  ORDER BY total_revenue DESC
) as category_rank
```

**Pattern 4: Strategic Segmentation Logic**

```sql
-- Multi-criteria CASE statement
CASE
  WHEN revenue_share > 0.10 AND growth_rate > 0.20
    THEN 'Core Category'
  WHEN revenue_share < 0.02 AND growth_rate > 0.50
    THEN 'Growth Category'
  WHEN revenue_share > 0.05 AND growth_rate BETWEEN 0.10 AND 0.20
    THEN 'Emerging Category'
  ELSE 'Niche Category'
END as strategic_tier
```

### 12.3 Cost Optimization Tips

**Partitioning:**

```sql
-- Partition by date for time-series data
{{ config(
  partition_by={
    "field": "order_date",
    "data_type": "date",
    "granularity": "day"
  }
) }}
```

**Clustering:**

```sql
-- Cluster by frequently filtered columns
{{ config(
  cluster_by=['customer_state', 'product_category']
) }}
```

**Query Optimization:**

```sql
-- Always filter on partition key
SELECT * FROM warehouse.fact_orders
WHERE order_date >= '2018-01-01'  -- Reduces scan by 50%+

-- Use approximate aggregations for large tables
SELECT APPROX_COUNT_DISTINCT(customer_id) FROM warehouse.fact_orders
```

### 12.4 Useful dbt Commands Reference

```bash
# Development
dbt compile                      # Compile without running
dbt run --select MODEL_NAME      # Run specific model
dbt run --select +MODEL_NAME     # Run model with upstream dependencies
dbt run --select MODEL_NAME+     # Run model with downstream dependencies
dbt run --exclude MODEL_NAME     # Run all except specific model

# Testing
dbt test --select MODEL_NAME     # Test specific model
dbt test --select tag:TAG        # Test by tag
dbt test --store-failures        # Store failures for investigation

# Documentation
dbt docs generate                # Generate docs
dbt docs serve                   # View docs locally

# Utilities
dbt clean                        # Clean target/ and logs/
dbt deps                         # Install packages
dbt debug                        # Test connection
dbt source freshness             # Check source staleness
dbt ls                           # List all models
dbt ls --select tag:executive    # List models by tag

# Production
dbt run --target prod            # Run against prod profile
dbt build --full-refresh         # Full rebuild (run + test)
dbt run-operation MACRO_NAME     # Run macro/operation
```

### 12.5 Executive Mart Quick Reference

| Mart Name                             | Purpose                       | Key Metrics                                         | Update Frequency |
| ------------------------------------- | ----------------------------- | --------------------------------------------------- | ---------------- |
| **mart_customer_retention_quarterly** | Quarterly cohort retention    | Q1 retention rate, LTV to date                      | Daily            |
| **mart_geographic_performance**       | State/city expansion strategy | Revenue by geography, retention, expansion priority | Daily            |
| **mart_unit_economics**               | CAC payback analysis          | LTV:CAC ratio, payback period, cohort health        | Daily            |
| **mart_churn_prediction**             | Customer churn risk           | Churn probability, risk segment, retention priority | Daily            |
| **mart_category_performance**         | Product diversification       | Revenue share, growth rate, strategic tier          | Daily            |
| **mart_executive_summary**            | C-suite KPI dashboard         | Revenue growth, retention, health score             | Daily            |

### 12.6 Contact Information

**Project Team:**

- **Project Owner:** [Your Name] - your.email@company.com
- **Data Engineering:** data-eng@company.com
- **Analytics:** analytics@company.com
- **DevOps:** platform@company.com

**Emergency Contacts:**

- **Prod Down:** oncall-data@company.com
- **Security Issue:** security@company.com

---

## Summary Checklist

Use this checklist to verify successful implementation:

- [ ] **Environment Setup**

  - [ ] GCP project configured
  - [ ] BigQuery datasets created (raw, staging, warehouse)
  - [ ] dbt installed and profiles.yml configured
  - [ ] dbt debug passes

- [ ] **Data Ingestion**

  - [ ] Raw data downloaded from Kaggle
  - [ ] Data uploaded to GCS
  - [ ] Data loaded to BigQuery raw tables
  - [ ] Row counts verified (~99K orders, ~96K customers)

- [ ] **Pipeline Build**

  - [ ] Staging layer built (7 views)
  - [ ] Warehouse layer built (4 dimensions + 1 fact)
  - [ ] Existing marts built (5 marts)
  - [ ] **NEW: Executive marts built (6 marts) ✅**

- [ ] **Testing**

  - [ ] All dbt tests passing (173+ tests)
  - [ ] Source freshness checks passing
  - [ ] Data quality validated
  - [ ] **NEW: Executive mart tests passing (30+ tests) ✅**

- [ ] **Documentation**

  - [ ] dbt docs generated
  - [ ] Model lineage verified
  - [ ] Column descriptions complete
  - [ ] **NEW: Executive marts documented ✅**

- [ ] **Deployment**

  - [ ] Code committed to feature branch
  - [ ] Pull request created and reviewed
  - [ ] Merged to main branch
  - [ ] Production job configured (dbt Cloud or Airflow)
  - [ ] First production run successful

- [ ] **Monitoring**
  - [ ] Daily job scheduled
  - [ ] Alerts configured
  - [ ] Dashboard connected to marts
  - [ ] Stakeholders notified

---

**Congratulations! Your Project Samba Insight implementation is complete.** 🎉

For questions or issues, refer to the troubleshooting section or contact the data team.

**Next Steps:**

1. Connect BI tools (Looker Studio, Looker) to executive marts
2. Schedule stakeholder demos
3. Iterate based on feedback
4. Plan phase 2 enhancements
