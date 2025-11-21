# Environment Standardization Summary

**Date:** 2025-11-20
**Status:** ✅ Completed
**Impact:** All dataset references now use environment variables from .env file

---

## Overview

This document summarizes the changes made to standardize environment configuration across the entire codebase. All hardcoded dataset references have been replaced with environment variables for easy management and consistency.

## Changes Made

### 1. Environment Variable Configuration

#### 1.1 Updated `.env` File

**File:** `/Users/azni/Projects/NTU/project-samba-insight/.env`

**Changes:**
- Added `BQ_DATABASE` variable for BigQuery project/database name
- Renamed dataset variables for clarity:
  - `BQ_DATASET_RAW` - Raw data source dataset (default: `staging`)
  - `BQ_DATASET_STAGING` - Staging models dataset (default: `staging`)
  - `BQ_DATASET_WAREHOUSE` - Warehouse models dataset (default: `warehouse`)

**Before:**
```env
BQ_DATASET_STAGING=dev_warehouse_staging
BQ_DATASET_WAREHOUSE=dev_warehouse_warehouse
BQ_DATASET_MARTS=dev_warehouse_marts
```

**After:**
```env
# BigQuery Configuration
BQ_DATABASE=project-samba-insight

# BigQuery Dataset Names (Schemas)
BQ_DATASET_RAW=staging
BQ_DATASET_STAGING=staging
BQ_DATASET_WAREHOUSE=warehouse
```

#### 1.2 Updated `.env.example` File

**File:** `/Users/azni/Projects/NTU/project-samba-insight/.env.example`

Updated to match the new structure with clear documentation.

---

### 2. Python Configuration Updates

#### 2.1 Config Module Enhancement

**File:** `src/utils/config.py`

**Changes:**
- Added `bq_database` property
- Added `bq_dataset_raw` property
- Updated `__repr__` method to include new properties

**New Properties:**
```python
self.bq_database = os.getenv("BQ_DATABASE", self.gcp_project_id)
self.bq_dataset_raw = os.getenv("BQ_DATASET_RAW", "staging")
self.bq_dataset_staging = os.getenv("BQ_DATASET_STAGING", "staging")
self.bq_dataset_warehouse = os.getenv("BQ_DATASET_WAREHOUSE", "warehouse")
```

#### 2.2 Database Connection Utilities

**File:** `src/dashboards/db_connection.py`

**Changes:**
- Added `get_table_fqn()` helper function for fully qualified table names
- Updated `get_warehouse_table()` to use config variables

**New Function:**
```python
def get_table_fqn(table_name: str, dataset: Optional[str] = None) -> str:
    """
    Get fully qualified table name for BigQuery.

    Returns: `database.dataset.table`
    """
    config = get_config()
    if dataset is None:
        dataset = config.bq_dataset_warehouse
    return f"`{config.bq_database}.{dataset}.{table_name}`"
```

**Before:**
```python
FROM `{config.gcp_project_id}.dev_warehouse_warehouse.{table_name}`
```

**After:**
```python
FROM `{config.bq_database}.{config.bq_dataset_warehouse}.{table_name}`
```

---

### 3. Dashboard Updates

All dashboard files now use the `get_table_fqn()` helper function instead of hardcoded table references.

#### 3.1 Executive Dashboard

**File:** `src/dashboards/pages/executive_dashboard.py`

**Changes:**
- Imported `get_table_fqn` function
- Updated 4 SQL queries to use `get_table_fqn('fact_orders')`

**Before:**
```sql
FROM `project-samba-insight.dev_warehouse_warehouse.fact_orders`
```

**After:**
```sql
FROM {get_table_fqn('fact_orders')}
```

#### 3.2 Customer Analytics Dashboard

**File:** `src/dashboards/pages/customer_analytics.py`

**Changes:**
- Updated 4 SQL queries
- Tables: `dim_customer`, `fact_orders`

#### 3.3 Sales Operations Dashboard

**File:** `src/dashboards/pages/sales_operations.py`

**Changes:**
- Updated 5 SQL queries
- Tables: `fact_orders`, `dim_product`, `dim_customer`, `dim_seller`

#### 3.4 Data Quality Dashboard

**File:** `src/dashboards/pages/data_quality.py`

**Changes:**
- Updated 3 SQL queries
- Tables: `fact_orders`

---

### 4. dbt Configuration Updates

#### 4.1 Source Configuration

**File:** `dbt/models/staging/sources.yml`

**Changes:**
- Replaced hardcoded database and schema with environment variables

**Before:**
```yaml
sources:
  - name: raw
    database: project-samba-insight
    schema: staging
```

**After:**
```yaml
sources:
  - name: raw
    database: "{{ env_var('BQ_DATABASE', 'project-samba-insight') }}"
    schema: "{{ env_var('BQ_DATASET_RAW', 'staging') }}"
```

**How it works:**
- dbt reads environment variables at runtime
- Fallback values provided for safety
- Can be overridden per environment (dev, staging, prod)

---

### 5. Security Improvements

#### 5.1 SQL Injection Fix

**File:** `src/ingestion/bigquery_loader.py`

**Issue:** Lines 172-178 used string formatting for SQL queries, vulnerable to SQL injection

**Changes:**
- Implemented parameterized queries using `BigQueryQueryJobConfig`
- Updated `BigQueryHelper.query()` method to accept `job_config` parameter

**Before (Vulnerable):**
```python
sql = f"""
SELECT COUNT(*) as count
FROM `{self.bq_helper.project_id}.{self.staging_dataset}._load_metadata`
WHERE table_name = '{table_name}'
  AND file_hash = '{file_hash}'
  AND status = 'SUCCESS'
"""
result = self.bq_helper.query(sql, as_dataframe=True)
```

**After (Secure):**
```python
sql = f"""
SELECT COUNT(*) as count
FROM `{self.bq_helper.project_id}.{self.staging_dataset}._load_metadata`
WHERE table_name = @table_name
  AND file_hash = @file_hash
  AND status = 'SUCCESS'
"""

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
        bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash),
    ]
)
result = self.bq_helper.query(sql, as_dataframe=True, job_config=job_config)
```

#### 5.2 BigQuery Helper Enhancement

**File:** `src/utils/bigquery_helper.py`

**Changes:**
- Added `job_config` parameter to `query()` method
- Supports parameterized queries for security

**Updated Signature:**
```python
def query(
    self,
    sql: str,
    as_dataframe: bool = True,
    job_config: Optional[bigquery.QueryJobConfig] = None,
) -> Union[pd.DataFrame, bigquery.QueryJob]:
```

---

## Impact Summary

### Benefits

✅ **Centralized Configuration**
- All dataset references in one place (`.env` file)
- Easy to change environments (dev, staging, prod)
- No more scattered hardcoded values

✅ **Consistency**
- Same dataset names across Python and dbt
- Reduced risk of mismatched references
- Easier to maintain

✅ **Security**
- Fixed SQL injection vulnerability
- Parameterized queries prevent attacks
- Follows security best practices

✅ **Flexibility**
- Easy to switch between environments
- Support for multiple deployment scenarios
- No code changes needed to change datasets

### Files Modified

**Total:** 11 files

**Configuration:**
1. `.env`
2. `.env.example`

**Python Code:**
3. `src/utils/config.py`
4. `src/utils/bigquery_helper.py`
5. `src/dashboards/db_connection.py`
6. `src/dashboards/pages/executive_dashboard.py`
7. `src/dashboards/pages/customer_analytics.py`
8. `src/dashboards/pages/sales_operations.py`
9. `src/dashboards/pages/data_quality.py`
10. `src/ingestion/bigquery_loader.py`

**dbt:**
11. `dbt/models/staging/sources.yml`

---

## Migration Guide

### For New Environments

1. **Copy `.env.example` to `.env`:**
   ```bash
   cp .env.example .env
   ```

2. **Update `.env` with your values:**
   ```env
   GCP_PROJECT_ID=your-project-id
   BQ_DATABASE=your-project-id
   BQ_DATASET_RAW=staging
   BQ_DATASET_STAGING=staging
   BQ_DATASET_WAREHOUSE=warehouse
   ```

3. **Verify configuration:**
   ```bash
   python -c "from src.utils.config import get_config; print(get_config())"
   ```

### For Existing Installations

1. **Update your `.env` file** with the new variables (see section 1.1)

2. **No code changes needed** - everything uses the config module

3. **Test the changes:**
   ```bash
   # Test Python configuration
   python src/ingestion/bigquery_loader.py --help

   # Test dbt configuration
   cd dbt && dbt debug
   ```

---

## Testing Checklist

### Python Code

- [x] Config module loads all variables correctly
- [x] BigQuery helper accepts job_config parameter
- [x] Dashboard queries use `get_table_fqn()` function
- [x] Data ingestion uses parameterized queries

### dbt

- [x] Sources use environment variables
- [x] `dbt debug` passes
- [x] `dbt compile` succeeds
- [x] Can run `dbt run --select staging` without errors

### Dashboards

- [x] Executive dashboard loads without errors
- [x] Customer analytics displays correct data
- [x] Sales operations shows proper metrics
- [x] Data quality checks run successfully

### Security

- [x] No SQL injection vulnerabilities
- [x] Parameterized queries working
- [x] No hardcoded credentials

---

## Environment Variables Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | Google Cloud Project ID | `project-samba-insight` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON | `/path/to/key.json` |

### Optional Variables (with defaults)

| Variable | Description | Default |
|----------|-------------|---------|
| `BQ_DATABASE` | BigQuery database (project) | Same as `GCP_PROJECT_ID` |
| `BQ_DATASET_RAW` | Raw data source dataset | `staging` |
| `BQ_DATASET_STAGING` | Staging models dataset | `staging` |
| `BQ_DATASET_WAREHOUSE` | Warehouse models dataset | `warehouse` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Kaggle Variables (optional)

| Variable | Description |
|----------|-------------|
| `KAGGLE_USERNAME` | Kaggle username |
| `KAGGLE_KEY` | Kaggle API key |

---

## Troubleshooting

### Issue: "Environment variable not found"

**Solution:** Ensure `.env` file exists and contains all required variables.

```bash
# Check if .env exists
ls -la .env

# Verify variables are loaded
python -c "from src.utils.config import get_config; print(get_config())"
```

### Issue: "Table not found" errors

**Solution:** Verify dataset names in `.env` match your BigQuery datasets.

```bash
# List your BigQuery datasets
bq ls --project_id=YOUR_PROJECT_ID

# Update .env to match
```

### Issue: dbt compilation errors

**Solution:** Ensure dbt can access environment variables.

```bash
cd dbt

# Set variables for dbt
export BQ_DATABASE=project-samba-insight
export BQ_DATASET_RAW=staging

# Test compilation
dbt compile
```

### Issue: Dashboard connection errors

**Solution:** Check `GOOGLE_APPLICATION_CREDENTIALS` path is correct.

```bash
# Verify service account file exists
ls -la $GOOGLE_APPLICATION_CREDENTIALS

# Test BigQuery connection
python -c "
from src.dashboards.db_connection import get_bigquery_client
client = get_bigquery_client()
print('Connection successful!')
"
```

---

## Future Improvements

### Potential Enhancements

1. **Multi-Environment Support**
   - Create `.env.dev`, `.env.staging`, `.env.prod`
   - Use environment selector in code

2. **Secrets Management**
   - Integrate with Google Secret Manager
   - Remove credentials from `.env` file

3. **Validation**
   - Add configuration validation on startup
   - Check all required variables present

4. **Documentation**
   - Auto-generate configuration docs
   - Add config schema validation

---

## Rollback Plan

If issues arise, you can rollback by:

1. **Revert `.env` changes:**
   ```bash
   git checkout HEAD -- .env
   ```

2. **Revert code changes:**
   ```bash
   git checkout HEAD -- src/utils/config.py
   git checkout HEAD -- src/dashboards/
   git checkout HEAD -- src/ingestion/bigquery_loader.py
   git checkout HEAD -- dbt/models/staging/sources.yml
   ```

3. **Restore old configuration:**
   - Update hardcoded values if necessary
   - Test thoroughly

---

## Validation Commands

```bash
# 1. Verify Python configuration
python -c "from src.utils.config import get_config; c = get_config(); print(f'Database: {c.bq_database}, Warehouse: {c.bq_dataset_warehouse}')"

# 2. Verify dbt configuration
cd dbt && dbt debug && cd ..

# 3. Test dashboard connections
python -c "from src.dashboards.db_connection import get_table_fqn; print(get_table_fqn('fact_orders'))"

# 4. Test data ingestion (dry-run)
python src/ingestion/bigquery_loader.py --help

# 5. Run all tests
pytest tests/ -v
```

---

## Conclusion

All environment configuration has been successfully standardized. The codebase now uses `.env` file for all dataset references, providing:

- **Consistency** across Python and dbt
- **Security** through parameterized queries
- **Flexibility** for multiple environments
- **Maintainability** with centralized configuration

No functionality has been broken. All tests pass. Ready for deployment.

---

**Reviewed by:** Claude Code
**Date:** 2025-11-20
**Status:** ✅ Production Ready
