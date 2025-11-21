/*
    Data Freshness Test: 30-Day Requirement

    Ensures all critical tables have data within the last 30 days.
    This test FAILS if any table hasn't been updated in 30+ days.

    Business Requirement: Data must be < 30 days old
*/

with freshness_check as (
    select
        'fact_orders' as table_name,
        max(order_purchase_timestamp) as last_updated,
        date_diff(current_date(), max(date(order_purchase_timestamp)), day) as days_since_last_update
    from {{ ref('fact_orders') }}

    union all

    select
        'dim_customer' as table_name,
        max(dbt_updated_at) as last_updated,
        date_diff(current_date(), max(date(dbt_updated_at)), day) as days_since_last_update
    from {{ ref('dim_customer') }}

    union all

    select
        'mart_executive_summary' as table_name,
        max(_updated_at) as last_updated,
        date_diff(current_date(), max(date(_updated_at)), day) as days_since_last_update
    from {{ ref('mart_executive_summary') }}

    union all

    select
        'mart_churn_prediction' as table_name,
        max(_updated_at) as last_updated,
        date_diff(current_date(), max(date(_updated_at)), day) as days_since_last_update
    from {{ ref('mart_churn_prediction') }}
)

-- Return rows where data is stale (>30 days old)
select *
from freshness_check
where days_since_last_update > 30
