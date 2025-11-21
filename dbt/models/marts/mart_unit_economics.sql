{{
    config(
        materialized='table',
        tags=['mart', 'economics', 'executive']
    )
}}

/*
    Unit Economics & CAC Payback Analysis

    Calculates customer acquisition cost (CAC) payback period and unit economics
    by cohort and geography.

    **IMPORTANT:** This model requires CAC data to be loaded into a source table.
    Currently uses estimated CAC of R$50 per customer as a placeholder.

    TO ADD REAL CAC DATA:
    1. Create staging table: `staging.marketing_spend_raw`
       Columns: month, channel, spend, customers_acquired
    2. Create staging model: `stg_marketing_spend.sql`
    3. Update this model to join real CAC data

    Grain: One row per cohort_quarter, geography

    Business Impact: CAC payback < 12 months is healthy for e-commerce
*/

with cohort_ltv as (
    select
        cohort_quarter_start,
        cohort_quarter_label,
        customer_state,
        customer_city,
        quarters_since_cohort,

        cohort_size,
        ltv_to_date,
        cumulative_revenue,
        cumulative_orders,
        retention_rate_pct

    from {{ ref('mart_customer_retention_quarterly') }}
),

-- TODO: Replace with actual CAC data when available
-- Placeholder: Estimated CAC based on industry benchmarks
estimated_cac as (
    select distinct
        cohort_quarter_start,
        cohort_quarter_label,
        customer_state,
        customer_city,

        -- Estimated CAC by region (placeholder values)
        case
            when customer_state in ('SP', 'RJ', 'MG') then 60.00  -- High competition states
            when customer_state in ('PR', 'SC', 'RS') then 50.00  -- Medium competition
            else 40.00  -- Lower competition states
        end as estimated_cac_per_customer,

        'PLACEHOLDER - Replace with actual marketing spend data' as cac_data_source

    from cohort_ltv
),

unit_economics as (
    select
        cl.cohort_quarter_start,
        cl.cohort_quarter_label,
        cl.customer_state,
        cl.customer_city,
        cl.quarters_since_cohort,

        -- Cohort metrics
        cl.cohort_size,
        cl.ltv_to_date,
        cl.cumulative_revenue,
        cl.cumulative_orders,
        cl.retention_rate_pct,

        -- CAC
        ec.estimated_cac_per_customer as cac,
        ec.cac_data_source,

        -- Unit Economics
        round(cl.ltv_to_date - ec.estimated_cac_per_customer, 2) as customer_profit,

        -- CAC Ratio (LTV / CAC) - Healthy > 3.0
        round(
            cl.ltv_to_date / nullif(ec.estimated_cac_per_customer, 0),
            2
        ) as ltv_cac_ratio,

        -- Payback status
        case
            when cl.ltv_to_date >= ec.estimated_cac_per_customer then true
            else false
        end as has_paid_back_cac,

        -- Gross margin assumptions (placeholder - replace with actual COGS)
        -- E-commerce typical gross margin: 40-50%
        round(cl.cumulative_revenue * 0.45, 2) as estimated_gross_profit,

        -- Contribution margin after CAC
        round(
            (cl.cumulative_revenue * 0.45) - ec.estimated_cac_per_customer,
            2
        ) as contribution_margin_per_customer,

        current_timestamp() as _updated_at

    from cohort_ltv as cl
    inner join estimated_cac as ec
        on cl.cohort_quarter_start = ec.cohort_quarter_start
        and cl.customer_state = ec.customer_state
        and cl.customer_city = ec.customer_city
),

-- Identify CAC payback quarter
payback_analysis as (
    select
        *,

        -- Flag the quarter where CAC was paid back
        case
            when has_paid_back_cac
                and lag(has_paid_back_cac) over (
                    partition by cohort_quarter_start, customer_state, customer_city
                    order by quarters_since_cohort
                ) = false
            then quarters_since_cohort
        end as payback_quarter,

        -- Cohort health score (0-100)
        case
            when ltv_cac_ratio >= 4.0 and retention_rate_pct >= 50 then 90
            when ltv_cac_ratio >= 3.0 and retention_rate_pct >= 40 then 75
            when ltv_cac_ratio >= 2.0 and retention_rate_pct >= 30 then 60
            when ltv_cac_ratio >= 1.5 and retention_rate_pct >= 20 then 45
            when ltv_cac_ratio >= 1.0 then 30
            else 15
        end as cohort_health_score,

        -- Executive insights
        case
            when ltv_cac_ratio < 1.0 then 'CRITICAL: Not recovering CAC'
            when ltv_cac_ratio < 2.0 then 'WARNING: Low margins'
            when ltv_cac_ratio < 3.0 then 'ACCEPTABLE: Room for improvement'
            when ltv_cac_ratio < 4.0 then 'GOOD: Healthy unit economics'
            else 'EXCELLENT: Strong unit economics'
        end as unit_economics_status

    from unit_economics
)

select * from payback_analysis
order by
    cohort_quarter_start desc,
    customer_state,
    customer_city,
    quarters_since_cohort
