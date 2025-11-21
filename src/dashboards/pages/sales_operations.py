"""
Sales Operations Dashboard

Detailed sales analysis by category, geography, and seller performance.
"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.dashboards.db_connection import get_table_fqn, run_query  # noqa: E402
from src.utils.config import get_config  # noqa: E402


def render():
    """Render the sales operations dashboard."""
    st.title("💰 Sales Operations")
    st.markdown("### Sales Analysis by Category, Geography & Sellers")

    st.markdown("---")

    # Get config for dataset references
    config = get_config()

    # Top categories
    st.markdown("### 📦 Top Product Categories")

    category_query = f"""
    WITH order_products AS (
        SELECT DISTINCT
            oi.order_id,
            p.product_category_name_en as category
        FROM {get_table_fqn("stg_order_items", dataset=config.bq_dataset_staging)} oi
        JOIN {get_table_fqn("dim_product")} p
            ON oi.product_id = p.product_id
    )
    SELECT
        op.category,
        COUNT(DISTINCT f.order_id) as orders,
        ROUND(SUM(f.total_order_value), 2) as revenue,
        ROUND(AVG(f.total_order_value), 2) as avg_order_value
    FROM {get_table_fqn("fact_orders")} f
    JOIN order_products op
        ON f.order_id = op.order_id
    WHERE f.order_status = 'delivered'
    GROUP BY op.category
    ORDER BY revenue DESC
    LIMIT 15
    """

    categories = run_query(category_query)

    col1, col2 = st.columns(2)

    with col1:
        fig_cat_revenue = px.bar(
            categories.head(10),
            x="revenue",
            y="category",
            orientation="h",
            title="Top 10 Categories by Revenue",
            labels={"revenue": "Revenue (R$)", "category": "Category"},
            color="revenue",
            color_continuous_scale="Blues",
        )
        fig_cat_revenue.update_layout(height=400, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_cat_revenue, use_container_width=True)

    with col2:
        fig_cat_orders = px.bar(
            categories.head(10),
            x="orders",
            y="category",
            orientation="h",
            title="Top 10 Categories by Order Volume",
            labels={"orders": "Orders", "category": "Category"},
            color="orders",
            color_continuous_scale="Greens",
        )
        fig_cat_orders.update_layout(height=400, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_cat_orders, use_container_width=True)

    st.markdown("---")

    # Geographic distribution
    st.markdown("### 🗺️ Sales by State")

    geo_query = f"""
    SELECT
        c.customer_state as state,
        COUNT(DISTINCT f.order_id) as orders,
        ROUND(SUM(f.total_order_value), 2) as revenue
    FROM {get_table_fqn("fact_orders")} f
    JOIN {get_table_fqn("dim_customer")} c
        ON f.customer_key = c.customer_key
    WHERE f.order_status = 'delivered'
    GROUP BY state
    ORDER BY revenue DESC
    """

    geo_data = run_query(geo_query)

    col1, col2 = st.columns([2, 1])

    with col1:
        fig_geo = px.bar(
            geo_data.head(15),
            x="state",
            y="revenue",
            title="Top 15 States by Revenue",
            labels={"revenue": "Revenue (R$)", "state": "State"},
            color="revenue",
            color_continuous_scale="Viridis",
        )
        fig_geo.update_layout(height=400)
        st.plotly_chart(fig_geo, use_container_width=True)

    with col2:
        st.markdown("#### Top 5 States")
        top_states = geo_data.head(5)
        for _idx, row in top_states.iterrows():
            st.metric(
                row["state"],
                f"R$ {row['revenue']:,.0f}",
                f"{row['orders']:,} orders",
            )

    st.markdown("---")

    # Payment analysis
    st.markdown("### 💳 Payment Method Analysis")

    payment_query = f"""
    SELECT
        payment_types,
        COUNT(*) as orders,
        ROUND(SUM(total_payment_value), 2) as total_paid,
        ROUND(AVG(max_installments), 2) as avg_installments
    FROM {get_table_fqn("fact_orders")}
    WHERE payment_types IS NOT NULL
        AND order_status = 'delivered'
    GROUP BY payment_types
    ORDER BY orders DESC
    LIMIT 10
    """

    payment_data = run_query(payment_query)

    col1, col2 = st.columns(2)

    with col1:
        fig_payment = px.pie(
            payment_data,
            values="orders",
            names="payment_types",
            title="Orders by Payment Method",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_payment.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_payment, use_container_width=True)

    with col2:
        st.markdown("#### Payment Summary")
        st.dataframe(
            payment_data.style.format(
                {"orders": "{:,}", "total_paid": "R$ {:,.2f}", "avg_installments": "{:.2f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("---")

    # Seller performance
    st.markdown("### 🏪 Top Performing Sellers")

    seller_query = f"""
    SELECT
        seller_id,
        seller_city,
        seller_state,
        total_orders,
        total_revenue,
        unique_products_sold,
        seller_tier
    FROM {get_table_fqn("dim_seller")}
    WHERE total_orders > 0
    ORDER BY total_revenue DESC
    LIMIT 20
    """

    sellers = run_query(seller_query)

    st.dataframe(
        sellers.style.format(
            {
                "total_orders": "{:,}",
                "total_revenue": "R$ {:,.2f}",
                "unique_products_sold": "{:,}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("---")
    st.info("💡 **Insight:** Filter and export data for deeper analysis")
