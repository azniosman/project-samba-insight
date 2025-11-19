"""
Executive Dashboard

High-level KPIs and trends for executive decision-making.
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.dashboards.db_connection import run_query


def render():
    """Render the executive dashboard."""
    st.title("📈 Executive Dashboard")
    st.markdown("### Key Performance Indicators & Business Trends")

    # Date filter
    col1, col2 = st.columns([3, 1])
    with col2:
        st.button("🔄 Refresh Data")  # Refresh button (side effect only)

    st.markdown("---")

    # Get KPIs
    kpi_query = """
    SELECT
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_key) as total_customers,
        ROUND(SUM(total_order_value), 2) as total_revenue,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(review_score), 2) as avg_review_score,
        SUM(CASE WHEN is_on_time_delivery THEN 1 ELSE 0 END) / COUNT(*) * 100 as on_time_pct
    FROM `project-samba-insight.dev_warehouse_warehouse.fact_orders`
    WHERE order_status = 'delivered'
    """

    kpis = run_query(kpi_query).iloc[0]

    # Display KPIs
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Total Orders", f"{int(kpis['total_orders']):,}")

    with col2:
        st.metric("Total Customers", f"{int(kpis['total_customers']):,}")

    with col3:
        st.metric("Total Revenue", f"R$ {kpis['total_revenue']:,.0f}")

    with col4:
        st.metric("Avg Order Value", f"R$ {kpis['avg_order_value']:.2f}")

    with col5:
        st.metric("Avg Review Score", f"{kpis['avg_review_score']:.2f} ⭐")

    with col6:
        st.metric("On-Time Delivery", f"{kpis['on_time_pct']:.1f}%")

    st.markdown("---")

    # Revenue trend
    st.markdown("### 📊 Monthly Revenue Trend")

    revenue_trend_query = """
    SELECT
        FORMAT_DATE('%Y-%m', order_purchase_date) as month,
        COUNT(DISTINCT order_id) as orders,
        ROUND(SUM(total_order_value), 2) as revenue
    FROM `project-samba-insight.dev_warehouse_warehouse.fact_orders`
    WHERE order_status = 'delivered'
    GROUP BY month
    ORDER BY month
    """

    revenue_trend = run_query(revenue_trend_query)

    fig_revenue = go.Figure()
    fig_revenue.add_trace(
        go.Scatter(
            x=revenue_trend["month"],
            y=revenue_trend["revenue"],
            mode="lines+markers",
            name="Revenue",
            line={"color": "#1f77b4", "width": 3},
            marker={"size": 8},
        )
    )

    fig_revenue.update_layout(
        title="Monthly Revenue (R$)",
        xaxis_title="Month",
        yaxis_title="Revenue (R$)",
        height=400,
        hovermode="x unified",
    )

    st.plotly_chart(fig_revenue, use_container_width=True)

    # Two columns for additional charts
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📦 Order Volume by Month")

        fig_orders = px.bar(
            revenue_trend,
            x="month",
            y="orders",
            title="Monthly Order Volume",
            labels={"month": "Month", "orders": "Orders"},
            color="orders",
            color_continuous_scale="Blues",
        )

        fig_orders.update_layout(height=350)
        st.plotly_chart(fig_orders, use_container_width=True)

    with col2:
        st.markdown("### ⭐ Review Score Distribution")

        review_dist_query = """
        SELECT
            review_score,
            COUNT(*) as count
        FROM `project-samba-insight.dev_warehouse_warehouse.fact_orders`
        WHERE review_score IS NOT NULL
        GROUP BY review_score
        ORDER BY review_score
        """

        review_dist = run_query(review_dist_query)

        fig_reviews = px.bar(
            review_dist,
            x="review_score",
            y="count",
            title="Review Score Distribution",
            labels={"review_score": "Review Score", "count": "Count"},
            color="review_score",
            color_continuous_scale="RdYlGn",
        )

        fig_reviews.update_layout(height=350)
        st.plotly_chart(fig_reviews, use_container_width=True)

    st.markdown("---")

    # Order status breakdown
    st.markdown("### 📋 Order Status Breakdown")

    status_query = """
    SELECT
        order_status,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM `project-samba-insight.dev_warehouse_warehouse.fact_orders`
    GROUP BY order_status
    ORDER BY count DESC
    """

    status_data = run_query(status_query)

    col1, col2 = st.columns([1, 2])

    with col1:
        st.dataframe(
            status_data.style.format({"count": "{:,}", "percentage": "{:.2f}%"}),
            use_container_width=True,
            hide_index=True,
        )

    with col2:
        fig_status = px.pie(
            status_data,
            values="count",
            names="order_status",
            title="Order Status Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig_status.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_status, use_container_width=True)

    st.markdown("---")
    st.info("💡 **Insight:** Data refreshes every 5 minutes from BigQuery data warehouse")
