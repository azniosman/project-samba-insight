"""
Customer Analytics Dashboard

Customer segmentation, retention, and behavioral analysis.
"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.dashboards.db_connection import run_query


def render():
    """Render the customer analytics dashboard."""
    st.title("👥 Customer Analytics")
    st.markdown("### Customer Segmentation & Behavioral Analysis")

    st.markdown("---")

    # Customer segments
    st.markdown("### 🎯 Customer Segmentation")

    segment_query = """
    SELECT
        customer_segment,
        COUNT(*) as customers,
        ROUND(AVG(total_orders), 2) as avg_orders,
        ROUND(AVG(total_orders * 150), 2) as estimated_ltv
    FROM `project-samba-insight.dev_warehouse_warehouse.dim_customer`
    WHERE total_orders > 0
    GROUP BY customer_segment
    ORDER BY customers DESC
    """

    segments = run_query(segment_query)

    col1, col2, col3 = st.columns(3)

    for idx, row in segments.iterrows():
        col = [col1, col2, col3][idx % 3]
        with col:
            st.metric(
                f"{row['customer_segment'].title()} Customers",
                f"{int(row['customers']):,}",
                f"Avg {row['avg_orders']:.1f} orders",
            )

    col1, col2 = st.columns(2)

    with col1:
        fig_seg_customers = px.pie(
            segments,
            values="customers",
            names="customer_segment",
            title="Customer Distribution by Segment",
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_seg_customers.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_seg_customers, use_container_width=True)

    with col2:
        fig_seg_ltv = px.bar(
            segments,
            x="customer_segment",
            y="estimated_ltv",
            title="Estimated Lifetime Value by Segment",
            labels={"estimated_ltv": "Est. LTV (R$)", "customer_segment": "Segment"},
            color="estimated_ltv",
            color_continuous_scale="Greens",
        )
        st.plotly_chart(fig_seg_ltv, use_container_width=True)

    st.markdown("---")

    # Geographic distribution
    st.markdown("### 📍 Customer Geographic Distribution")

    cust_geo_query = """
    SELECT
        customer_state,
        COUNT(*) as customers,
        ROUND(AVG(total_orders), 2) as avg_orders_per_customer
    FROM `project-samba-insight.dev_warehouse_warehouse.dim_customer`
    WHERE total_orders > 0
    GROUP BY customer_state
    ORDER BY customers DESC
    LIMIT 15
    """

    cust_geo = run_query(cust_geo_query)

    fig_cust_geo = px.bar(
        cust_geo,
        x="customer_state",
        y="customers",
        title="Customers by State (Top 15)",
        labels={"customers": "Customers", "customer_state": "State"},
        color="avg_orders_per_customer",
        color_continuous_scale="Blues",
    )
    fig_cust_geo.update_layout(height=400)
    st.plotly_chart(fig_cust_geo, use_container_width=True)

    st.markdown("---")

    # Review sentiment analysis
    st.markdown("### ⭐ Review Sentiment Analysis")

    sentiment_query = """
    SELECT
        review_sentiment,
        COUNT(*) as count,
        ROUND(AVG(review_score), 2) as avg_score
    FROM `project-samba-insight.dev_warehouse_warehouse.fact_orders`
    WHERE review_sentiment IS NOT NULL
    GROUP BY review_sentiment
    ORDER BY
        CASE review_sentiment
            WHEN 'positive' THEN 1
            WHEN 'neutral' THEN 2
            WHEN 'negative' THEN 3
        END
    """

    sentiment = run_query(sentiment_query)

    col1, col2 = st.columns(2)

    with col1:
        fig_sentiment = px.bar(
            sentiment,
            x="review_sentiment",
            y="count",
            title="Review Sentiment Distribution",
            labels={"count": "Orders", "review_sentiment": "Sentiment"},
            color="avg_score",
            color_continuous_scale="RdYlGn",
        )
        st.plotly_chart(fig_sentiment, use_container_width=True)

    with col2:
        # Calculate percentages
        sentiment["percentage"] = (sentiment["count"] / sentiment["count"].sum() * 100).round(2)

        st.markdown("#### Sentiment Breakdown")
        for _, row in sentiment.iterrows():
            emoji = (
                "😊"
                if row["review_sentiment"] == "positive"
                else "😐"
                if row["review_sentiment"] == "neutral"
                else "😞"
            )
            st.metric(
                f"{emoji} {row['review_sentiment'].title()}",
                f"{row['percentage']:.1f}%",
                f"Avg score: {row['avg_score']:.2f}",
            )

    st.markdown("---")

    # Top customers
    st.markdown("### 🌟 Top Customers by Order Volume")

    top_customers_query = """
    SELECT
        customer_id,
        customer_city,
        customer_state,
        total_orders,
        delivered_orders,
        avg_review_score,
        customer_segment
    FROM `project-samba-insight.dev_warehouse_warehouse.dim_customer`
    WHERE total_orders > 0
    ORDER BY total_orders DESC
    LIMIT 25
    """

    top_customers = run_query(top_customers_query)

    st.dataframe(
        top_customers.style.format(
            {
                "total_orders": "{:,}",
                "delivered_orders": "{:,}",
                "avg_review_score": "{:.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("---")
    st.info(
        "💡 **Insight:** Focus retention efforts on 'repeat' customers to convert them to 'loyal'"
    )
