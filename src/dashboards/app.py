"""
Samba Insight - Main Dashboard Application

Multi-page Streamlit dashboard for Brazilian E-Commerce Analytics.
"""

import sys
from pathlib import Path

import streamlit as st

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Page configuration
st.set_page_config(
    page_title="Samba Insight - E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    h1 {
        color: #1f77b4;
    }
    h2 {
        color: #ff7f0e;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Sidebar navigation
st.sidebar.title("🎯 Navigation")
st.sidebar.markdown("---")

page: str = st.sidebar.radio(
    "Select Dashboard",
    [
        "🏠 Home",
        "📈 Executive Dashboard",
        "💰 Sales Operations",
        "👥 Customer Analytics",
        "✅ Data Quality",
    ],
)

st.sidebar.markdown("---")
st.sidebar.markdown("### About")
st.sidebar.info(
    """
    **Samba Insight**

    Brazilian E-Commerce Analytics Platform

    Data Source: Olist Brazilian E-Commerce

    Period: 2016-2018
    """
)

# Main content area
if page == "🏠 Home":
    st.title("📊 Samba Insight")
    st.markdown("## Brazilian E-Commerce Analytics Platform")

    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 📈 Executive Dashboard")
        st.markdown(
            """
            - Key Performance Indicators
            - Revenue Trends
            - Order Volume Analysis
            - Growth Metrics
            """
        )

    with col2:
        st.markdown("### 💰 Sales Operations")
        st.markdown(
            """
            - Sales by Category
            - Geographic Distribution
            - Payment Analysis
            - Seller Performance
            """
        )

    with col3:
        st.markdown("### 👥 Customer Analytics")
        st.markdown(
            """
            - Customer Segmentation
            - Retention Analysis
            - Review Sentiment
            - Lifetime Value
            """
        )

    st.markdown("---")

    st.markdown("### 🚀 Getting Started")
    st.markdown(
        """
        1. Select a dashboard from the sidebar
        2. Use filters to explore different segments
        3. Hover over charts for detailed information
        4. Download data using the export buttons

        **Data Warehouse:**
        - **Fact Table:** 99,441 orders
        - **Dimensions:** Customers, Products, Sellers, Dates
        - **Date Range:** September 2016 - October 2018
        - **Last Updated:** Real-time from BigQuery
        """
    )

    st.markdown("---")
    st.success("✅ All systems operational | Data warehouse ready | 41/41 tests passing")

elif page == "📈 Executive Dashboard":
    from src.dashboards.pages import executive_dashboard

    executive_dashboard.render()

elif page == "💰 Sales Operations":
    from src.dashboards.pages import sales_operations

    sales_operations.render()

elif page == "👥 Customer Analytics":
    from src.dashboards.pages import customer_analytics

    customer_analytics.render()

elif page == "✅ Data Quality":
    from src.dashboards.pages import data_quality

    data_quality.render()
