"""
Database Connection Utilities for Streamlit Dashboards

Handles BigQuery connections and query caching.
"""

import sys
from pathlib import Path
from typing import Optional

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import get_config


@st.cache_resource
def get_bigquery_client():
    """
    Get cached BigQuery client.

    Returns:
        BigQuery client instance
    """
    config = get_config()

    if config.google_application_credentials:
        credentials = service_account.Credentials.from_service_account_file(
            config.google_application_credentials,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        client = bigquery.Client(project=config.gcp_project_id, credentials=credentials)
    else:
        client = bigquery.Client(project=config.gcp_project_id)

    return client


@st.cache_data(ttl=300)  # Cache for 5 minutes
def run_query(query: str):
    """
    Execute a BigQuery query and return results as DataFrame.

    Args:
        query: SQL query to execute

    Returns:
        pandas DataFrame with query results
    """
    client = get_bigquery_client()
    return client.query(query).to_dataframe()


def get_warehouse_table(table_name: str, limit: Optional[int] = None):
    """
    Get data from a warehouse table.

    Args:
        table_name: Name of the table (e.g., 'fact_orders', 'dim_customer')
        limit: Optional row limit

    Returns:
        pandas DataFrame
    """
    config = get_config()
    query = f"""
    SELECT *
    FROM `{config.gcp_project_id}.dev_warehouse_warehouse.{table_name}`
    """
    if limit:
        query += f" LIMIT {limit}"

    return run_query(query)
