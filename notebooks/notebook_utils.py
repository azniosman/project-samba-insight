"""
Notebook Utilities Module

Common utility functions for Jupyter notebooks in the Samba Insight project.
Provides database connections, query helpers, and visualization utilities.
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from google.cloud import bigquery
from google.oauth2 import service_account


class NotebookConfig:
    """Configuration manager for notebooks."""

    def __init__(self):
        """Initialize configuration from environment or config utility."""
        # Add project root to path
        self.project_root = Path().absolute().parent
        if str(self.project_root) not in sys.path:
            sys.path.insert(0, str(self.project_root))

        # Force reload environment variables from .env file
        from dotenv import load_dotenv

        env_file = self.project_root / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=True)

        # Try to import config utility (preferred method)
        try:
            from src.utils.config import get_config

            config = get_config(reload=True)  # Force reload to pick up latest env vars
            self.project_id = config.gcp_project_id
            self.credentials_path = config.google_application_credentials
            # Use warehouse dataset from config (environment-aware)
            # Dev: dev_warehouse_warehouse | Prod: warehouse
            self.dataset = config.bq_dataset_warehouse
        except ImportError:
            # Fallback: read directly from environment variables if config import fails
            self.credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            self.project_id = os.getenv("GCP_PROJECT_ID", "project-samba-insight")

            # Dataset with intelligent fallback:
            # 1. Use BQ_DATASET_WAREHOUSE if set
            # 2. Otherwise construct from ENVIRONMENT: {env}_warehouse_warehouse
            env = os.getenv("ENVIRONMENT", "dev")
            self.dataset = os.getenv("BQ_DATASET_WAREHOUSE", f"{env}_warehouse_warehouse")

    def get_bigquery_client(self) -> bigquery.Client:
        """
        Get authenticated BigQuery client.

        Returns:
            Authenticated BigQuery client
        """
        if not self.credentials_path or not os.path.exists(self.credentials_path):
            print("⚠️  Using application default credentials")
            return bigquery.Client(project=self.project_id)
        else:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            return bigquery.Client(project=self.project_id, credentials=credentials)


class BigQueryHelper:
    """Helper class for BigQuery operations in notebooks."""

    def __init__(self, client: bigquery.Client, project_id: str, dataset: str):
        """
        Initialize BigQuery helper.

        Args:
            client: Authenticated BigQuery client
            project_id: GCP project ID
            dataset: BigQuery dataset name
        """
        self.client = client
        self.project_id = project_id
        self.dataset = dataset

    def query_to_dataframe(
        self, query: str, params: Optional[Dict[str, Any]] = None, use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters
            use_cache: Whether to use BigQuery cache

        Returns:
            Query results as pandas DataFrame
        """
        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = use_cache

        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
            ]

        try:
            df = self.client.query(query, job_config=job_config).to_dataframe()
            return df
        except Exception as e:
            print(f"❌ Query failed: {e}")
            raise

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table metadata
        """
        table_ref = f"{self.project_id}.{self.dataset}.{table_name}"

        try:
            table = self.client.get_table(table_ref)
            return {
                "table_id": table.table_id,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created,
                "modified": table.modified,
                "schema": [
                    {"name": field.name, "type": field.field_type} for field in table.schema
                ],
            }
        except Exception as e:
            print(f"❌ Failed to get table info: {e}")
            return {}

    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        table_ref = f"{self.project_id}.{self.dataset}.{table_name}"
        try:
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False


class VisualizationHelper:
    """Helper class for creating consistent visualizations."""

    def __init__(self, style: str = "whitegrid", figsize: Tuple[int, int] = (12, 6)):
        """
        Initialize visualization helper.

        Args:
            style: Seaborn style to use
            figsize: Default figure size
        """
        sns.set_style(style)
        self.default_figsize = figsize
        self.colors = {
            "primary": "#1f77b4",
            "success": "#2ca02c",
            "warning": "#ff7f0e",
            "danger": "#d62728",
            "info": "#17becf",
        }

    def create_line_plot(
        self,
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str,
        xlabel: Optional[str] = None,
        ylabel: Optional[str] = None,
        figsize: Optional[Tuple[int, int]] = None,
        **kwargs,
    ) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create a line plot.

        Args:
            df: DataFrame with data
            x: Column name for x-axis
            y: Column name for y-axis
            title: Plot title
            xlabel: X-axis label
            ylabel: Y-axis label
            figsize: Figure size
            **kwargs: Additional arguments for plt.plot

        Returns:
            Tuple of (figure, axes)
        """
        figsize = figsize or self.default_figsize
        fig, ax = plt.subplots(figsize=figsize)

        ax.plot(
            df[x],
            df[y],
            marker="o",
            linewidth=2,
            color=kwargs.pop("color", self.colors["primary"]),
            **kwargs,
        )
        ax.set_title(title, fontsize=16, fontweight="bold")
        ax.set_xlabel(xlabel or x, fontsize=12)
        ax.set_ylabel(ylabel or y, fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=45)

        plt.tight_layout()
        return fig, ax

    def create_bar_plot(
        self,
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str,
        xlabel: Optional[str] = None,
        ylabel: Optional[str] = None,
        figsize: Optional[Tuple[int, int]] = None,
        horizontal: bool = False,
        **kwargs,
    ) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create a bar plot.

        Args:
            df: DataFrame with data
            x: Column name for x-axis (or y-axis if horizontal)
            y: Column name for y-axis (or x-axis if horizontal)
            title: Plot title
            xlabel: X-axis label
            ylabel: Y-axis label
            figsize: Figure size
            horizontal: Whether to create horizontal bars
            **kwargs: Additional arguments for plt.bar/plt.barh

        Returns:
            Tuple of (figure, axes)
        """
        figsize = figsize or self.default_figsize
        fig, ax = plt.subplots(figsize=figsize)

        color = kwargs.pop("color", self.colors["success"])
        alpha = kwargs.pop("alpha", 0.7)

        if horizontal:
            y_pos = np.arange(len(df))
            ax.barh(y_pos, df[y], color=color, alpha=alpha, **kwargs)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(df[x])
            ax.invert_yaxis()
            ax.set_xlabel(ylabel or y, fontsize=12)
        else:
            ax.bar(df[x], df[y], color=color, alpha=alpha, **kwargs)
            ax.set_xlabel(xlabel or x, fontsize=12)
            ax.set_ylabel(ylabel or y, fontsize=12)
            ax.tick_params(axis="x", rotation=45)

        ax.set_title(title, fontsize=16, fontweight="bold")
        ax.grid(True, alpha=0.3, axis="y" if not horizontal else "x")

        plt.tight_layout()
        return fig, ax

    def create_heatmap(
        self,
        df: pd.DataFrame,
        title: str,
        xlabel: Optional[str] = None,
        ylabel: Optional[str] = None,
        figsize: Optional[Tuple[int, int]] = None,
        cmap: str = "RdYlGn",
        **kwargs,
    ) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create a heatmap.

        Args:
            df: DataFrame with data (will be used as heatmap matrix)
            title: Plot title
            xlabel: X-axis label
            ylabel: Y-axis label
            figsize: Figure size
            cmap: Colormap to use
            **kwargs: Additional arguments for sns.heatmap

        Returns:
            Tuple of (figure, axes)
        """
        figsize = figsize or (14, 8)
        fig, ax = plt.subplots(figsize=figsize)

        # Set default values for annot and fmt if not provided in kwargs
        if "annot" not in kwargs:
            kwargs["annot"] = True
        if "fmt" not in kwargs:
            kwargs["fmt"] = ".1f"

        sns.heatmap(df, cmap=cmap, ax=ax, **kwargs)
        ax.set_title(title, fontsize=16, fontweight="bold")
        if xlabel:
            ax.set_xlabel(xlabel, fontsize=12)
        if ylabel:
            ax.set_ylabel(ylabel, fontsize=12)

        plt.tight_layout()
        return fig, ax


class DataValidator:
    """Helper class for data validation in notebooks."""

    @staticmethod
    def check_dataframe_not_empty(df: pd.DataFrame, name: str = "DataFrame") -> bool:
        """
        Check if DataFrame is not empty.

        Args:
            df: DataFrame to check
            name: Name for error messages

        Returns:
            True if valid, raises ValueError if empty
        """
        if df is None or df.empty:
            raise ValueError(f"❌ {name} is empty or None")
        return True

    @staticmethod
    def check_columns_exist(df: pd.DataFrame, columns: List[str], name: str = "DataFrame") -> bool:
        """
        Check if required columns exist in DataFrame.

        Args:
            df: DataFrame to check
            columns: List of required column names
            name: Name for error messages

        Returns:
            True if all columns exist, raises ValueError if missing
        """
        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"❌ {name} missing required columns: {missing}")
        return True

    @staticmethod
    def check_no_nulls(df: pd.DataFrame, columns: List[str], name: str = "DataFrame") -> bool:
        """
        Check if specified columns have no null values.

        Args:
            df: DataFrame to check
            columns: List of columns to check for nulls
            name: Name for error messages

        Returns:
            True if no nulls, raises ValueError if nulls found
        """
        null_cols = [col for col in columns if df[col].isnull().any()]
        if null_cols:
            raise ValueError(f"❌ {name} has null values in columns: {null_cols}")
        return True


def setup_notebook_environment() -> Tuple[NotebookConfig, bigquery.Client, BigQueryHelper]:
    """
    Set up the notebook environment with all necessary configurations.

    Returns:
        Tuple of (config, client, helper)
    """
    # Suppress warnings
    import warnings

    warnings.filterwarnings("ignore")

    # Initialize config
    config = NotebookConfig()

    # Get BigQuery client
    client = config.get_bigquery_client()

    # Ensure project_id is set
    if not config.project_id:
        raise ValueError("GCP_PROJECT_ID must be set in environment or config")

    # Create helper
    helper = BigQueryHelper(client, config.project_id, config.dataset)

    print("✅ Notebook environment ready")
    print(f"📊 Project: {config.project_id}")
    print(f"📊 Dataset: {config.dataset}")

    return config, client, helper


def export_dataframe(
    df: pd.DataFrame,
    filename: str,
    output_dir: Optional[Path] = None,
    formats: Optional[List[str]] = None,
) -> Dict[str, Path]:
    """
    Export DataFrame to one or more formats.

    Args:
        df: DataFrame to export
        filename: Base filename (without extension)
        output_dir: Output directory (default: project_root/reports)
        formats: List of formats to export ('csv', 'xlsx', 'parquet')

    Returns:
        Dictionary mapping format to exported file path
    """
    if formats is None:
        formats = ["csv"]

    if output_dir is None:
        project_root = Path().absolute().parent
        output_dir = project_root / "reports"

    output_dir.mkdir(parents=True, exist_ok=True)
    exported = {}

    for fmt in formats:
        filepath = output_dir / f"{filename}.{fmt}"

        if fmt == "csv":
            df.to_csv(filepath, index=False)
        elif fmt == "xlsx":
            df.to_excel(filepath, index=False, engine="openpyxl")
        elif fmt == "parquet":
            df.to_parquet(filepath, index=False)
        else:
            print(f"⚠️  Unknown format: {fmt}")
            continue

        exported[fmt] = filepath
        print(f"✅ Exported to: {filepath}")

    return exported
