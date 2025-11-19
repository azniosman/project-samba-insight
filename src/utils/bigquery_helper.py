"""
BigQuery Helper Module

Common utilities for working with Google BigQuery.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from .config import get_config
from .logger import get_logger

logger = get_logger(__name__)


class BigQueryHelper:
    """Helper class for BigQuery operations."""

    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Initialize BigQuery helper.

        Args:
            project_id: GCP project ID. If None, uses config.
            credentials_path: Path to service account JSON. If None, uses config.
        """
        config = get_config()
        self.project_id = project_id or config.gcp_project_id
        self.credentials_path = credentials_path or config.google_application_credentials

        # Initialize client
        if self.credentials_path and Path(self.credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            self.client = bigquery.Client(project=self.project_id, credentials=credentials)
        else:
            # Use default credentials
            self.client = bigquery.Client(project=self.project_id)

        logger.info("bigquery_client_initialized", project_id=self.project_id)

    def dataset_exists(self, dataset_id: str) -> bool:
        """
        Check if a dataset exists.

        Args:
            dataset_id: Dataset ID

        Returns:
            True if dataset exists, False otherwise
        """
        try:
            self.client.get_dataset(dataset_id)
            return True
        except NotFound:
            return False

    def create_dataset(
        self,
        dataset_id: str,
        location: str = "US",
        description: Optional[str] = None,
        exists_ok: bool = True,
    ) -> bigquery.Dataset:
        """
        Create a BigQuery dataset.

        Args:
            dataset_id: Dataset ID
            location: Dataset location
            description: Dataset description
            exists_ok: If True, don't raise error if dataset exists

        Returns:
            Created or existing dataset
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location

        if description:
            dataset.description = description

        try:
            dataset = self.client.create_dataset(dataset, exists_ok=exists_ok)
            logger.info(
                "dataset_created",
                dataset_id=dataset_id,
                location=location,
                exists_ok=exists_ok,
            )
            return dataset
        except Exception as e:
            logger.error("dataset_creation_failed", dataset_id=dataset_id, error=str(e))
            raise

    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        Check if a table exists.

        Args:
            dataset_id: Dataset ID
            table_id: Table ID

        Returns:
            True if table exists, False otherwise
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: List[bigquery.SchemaField],
        exists_ok: bool = True,
    ) -> bigquery.Table:
        """
        Create a BigQuery table.

        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            schema: Table schema
            exists_ok: If True, don't raise error if table exists

        Returns:
            Created or existing table
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = bigquery.Table(table_ref, schema=schema)

        try:
            table = self.client.create_table(table, exists_ok=exists_ok)
            logger.info("table_created", dataset_id=dataset_id, table_id=table_id)
            return table
        except Exception as e:
            logger.error(
                "table_creation_failed",
                dataset_id=dataset_id,
                table_id=table_id,
                error=str(e),
            )
            raise

    def load_dataframe(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
        create_dataset: bool = True,
    ) -> bigquery.LoadJob:
        """
        Load a pandas DataFrame to BigQuery.

        Args:
            df: DataFrame to load
            dataset_id: Target dataset ID
            table_id: Target table ID
            write_disposition: Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
            create_dataset: If True, create dataset if it doesn't exist

        Returns:
            Completed load job
        """
        # Create dataset if needed
        if create_dataset and not self.dataset_exists(dataset_id):
            self.create_dataset(dataset_id)

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,  # Auto-detect schema
        )

        logger.info(
            "loading_dataframe",
            dataset_id=dataset_id,
            table_id=table_id,
            rows=len(df),
            write_disposition=write_disposition,
        )

        try:
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for job to complete

            logger.info(
                "dataframe_loaded",
                dataset_id=dataset_id,
                table_id=table_id,
                rows_loaded=job.output_rows,
            )
            return job
        except Exception as e:
            logger.error(
                "dataframe_load_failed",
                dataset_id=dataset_id,
                table_id=table_id,
                error=str(e),
            )
            raise

    def load_csv(
        self,
        csv_path: Union[str, Path],
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
        skip_leading_rows: int = 1,
        autodetect: bool = True,
        create_dataset: bool = True,
    ) -> bigquery.LoadJob:
        """
        Load a CSV file to BigQuery.

        Args:
            csv_path: Path to CSV file
            dataset_id: Target dataset ID
            table_id: Target table ID
            write_disposition: Write disposition
            skip_leading_rows: Number of rows to skip (usually 1 for header)
            autodetect: Auto-detect schema
            create_dataset: If True, create dataset if it doesn't exist

        Returns:
            Completed load job
        """
        # Create dataset if needed
        if create_dataset and not self.dataset_exists(dataset_id):
            self.create_dataset(dataset_id)

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            autodetect=autodetect,
            write_disposition=write_disposition,
            allow_quoted_newlines=True,  # Allow newlines within quoted fields
        )

        logger.info(
            "loading_csv",
            csv_path=str(csv_path),
            dataset_id=dataset_id,
            table_id=table_id,
        )

        try:
            with open(csv_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )
                job.result()  # Wait for job to complete

            logger.info(
                "csv_loaded",
                csv_path=str(csv_path),
                dataset_id=dataset_id,
                table_id=table_id,
                rows_loaded=job.output_rows,
            )
            return job
        except Exception as e:
            logger.error(
                "csv_load_failed",
                csv_path=str(csv_path),
                dataset_id=dataset_id,
                table_id=table_id,
                error=str(e),
            )
            raise

    def query(self, sql: str, as_dataframe: bool = True) -> Union[pd.DataFrame, bigquery.QueryJob]:
        """
        Execute a SQL query.

        Args:
            sql: SQL query to execute
            as_dataframe: If True, return results as DataFrame

        Returns:
            Query results as DataFrame or QueryJob
        """
        logger.info("executing_query", sql_preview=sql[:100])

        try:
            query_job = self.client.query(sql)
            result = query_job.result()  # Wait for query to complete

            logger.info(
                "query_completed",
                total_bytes_processed=query_job.total_bytes_processed,
                total_bytes_billed=query_job.total_bytes_billed,
            )

            if as_dataframe:
                return result.to_dataframe()
            return query_job
        except Exception as e:
            logger.error("query_failed", error=str(e), sql=sql[:200])
            raise

    def delete_table(self, dataset_id: str, table_id: str, not_found_ok: bool = True) -> None:
        """
        Delete a table.

        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            not_found_ok: If True, don't raise error if table doesn't exist
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        try:
            self.client.delete_table(table_ref, not_found_ok=not_found_ok)
            logger.info("table_deleted", dataset_id=dataset_id, table_id=table_id)
        except Exception as e:
            logger.error(
                "table_deletion_failed",
                dataset_id=dataset_id,
                table_id=table_id,
                error=str(e),
            )
            raise

    def get_table_info(self, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """
        Get table metadata.

        Args:
            dataset_id: Dataset ID
            table_id: Table ID

        Returns:
            Dictionary with table metadata
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)

        return {
            "project_id": table.project,
            "dataset_id": table.dataset_id,
            "table_id": table.table_id,
            "created": table.created,
            "modified": table.modified,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "schema": [{"name": field.name, "type": field.field_type} for field in table.schema],
        }
