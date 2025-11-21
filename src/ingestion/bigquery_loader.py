"""
BigQuery Staging Loader Module

Loads raw data from local files or GCS to BigQuery staging tables.
Implements idempotent loading with metadata tracking.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd
from google.cloud import bigquery

# Add project root to path if running as standalone script
if __name__ == "__main__" or __package__ is None:
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Use absolute imports (sys.path configured above for standalone execution)
from src.utils.bigquery_helper import BigQueryHelper
from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BigQueryLoader:
    """Loads data to BigQuery staging tables with idempotency."""

    # Brazilian E-Commerce dataset table mappings
    TABLE_MAPPINGS = {
        "olist_customers_dataset.csv": "customers_raw",
        "olist_geolocation_dataset.csv": "geolocation_raw",
        "olist_order_items_dataset.csv": "order_items_raw",
        "olist_order_payments_dataset.csv": "order_payments_raw",
        "olist_order_reviews_dataset.csv": "order_reviews_raw",
        "olist_orders_dataset.csv": "orders_raw",
        "olist_products_dataset.csv": "products_raw",
        "olist_sellers_dataset.csv": "sellers_raw",
        "product_category_name_translation.csv": "product_category_translation_raw",
    }

    def __init__(self, staging_dataset: Optional[str] = None):
        """
        Initialize BigQuery loader.

        Args:
            staging_dataset: Staging dataset name. If None, uses config.
        """
        config = get_config()
        self.bq_helper = BigQueryHelper()
        self.staging_dataset = staging_dataset or config.bq_dataset_staging

        # Create staging dataset if it doesn't exist
        if not self.bq_helper.dataset_exists(self.staging_dataset):
            logger.info("creating_staging_dataset", dataset=self.staging_dataset)
            self.bq_helper.create_dataset(
                dataset_id=self.staging_dataset,
                description="Staging dataset for raw Brazilian E-Commerce data",
            )

        # Create metadata table if it doesn't exist
        self._ensure_metadata_table()

        logger.info("bigquery_loader_initialized", staging_dataset=self.staging_dataset)

    def _ensure_metadata_table(self) -> None:
        """Create load metadata table if it doesn't exist."""
        table_id = "_load_metadata"

        if self.bq_helper.table_exists(self.staging_dataset, table_id):
            return

        schema = [
            bigquery.SchemaField("load_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows_loaded", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("file_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        ]

        self.bq_helper.create_table(
            dataset_id=self.staging_dataset,
            table_id=table_id,
            schema=schema,
        )

        logger.info("metadata_table_created", table_id=table_id)

    def _record_load_metadata(
        self,
        table_name: str,
        source_file: str,
        rows_loaded: int,
        file_hash: Optional[str] = None,
        status: str = "SUCCESS",
    ) -> None:
        """
        Record load metadata.

        Args:
            table_name: Target table name
            source_file: Source file path or URI
            rows_loaded: Number of rows loaded
            file_hash: Hash of source file for idempotency
            status: Load status (SUCCESS, FAILED)
        """
        load_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        metadata_df = pd.DataFrame(
            [
                {
                    "load_id": load_id,
                    "table_name": table_name,
                    "source_file": str(source_file),
                    "rows_loaded": rows_loaded,
                    "load_timestamp": datetime.now(),
                    "file_hash": file_hash,
                    "status": status,
                }
            ]
        )

        self.bq_helper.load_dataframe(
            df=metadata_df,
            dataset_id=self.staging_dataset,
            table_id="_load_metadata",
            write_disposition="WRITE_APPEND",
        )

    def _get_file_hash(self, file_path: Path) -> str:
        """
        Calculate MD5 hash of a file.

        Args:
            file_path: Path to file

        Returns:
            MD5 hash string
        """
        import hashlib

        hash_md5 = hashlib.md5(
            usedforsecurity=False
        )  # nosec B324 - used for file checksums, not security
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_already_loaded(self, table_name: str, file_hash: str) -> bool:
        """
        Check if file has already been loaded (idempotency check).

        Args:
            table_name: Target table name
            file_hash: Hash of source file

        Returns:
            True if already loaded with same hash, False otherwise
        """
        sql = f"""
        SELECT COUNT(*) as count
        FROM `{self.bq_helper.project_id}.{self.staging_dataset}._load_metadata`
        WHERE table_name = @table_name
          AND file_hash = @file_hash
          AND status = 'SUCCESS'
        """

        try:
            # Use parameterized query to prevent SQL injection
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
                    bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash),
                ]
            )
            result = self.bq_helper.query(sql, as_dataframe=True, job_config=job_config)
            return bool(result.iloc[0]["count"] > 0)
        except Exception:
            # If query fails, assume not loaded
            return False

    def load_csv_file(
        self,
        csv_path: Union[str, Path],
        table_name: Optional[str] = None,
        skip_if_loaded: bool = True,
    ) -> Optional[bigquery.LoadJob]:
        """
        Load a CSV file to BigQuery staging.

        Args:
            csv_path: Path to CSV file
            table_name: Target table name. If None, inferred from filename
            skip_if_loaded: If True, skip if file already loaded

        Returns:
            Load job or None if skipped
        """
        csv_path = Path(csv_path)

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Infer table name if not provided
        if table_name is None:
            filename = csv_path.name
            table_name = self.TABLE_MAPPINGS.get(filename, filename.replace(".csv", "_raw"))

        logger.info("loading_csv", csv_path=str(csv_path), table_name=table_name)

        # Check if already loaded (idempotency)
        if skip_if_loaded:
            file_hash = self._get_file_hash(csv_path)
            if self._is_already_loaded(table_name, file_hash):
                logger.info(
                    "file_already_loaded",
                    csv_path=str(csv_path),
                    table_name=table_name,
                    file_hash=file_hash,
                )
                return None

        try:
            # Load CSV to BigQuery
            job = self.bq_helper.load_csv(
                csv_path=csv_path,
                dataset_id=self.staging_dataset,
                table_id=table_name,
                write_disposition="WRITE_TRUNCATE",  # Replace data
            )

            # Record metadata
            self._record_load_metadata(
                table_name=table_name,
                source_file=str(csv_path),
                rows_loaded=job.output_rows or 0,
                file_hash=file_hash if skip_if_loaded else None,
                status="SUCCESS",
            )

            logger.info(
                "csv_loaded_successfully",
                csv_path=str(csv_path),
                table_name=table_name,
                rows=job.output_rows,
            )

            return job

        except Exception as e:
            # Record failure
            self._record_load_metadata(
                table_name=table_name,
                source_file=str(csv_path),
                rows_loaded=0,
                status="FAILED",
            )

            logger.error(
                "csv_load_failed",
                csv_path=str(csv_path),
                table_name=table_name,
                error=str(e),
            )
            raise

    def load_directory(
        self,
        directory: Union[str, Path],
        pattern: str = "*.csv",
        skip_if_loaded: bool = True,
    ) -> Dict[str, Optional[bigquery.LoadJob]]:
        """
        Load all CSV files from a directory.

        Args:
            directory: Directory containing CSV files
            pattern: File pattern to match
            skip_if_loaded: If True, skip files already loaded

        Returns:
            Dictionary mapping filenames to load jobs
        """
        directory = Path(directory)

        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        # Find CSV files
        csv_files = list(directory.glob(pattern))

        if not csv_files:
            logger.warning("no_csv_files_found", directory=str(directory), pattern=pattern)
            return {}

        logger.info(
            "loading_directory",
            directory=str(directory),
            files_count=len(csv_files),
        )

        # Load each file
        results = {}
        for csv_file in csv_files:
            try:
                job = self.load_csv_file(csv_file, skip_if_loaded=skip_if_loaded)
                results[csv_file.name] = job
            except Exception as e:
                logger.error("file_load_error", file=csv_file.name, error=str(e))
                results[csv_file.name] = None

        # Summary
        loaded_count = sum(1 for job in results.values() if job is not None)
        skipped_count = sum(1 for job in results.values() if job is None)

        logger.info(
            "directory_load_completed",
            total_files=len(csv_files),
            loaded=loaded_count,
            skipped=skipped_count,
        )

        return results

    def load_kaggle_data(
        self,
        kaggle_data_dir: Optional[Union[str, Path]] = None,
        skip_if_loaded: bool = True,
    ) -> Dict[str, Optional[bigquery.LoadJob]]:
        """
        Load all Brazilian E-Commerce Kaggle data to staging.

        Args:
            kaggle_data_dir: Directory containing Kaggle CSV files. If None, uses default.
            skip_if_loaded: If True, skip files already loaded

        Returns:
            Dictionary mapping filenames to load jobs
        """
        config = get_config()

        if kaggle_data_dir is None:
            kaggle_data_dir = config.data_raw_dir / "brazilian-ecommerce"

        kaggle_data_dir = Path(kaggle_data_dir)

        logger.info("loading_kaggle_data", directory=str(kaggle_data_dir))

        return self.load_directory(
            directory=kaggle_data_dir,
            pattern="*.csv",
            skip_if_loaded=skip_if_loaded,
        )


def main():
    """Main function for standalone execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Load data to BigQuery staging tables")
    parser.add_argument(
        "--file",
        type=Path,
        help="Single CSV file to load",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        help="Directory containing CSV files to load",
    )
    parser.add_argument(
        "--table",
        help="Target table name (auto-detected if not specified)",
    )
    parser.add_argument(
        "--dataset",
        help="Staging dataset name (uses config default if not specified)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reload even if already loaded",
    )
    parser.add_argument(
        "--kaggle",
        action="store_true",
        help="Load Kaggle Brazilian E-Commerce data",
    )

    args = parser.parse_args()

    # Initialize loader
    loader = BigQueryLoader(staging_dataset=args.dataset)

    skip_if_loaded = not args.force

    # Load Kaggle data
    if args.kaggle:
        results = loader.load_kaggle_data(skip_if_loaded=skip_if_loaded)
        print(f"\nLoaded {sum(1 for j in results.values() if j)} of {len(results)} files")
        return

    # Load single file
    if args.file:
        job = loader.load_csv_file(
            csv_path=args.file,
            table_name=args.table,
            skip_if_loaded=skip_if_loaded,
        )
        if job:
            print(f"\nLoaded {job.output_rows} rows to {args.table or 'auto-detected table'}")
        else:
            print("\nFile already loaded (skipped)")
        return

    # Load directory
    if args.directory:
        results = loader.load_directory(directory=args.directory, skip_if_loaded=skip_if_loaded)
        print(f"\nLoaded {sum(1 for j in results.values() if j)} of {len(results)} files")
        return

    print("Error: Specify --file, --directory, or --kaggle")
    parser.print_help()


if __name__ == "__main__":
    from src.utils.logger import setup_logging

    setup_logging("bigquery_loader")
    main()
