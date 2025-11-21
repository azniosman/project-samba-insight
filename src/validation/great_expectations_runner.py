"""
Great Expectations Validation Runner

Runs Great Expectations validation suites against BigQuery warehouse tables.
Generates data quality reports and alerts on validation failures.
"""

import sys
from pathlib import Path
from typing import Dict, Optional

# Add project root to path BEFORE imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import get_config
from src.utils.logger import setup_logging

try:
    from great_expectations.data_context import FileDataContext

    import great_expectations as gx
except ImportError:
    print("Great Expectations not installed. Install with: pip install great_expectations")
    sys.exit(1)


logger = setup_logging(__name__)


class GreatExpectationsRunner:
    """
    Runner for Great Expectations data validation.

    Connects to BigQuery warehouse and runs expectation suites
    to validate data quality beyond dbt tests.
    """

    def __init__(self, context_root_dir: Optional[Path] = None):
        """
        Initialize Great Expectations context.

        Args:
            context_root_dir: Path to GE context directory
        """
        self.config = get_config()

        if context_root_dir is None:
            context_root_dir = project_root / "great_expectations"

        self.context_root_dir = context_root_dir

        # Initialize GE context
        try:
            self.context = gx.get_context(context_root_dir=str(context_root_dir))  # type: ignore[attr-defined]
            logger.info(f"Loaded Great Expectations context from {context_root_dir}")

            # Add BigQuery datasource if not exists
            self._setup_datasource()
        except Exception as e:
            logger.error(f"Failed to load GE context: {e}")
            raise

    def _setup_datasource(self):
        """Setup BigQuery datasource using fluent API."""
        try:
            # Check if datasource exists
            self.context.data_sources.get("bigquery_warehouse")
            logger.info("BigQuery datasource already configured")
        except Exception:
            # Create new datasource
            logger.info("Creating BigQuery datasource")
            self.context.data_sources.add_sql(
                name="bigquery_warehouse",
                connection_string=f"bigquery://{self.config.gcp_project_id}/{self.config.environment}_warehouse_warehouse",
            )
            logger.info("BigQuery datasource created")

    def validate_table(
        self, table_name: str, expectation_suite_name: str, schema_name: Optional[str] = None
    ) -> Dict:
        """
        Validate a specific table against an expectation suite.

        Args:
            table_name: Name of the table to validate
            expectation_suite_name: Name of the expectation suite
            schema_name: BigQuery schema/dataset name

        Returns:
            Validation results dictionary
        """
        if schema_name is None:
            schema_name = f"{self.config.environment}_warehouse_warehouse"

        logger.info(f"Validating {schema_name}.{table_name} with suite {expectation_suite_name}")

        try:
            # Get datasource
            datasource = self.context.data_sources.get("bigquery_warehouse")

            # Add data asset if not exists
            try:
                asset = datasource.get_asset(table_name)
            except Exception:
                asset = datasource.add_table_asset(
                    name=table_name, table_name=table_name, schema_name=schema_name
                )

            # Get batch
            batch_request = asset.build_batch_request()

            # Get or create expectation suite
            try:
                self.context.suites.get(expectation_suite_name)
            except Exception:
                # Suite doesn't exist, create a basic one
                logger.warning(f"Suite {expectation_suite_name} not found, creating basic suite")
                self.context.suites.add(expectation_suite_name)

            # Run validation directly on batch
            validator = self.context.get_validator(
                batch_request=batch_request, expectation_suite_name=expectation_suite_name
            )

            results = validator.validate()

            # Log results
            success = results.success
            if success:
                logger.info(f"✅ Validation passed for {table_name}")
            else:
                logger.warning(f"❌ Validation failed for {table_name}")

            return {"success": success, "results": results}

        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            return {"success": False, "error": str(e)}

    def validate_all_tables(self) -> Dict[str, Dict]:
        """
        Validate all configured tables with their expectation suites.

        Returns:
            Dictionary mapping table names to validation results
        """
        validations = {
            "fact_orders": "fact_orders_suite",
            "mart_sales_daily": "mart_sales_daily_suite",
        }

        results = {}

        for table_name, suite_name in validations.items():
            try:
                result = self.validate_table(table_name, suite_name)
                results[table_name] = result
            except Exception as e:
                logger.error(f"Failed to validate {table_name}: {e}")
                results[table_name] = {"success": False, "error": str(e)}

        # Summary
        passed = sum(1 for r in results.values() if r.get("success", False))
        total = len(results)

        logger.info(f"Validation Summary: {passed}/{total} tables passed")

        return results

    def generate_data_docs(self):
        """Generate Great Expectations data documentation site."""
        logger.info("Building Great Expectations data docs...")

        try:
            self.context.build_data_docs()
            docs_path = (
                self.context_root_dir / "uncommitted" / "data_docs" / "local_site" / "index.html"
            )
            logger.info(f"✅ Data docs built successfully: {docs_path}")
            return docs_path
        except Exception as e:
            logger.error(f"Failed to build data docs: {e}")
            raise


def main():
    """Main entry point for Great Expectations validation."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Great Expectations data validation")
    parser.add_argument(
        "--table", help="Specific table to validate (validates all if not specified)"
    )
    parser.add_argument("--suite", help="Expectation suite name (required if --table is specified)")
    parser.add_argument("--build-docs", action="store_true", help="Build data documentation site")

    args = parser.parse_args()

    # Initialize runner
    runner = GreatExpectationsRunner()

    if args.table:
        if not args.suite:
            print("Error: --suite is required when --table is specified")
            sys.exit(1)

        # Validate single table
        result = runner.validate_table(args.table, args.suite)

        if not result["success"]:
            sys.exit(1)
    else:
        # Validate all tables
        results = runner.validate_all_tables()

        # Exit with error if any validations failed
        if not all(r.get("success", False) for r in results.values()):
            sys.exit(1)

    # Build data docs if requested
    if args.build_docs:
        runner.generate_data_docs()

    print("✅ Great Expectations validation complete")


if __name__ == "__main__":
    main()
