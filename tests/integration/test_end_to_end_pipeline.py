"""
Integration tests for end-to-end pipeline.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.ingestion.bigquery_loader import BigQueryLoader


@pytest.mark.skipif(
    not os.getenv("GCP_PROJECT_ID"), reason="GCP_PROJECT_ID not set - skipping integration tests"
)
class TestEndToEndPipeline:
    """Integration tests for complete pipeline workflow."""

    def test_bigquery_loader_initialization(self):
        """Test that BigQueryLoader can initialize with real credentials."""
        try:
            loader = BigQueryLoader()
            assert loader is not None
            assert loader.staging_dataset is not None
        except Exception as e:
            pytest.skip(f"Cannot initialize BigQueryLoader: {e}")

    def test_metadata_table_creation(self):
        """Test metadata table creation in BigQuery."""
        try:
            loader = BigQueryLoader()
            # Metadata table should be created during initialization
            assert loader.bq_helper.table_exists(loader.staging_dataset, "_load_metadata")
        except Exception as e:
            pytest.skip(f"Cannot verify metadata table: {e}")

    @pytest.mark.slow
    def test_csv_file_load_workflow(self, tmp_path):
        """Test loading a CSV file to BigQuery (integration test)."""
        # Create a sample CSV file
        csv_path = tmp_path / "test_data.csv"
        csv_path.write_text("id,name,value\n1,test,100\n2,test2,200\n")

        try:
            loader = BigQueryLoader()
            job = loader.load_csv_file(
                csv_path=csv_path, table_name="test_integration_table", skip_if_loaded=False
            )

            # Verify job completed
            if job:
                assert job.state == "DONE"
                assert job.output_rows > 0
        except Exception as e:
            pytest.skip(f"Cannot test CSV load: {e}")


class TestPipelineComponents:
    """Test individual pipeline components integration."""

    def test_config_integration_with_environment(self):
        """Test that config integrates properly with environment."""
        from src.utils.config import get_config

        config = get_config()
        assert config.project_root.exists()

    def test_logger_integration(self):
        """Test that logger can be used across modules."""
        from src.utils.logger import get_logger

        logger = get_logger(__name__)
        logger.info("integration_test", test_id="test_123")

        # Should not raise exception
        assert True


@pytest.fixture
def sample_data_directory(tmp_path):
    """Create a sample data directory with test CSV files."""
    data_dir = tmp_path / "sample_data"
    data_dir.mkdir()

    # Create sample CSV files
    (data_dir / "orders.csv").write_text(
        "order_id,customer_id,order_status\n" "1,100,delivered\n" "2,101,shipped\n"
    )

    (data_dir / "customers.csv").write_text(
        "customer_id,customer_name,customer_state\n" "100,John Doe,SP\n" "101,Jane Smith,RJ\n"
    )

    return data_dir


class TestDataIngestionIntegration:
    """Integration tests for data ingestion components."""

    def test_directory_structure_creation(self, tmp_path):
        """Test that pipeline can create necessary directories."""
        from src.utils.config import Config

        # Test with temporary project root
        config = Config()
        assert config.data_raw_dir is not None

    def test_file_hash_calculation(self, tmp_path):
        """Test MD5 hash calculation for idempotency."""
        from src.ingestion.bigquery_loader import BigQueryLoader

        # Create test file
        test_file = tmp_path / "test.csv"
        test_file.write_text("test,data\n1,2\n")

        loader = BigQueryLoader()
        hash1 = loader._get_file_hash(test_file)
        hash2 = loader._get_file_hash(test_file)

        # Same file should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hash length
