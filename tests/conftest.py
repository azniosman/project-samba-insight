"""
Pytest Configuration and Shared Fixtures

This file contains pytest configuration and fixtures that are shared across
all test modules.
"""

import os
from pathlib import Path
from typing import Generator

import pytest
from google.cloud import bigquery
from google.oauth2 import service_account


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def test_data_dir(project_root: Path) -> Path:
    """Return the test data directory."""
    return project_root / "tests" / "data"


@pytest.fixture(scope="session")
def gcp_project_id() -> str:
    """Return the GCP project ID from environment variables."""
    project_id = os.getenv("GCP_PROJECT_ID", "test-project")
    return project_id


@pytest.fixture(scope="session")
def bigquery_client(gcp_project_id: str) -> Generator[bigquery.Client, None, None]:
    """
    Create a BigQuery client for testing.

    This fixture will use service account credentials if available,
    otherwise it will use default credentials.
    """
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        client = bigquery.Client(project=gcp_project_id, credentials=credentials)
    else:
        # Use default credentials or mock
        pytest.skip("BigQuery credentials not available for testing")
        return

    yield client
    client.close()


@pytest.fixture
def sample_order_data() -> dict:
    """Return sample order data for testing."""
    return {
        "order_id": "test_order_123",
        "customer_id": "customer_456",
        "order_status": "delivered",
        "order_purchase_timestamp": "2018-01-01 00:00:00",
        "order_delivered_customer_date": "2018-01-10 00:00:00",
    }


@pytest.fixture
def sample_customer_data() -> dict:
    """Return sample customer data for testing."""
    return {
        "customer_id": "customer_456",
        "customer_unique_id": "unique_789",
        "customer_zip_code_prefix": "01310",
        "customer_city": "sao paulo",
        "customer_state": "SP",
    }


@pytest.fixture
def sample_product_data() -> dict:
    """Return sample product data for testing."""
    return {
        "product_id": "product_123",
        "product_category_name": "electronics",
        "product_weight_g": 500,
        "product_length_cm": 20,
        "product_height_cm": 10,
        "product_width_cm": 15,
    }


@pytest.fixture(autouse=True)
def reset_environment():
    """
    Reset environment variables after each test.

    This fixture automatically runs before and after each test to ensure
    a clean environment.
    """
    # Store original environment
    original_env = os.environ.copy()

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


# Markers for test categorization
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow")
    config.addinivalue_line("markers", "integration: marks integration tests")
    config.addinivalue_line("markers", "unit: marks unit tests")
    config.addinivalue_line("markers", "bigquery: requires BigQuery access")
    config.addinivalue_line("markers", "gcs: requires GCS access")
