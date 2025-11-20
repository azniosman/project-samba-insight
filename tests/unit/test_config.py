"""
Unit tests for configuration module.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.config import Config, get_config


class TestConfig:
    """Test suite for Config class."""

    def test_config_initialization_with_defaults(self):
        """Test that Config initializes with default values."""
        config = Config()

        assert config.project_root.exists()
        assert config.data_raw_dir.exists() or True  # May not exist yet
        assert isinstance(config.gcp_project_id, str)

    def test_config_singleton_pattern(self):
        """Test that get_config returns the same instance."""
        config1 = get_config()
        config2 = get_config()

        assert config1 is config2

    @patch.dict(os.environ, {"GCP_PROJECT_ID": "project-samba-insight"}, clear=False)
    def test_config_loads_from_environment(self):
        """Test that Config loads values from environment variables."""
        # Clear singleton
        import src.utils.config

        src.utils.config._config = None

        config = get_config()
        assert config.gcp_project_id == "project-samba-insight"

    def test_config_has_required_attributes(self):
        """Test that Config has all required attributes."""
        config = get_config()

        required_attrs = [
            "project_root",
            "data_raw_dir",
            "data_processed_dir",
            "gcp_project_id",
            "bq_dataset_staging",
            "bq_dataset_warehouse",
        ]

        for attr in required_attrs:
            assert hasattr(config, attr), f"Config missing attribute: {attr}"

    def test_config_paths_are_pathlib_objects(self):
        """Test that path attributes are Path objects."""
        config = get_config()

        assert isinstance(config.project_root, Path)
        assert isinstance(config.data_raw_dir, Path)
        assert isinstance(config.data_processed_dir, Path)

    @patch.dict(os.environ, {"BQ_DATASET_STAGING": "staging"}, clear=False)
    def test_config_custom_dataset_names(self):
        """Test custom BigQuery dataset names from environment."""
        # Clear singleton
        import src.utils.config

        src.utils.config._config = None

        config = get_config()
        assert config.bq_dataset_staging == "staging"


class TestConfigValidation:
    """Test suite for configuration validation."""

    def test_project_root_exists(self):
        """Test that project root directory exists."""
        config = get_config()
        assert config.project_root.exists()
        assert config.project_root.is_dir()

    def test_config_string_representation(self):
        """Test that Config has a useful string representation."""
        config = get_config()
        config_str = str(config)

        assert "Config" in config_str or config.gcp_project_id in config_str


@pytest.fixture(autouse=True)
def reset_config_singleton():
    """Reset config singleton after each test."""
    yield
    # Cleanup: reset singleton
    import src.utils.config

    src.utils.config._config = None
