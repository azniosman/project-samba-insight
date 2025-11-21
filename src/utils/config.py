"""
Configuration Management Module

Loads and manages environment variables and application configuration.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


class Config:
    """Application configuration loaded from environment variables."""

    def __init__(self, env_file: Optional[Path] = None) -> None:
        """
        Initialize configuration.

        Args:
            env_file: Path to .env file. If None, searches for .env in project root.
        """
        # Load environment variables
        if env_file is None:
            # Search for .env in project root
            current_dir = Path(__file__).resolve()
            project_root = current_dir.parent.parent.parent
            env_file = project_root / ".env"

        if env_file.exists():
            load_dotenv(env_file)

        # GCP Configuration (optional - only required for BigQuery operations)
        self.gcp_project_id: Optional[str] = os.getenv("GCP_PROJECT_ID")
        self.google_application_credentials: Optional[str] = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )

        # Kaggle Configuration
        self.kaggle_username: Optional[str] = os.getenv("KAGGLE_USERNAME")
        self.kaggle_key: Optional[str] = os.getenv("KAGGLE_KEY")

        # Environment
        self.environment: str = os.getenv("ENVIRONMENT", "dev")

        # BigQuery Configuration
        self.bq_database: Optional[str] = os.getenv("BQ_DATABASE", self.gcp_project_id)

        # BigQuery Dataset Names (Schemas)
        self.bq_dataset_raw: str = os.getenv("BQ_DATASET_RAW", "staging")
        self.bq_dataset_staging: str = os.getenv("BQ_DATASET_STAGING", "staging")
        self.bq_dataset_warehouse: str = os.getenv("BQ_DATASET_WAREHOUSE", "warehouse")

        # Logging
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")

        # Project Paths
        self.project_root: Path = Path(__file__).resolve().parent.parent.parent
        self.data_dir: Path = self.project_root / "data"
        self.data_raw_dir: Path = self.data_dir / "raw"
        self.data_processed_dir: Path = self.data_dir / "processed"
        self.data_external_dir: Path = self.data_dir / "external"
        self.logs_dir: Path = self.project_root / "logs"

        # Validate configuration
        self._validate_config()

        # Ensure directories exist
        self._ensure_directories()

    def _get_required(self, key: str) -> str:
        """
        Get required environment variable.

        Args:
            key: Environment variable name

        Returns:
            Environment variable value

        Raises:
            ValueError: If environment variable is not set
        """
        value = os.getenv(key)
        if value is None:
            raise ValueError(
                f"Required environment variable '{key}' is not set. "
                f"Please check your .env file."
            )
        return value

    def _validate_config(self) -> None:
        """Validate configuration values."""
        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(
                f"Invalid LOG_LEVEL '{self.log_level}'. "
                f"Must be one of: {', '.join(valid_log_levels)}"
            )

        # Validate GCP credentials path exists if specified
        if self.google_application_credentials:
            creds_path = Path(self.google_application_credentials)
            if not creds_path.exists():
                raise ValueError(
                    f"GOOGLE_APPLICATION_CREDENTIALS file not found: {self.google_application_credentials}"
                )

    def _ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.data_dir,
            self.data_raw_dir,
            self.data_processed_dir,
            self.data_external_dir,
            self.logs_dir,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    @property
    def kaggle_configured(self) -> bool:
        """Check if Kaggle credentials are configured."""
        return bool(self.kaggle_username and self.kaggle_key)

    @property
    def gcp_configured(self) -> bool:
        """Check if GCP credentials are configured."""
        return bool(
            self.gcp_project_id
            and self.google_application_credentials
            and Path(self.google_application_credentials).exists()
        )

    def __repr__(self) -> str:
        """String representation of configuration (hides sensitive data)."""
        return (
            f"Config("
            f"environment='{self.environment}', "
            f"gcp_project_id='{self.gcp_project_id}', "
            f"bq_database='{self.bq_database}', "
            f"bq_dataset_raw='{self.bq_dataset_raw}', "
            f"bq_dataset_staging='{self.bq_dataset_staging}', "
            f"bq_dataset_warehouse='{self.bq_dataset_warehouse}', "
            f"log_level='{self.log_level}', "
            f"kaggle_configured={self.kaggle_configured}, "
            f"gcp_configured={self.gcp_configured}"
            f")"
        )


# Global configuration instance
_config: Optional[Config] = None


def get_config(reload: bool = False) -> Config:
    """
    Get global configuration instance (singleton pattern).

    Args:
        reload: If True, reload configuration from environment

    Returns:
        Config instance
    """
    global _config
    if _config is None or reload:
        _config = Config()
    return _config
