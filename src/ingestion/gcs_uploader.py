"""
GCS Uploader Module

Uploads data files to Google Cloud Storage.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

# Add project root to path if running as standalone script
if __name__ == "__main__" or __package__ is None:
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Use absolute imports (sys.path configured above for standalone execution)
from src.utils.config import get_config
from src.utils.gcs_helper import GCSHelper
from src.utils.logger import get_logger

logger = get_logger(__name__)


class GCSUploader:
    """Uploads data files to Google Cloud Storage."""

    def __init__(self, bucket_name: Optional[str] = None):
        """
        Initialize GCS uploader.

        Args:
            bucket_name: GCS bucket name. If None, uses project-{project_id}-data
        """
        config = get_config()
        self.gcs_helper = GCSHelper()

        # Determine bucket name
        if bucket_name is None:
            bucket_name = f"{config.gcp_project_id}-data"

        self.bucket_name = bucket_name

        # Create bucket if it doesn't exist
        if not self.gcs_helper.bucket_exists(bucket_name):
            logger.info("creating_bucket", bucket_name=bucket_name)
            self.gcs_helper.create_bucket(bucket_name, exists_ok=True)

        logger.info("gcs_uploader_initialized", bucket_name=bucket_name)

    def upload_file(
        self,
        local_path: Union[str, Path],
        gcs_path: Optional[str] = None,
        add_timestamp: bool = False,
    ) -> str:
        """
        Upload a single file to GCS.

        Args:
            local_path: Path to local file
            gcs_path: Path in GCS. If None, uses filename
            add_timestamp: If True, add timestamp to path

        Returns:
            GCS URI (gs://bucket/path)
        """
        local_path = Path(local_path)

        if gcs_path is None:
            gcs_path = local_path.name

        # Add timestamp if requested
        if add_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gcs_path = f"{timestamp}/{gcs_path}"

        # Upload file
        self.gcs_helper.upload_file(
            local_path=local_path,
            bucket_name=self.bucket_name,
            blob_name=gcs_path,
        )

        uri = self.gcs_helper.get_blob_uri(self.bucket_name, gcs_path)
        logger.info("file_uploaded_to_gcs", local_path=str(local_path), gcs_uri=uri)

        return uri

    def upload_directory(
        self,
        local_dir: Union[str, Path],
        gcs_prefix: str = "",
        pattern: str = "*.csv",
        add_timestamp: bool = False,
    ) -> List[str]:
        """
        Upload all files matching pattern from a directory to GCS.

        Args:
            local_dir: Local directory path
            gcs_prefix: Prefix for GCS paths
            pattern: File pattern to match (e.g., "*.csv")
            add_timestamp: If True, add timestamp to paths

        Returns:
            List of GCS URIs for uploaded files
        """
        local_dir = Path(local_dir)

        if not local_dir.exists():
            raise FileNotFoundError(f"Directory not found: {local_dir}")

        # Find matching files
        files = list(local_dir.glob(pattern))

        if not files:
            logger.warning("no_files_found", local_dir=str(local_dir), pattern=pattern)
            return []

        logger.info(
            "uploading_directory",
            local_dir=str(local_dir),
            pattern=pattern,
            files_count=len(files),
        )

        # Add timestamp if requested
        if add_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if gcs_prefix:
                gcs_prefix = f"{gcs_prefix}/{timestamp}"
            else:
                gcs_prefix = timestamp

        # Upload each file
        uris = []
        for file_path in files:
            gcs_path = f"{gcs_prefix}/{file_path.name}" if gcs_prefix else file_path.name
            uri = self.upload_file(file_path, gcs_path, add_timestamp=False)
            uris.append(uri)

        logger.info(
            "directory_uploaded",
            local_dir=str(local_dir),
            files_uploaded=len(uris),
        )

        return uris

    def upload_kaggle_data(
        self,
        kaggle_data_dir: Optional[Union[str, Path]] = None,
        add_timestamp: bool = True,
    ) -> List[str]:
        """
        Upload Kaggle Brazilian E-Commerce data to GCS.

        Args:
            kaggle_data_dir: Directory containing Kaggle data. If None, uses default.
            add_timestamp: If True, organize by timestamp

        Returns:
            List of GCS URIs for uploaded files
        """
        config = get_config()

        if kaggle_data_dir is None:
            kaggle_data_dir = config.data_raw_dir / "brazilian-ecommerce"

        kaggle_data_dir = Path(kaggle_data_dir)

        logger.info("uploading_kaggle_data", kaggle_data_dir=str(kaggle_data_dir))

        # Upload all CSV files
        uris = self.upload_directory(
            local_dir=kaggle_data_dir,
            gcs_prefix="raw/brazilian-ecommerce",
            pattern="*.csv",
            add_timestamp=add_timestamp,
        )

        return uris


def main():
    """Main function for standalone execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Upload data files to Google Cloud Storage")
    parser.add_argument(
        "--bucket",
        help="GCS bucket name",
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Single file to upload",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        help="Directory to upload",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="File pattern to match (default: *.csv)",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="GCS path prefix",
    )
    parser.add_argument(
        "--timestamp",
        action="store_true",
        help="Add timestamp to paths",
    )
    parser.add_argument(
        "--kaggle",
        action="store_true",
        help="Upload Kaggle Brazilian E-Commerce data",
    )

    args = parser.parse_args()

    # Initialize uploader
    uploader = GCSUploader(bucket_name=args.bucket)

    # Upload Kaggle data
    if args.kaggle:
        uris = uploader.upload_kaggle_data(add_timestamp=args.timestamp)
        print(f"\nUploaded {len(uris)} files:")
        for uri in uris:
            print(f"  - {uri}")
        return

    # Upload single file
    if args.file:
        uri = uploader.upload_file(args.file, add_timestamp=args.timestamp)
        print(f"\nFile uploaded to: {uri}")
        return

    # Upload directory
    if args.directory:
        uris = uploader.upload_directory(
            local_dir=args.directory,
            gcs_prefix=args.prefix,
            pattern=args.pattern,
            add_timestamp=args.timestamp,
        )
        print(f"\nUploaded {len(uris)} files:")
        for uri in uris:
            print(f"  - {uri}")
        return

    print("Error: Specify --file, --directory, or --kaggle")
    parser.print_help()


if __name__ == "__main__":
    from src.utils.logger import setup_logging

    setup_logging("gcs_uploader")
    main()
