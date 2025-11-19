"""
Kaggle Data Downloader Module

Downloads Brazilian E-Commerce dataset from Kaggle.
"""

import os
import zipfile
from pathlib import Path
from typing import Optional

from kaggle.api.kaggle_api_extended import KaggleApi

from ..utils.config import get_config
from ..utils.logger import get_logger

logger = get_logger(__name__)


class KaggleDownloader:
    """Downloads datasets from Kaggle."""

    # Brazilian E-Commerce Public Dataset by Olist
    DEFAULT_DATASET = "olistbr/brazilian-ecommerce"

    def __init__(self, download_dir: Optional[Path] = None):
        """
        Initialize Kaggle downloader.

        Args:
            download_dir: Directory to download files to. If None, uses config.
        """
        config = get_config()

        if not config.kaggle_configured:
            raise ValueError(
                "Kaggle credentials not configured. "
                "Please set KAGGLE_USERNAME and KAGGLE_KEY in your .env file."
            )

        self.download_dir = download_dir or config.data_raw_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Set Kaggle credentials from environment
        if config.kaggle_username:
            os.environ["KAGGLE_USERNAME"] = config.kaggle_username
        if config.kaggle_key:
            os.environ["KAGGLE_KEY"] = config.kaggle_key

        # Initialize Kaggle API
        self.api = KaggleApi()
        self.api.authenticate()

        logger.info("kaggle_downloader_initialized", download_dir=str(self.download_dir))

    def download_dataset(
        self,
        dataset: str = DEFAULT_DATASET,
        unzip: bool = True,
        force: bool = False,
    ) -> Path:
        """
        Download a Kaggle dataset.

        Args:
            dataset: Dataset identifier (owner/dataset-name)
            unzip: If True, extract zip files after download
            force: If True, re-download even if files exist

        Returns:
            Path to downloaded/extracted files
        """
        logger.info("downloading_dataset", dataset=dataset, unzip=unzip, force=force)

        # Check if files already exist
        dataset_name = dataset.split("/")[-1]
        dataset_dir = self.download_dir / dataset_name

        if dataset_dir.exists() and not force:
            csv_files = list(dataset_dir.glob("*.csv"))
            if csv_files:
                logger.info(
                    "dataset_already_exists",
                    dataset=dataset,
                    dataset_dir=str(dataset_dir),
                    files_count=len(csv_files),
                )
                return dataset_dir

        # Create dataset directory
        dataset_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Download dataset
            logger.info("downloading_from_kaggle", dataset=dataset)
            self.api.dataset_download_files(
                dataset,
                path=str(dataset_dir),
                unzip=unzip,
                quiet=False,
            )

            # If unzip is False, extract manually
            if not unzip:
                zip_files = list(dataset_dir.glob("*.zip"))
                for zip_file in zip_files:
                    logger.info("extracting_zip", zip_file=str(zip_file))
                    with zipfile.ZipFile(zip_file, "r") as zip_ref:
                        zip_ref.extractall(dataset_dir)
                    # Remove zip file after extraction
                    zip_file.unlink()

            # List downloaded files
            csv_files = list(dataset_dir.glob("*.csv"))
            logger.info(
                "dataset_downloaded",
                dataset=dataset,
                dataset_dir=str(dataset_dir),
                files_count=len(csv_files),
                files=[f.name for f in csv_files],
            )

            return dataset_dir

        except Exception as e:
            logger.error("dataset_download_failed", dataset=dataset, error=str(e))
            raise

    def list_dataset_files(self, dataset: str = DEFAULT_DATASET) -> list:
        """
        List files in a Kaggle dataset without downloading.

        Args:
            dataset: Dataset identifier

        Returns:
            List of file names
        """
        try:
            files = self.api.dataset_list_files(dataset).files
            file_names = [f.name for f in files]
            logger.info("dataset_files_listed", dataset=dataset, files=file_names)
            return file_names
        except Exception as e:
            logger.error("list_files_failed", dataset=dataset, error=str(e))
            raise

    def get_dataset_metadata(self, dataset: str = DEFAULT_DATASET) -> dict:
        """
        Get metadata about a Kaggle dataset.

        Args:
            dataset: Dataset identifier

        Returns:
            Dictionary with dataset metadata
        """
        try:
            metadata = self.api.dataset_view(dataset)
            info = {
                "title": metadata.title,
                "subtitle": metadata.subtitle,
                "creator_name": metadata.creatorName,
                "total_bytes": metadata.totalBytes,
                "url": metadata.url,
                "last_updated": str(metadata.lastUpdated),
                "download_count": metadata.downloadCount,
                "vote_count": metadata.voteCount,
            }
            logger.info("dataset_metadata_retrieved", dataset=dataset, title=info["title"])
            return info
        except Exception as e:
            logger.error("metadata_retrieval_failed", dataset=dataset, error=str(e))
            raise


def main():
    """Main function for standalone execution."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Brazilian E-Commerce dataset from Kaggle"
    )
    parser.add_argument(
        "--dataset",
        default=KaggleDownloader.DEFAULT_DATASET,
        help="Kaggle dataset identifier",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files exist",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="List files in dataset without downloading",
    )
    parser.add_argument(
        "--metadata",
        action="store_true",
        help="Show dataset metadata",
    )

    args = parser.parse_args()

    # Initialize downloader
    downloader = KaggleDownloader()

    # List files
    if args.list_files:
        files = downloader.list_dataset_files(args.dataset)
        print("\nDataset files:")
        for f in files:
            print(f"  - {f}")
        return

    # Show metadata
    if args.metadata:
        metadata = downloader.get_dataset_metadata(args.dataset)
        print("\nDataset metadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")
        return

    # Download dataset
    dataset_dir = downloader.download_dataset(args.dataset, force=args.force)
    print(f"\nDataset downloaded to: {dataset_dir}")

    # List CSV files
    csv_files = list(dataset_dir.glob("*.csv"))
    print(f"\nDownloaded {len(csv_files)} CSV files:")
    for csv_file in csv_files:
        size_mb = csv_file.stat().st_size / (1024 * 1024)
        print(f"  - {csv_file.name} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    from ..utils.logger import setup_logging

    setup_logging("kaggle_downloader")
    main()
