"""
Unit tests for Kaggle downloader module.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.kaggle_downloader import KaggleDownloader


class TestKaggleDownloader:
    """Test suite for KaggleDownloader class."""

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    def test_initialization(self, mock_kaggle_api):
        """Test KaggleDownloader initialization."""
        downloader = KaggleDownloader()

        assert downloader is not None
        mock_kaggle_api.return_value.authenticate.assert_called_once()

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    def test_initialization_with_custom_output_dir(self, mock_kaggle_api):
        """Test initialization with custom output directory."""
        custom_dir = Path("/tmp/custom_kaggle_data")
        downloader = KaggleDownloader(output_dir=custom_dir)

        assert downloader.output_dir == custom_dir

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    def test_download_dataset(self, mock_kaggle_api):
        """Test dataset download."""
        mock_api = mock_kaggle_api.return_value
        mock_api.dataset_download_files = MagicMock()

        downloader = KaggleDownloader()
        dataset_name = "olistbr/brazilian-ecommerce"

        with patch("src.ingestion.kaggle_downloader.zipfile.ZipFile"):
            result = downloader.download_dataset(dataset_name)

            assert result is not None
            mock_api.dataset_download_files.assert_called_once()

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    def test_download_dataset_handles_errors(self, mock_kaggle_api):
        """Test that download handles errors gracefully."""
        mock_api = mock_kaggle_api.return_value
        mock_api.dataset_download_files.side_effect = Exception("Download failed")

        downloader = KaggleDownloader()

        with pytest.raises(Exception) as exc_info:
            downloader.download_dataset("invalid/dataset")

        assert "Download failed" in str(exc_info.value)

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    @patch("src.ingestion.kaggle_downloader.Path.exists")
    def test_dataset_already_downloaded_skip(self, mock_exists, mock_kaggle_api):
        """Test skipping download if dataset already exists."""
        mock_exists.return_value = True

        downloader = KaggleDownloader()

        # Should return existing path without downloading
        result = downloader.download_dataset("olistbr/brazilian-ecommerce", skip_if_exists=True)

        assert result is not None

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    def test_extract_zip_file(self, mock_kaggle_api):
        """Test ZIP file extraction."""
        downloader = KaggleDownloader()

        with patch("src.ingestion.kaggle_downloader.zipfile.ZipFile") as mock_zip:
            mock_zip_instance = MagicMock()
            mock_zip.return_value.__enter__.return_value = mock_zip_instance

            zip_path = Path("/tmp/test.zip")
            extract_dir = Path("/tmp/extracted")

            # Should not raise exception
            downloader._extract_zip(zip_path, extract_dir)

            mock_zip_instance.extractall.assert_called_once_with(extract_dir)


class TestKaggleDownloaderIntegration:
    """Integration tests for Kaggle downloader."""

    @patch("src.ingestion.kaggle_downloader.KaggleApi")
    def test_full_download_workflow(self, mock_kaggle_api):
        """Test complete download workflow."""
        mock_api = mock_kaggle_api.return_value
        mock_api.dataset_download_files = MagicMock()

        downloader = KaggleDownloader()

        with patch("src.ingestion.kaggle_downloader.zipfile.ZipFile"):
            with patch("src.ingestion.kaggle_downloader.Path.exists", return_value=False):
                result = downloader.download_dataset("test/dataset")

                assert result is not None
