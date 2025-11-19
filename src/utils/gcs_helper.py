"""
Google Cloud Storage Helper Module

Common utilities for working with Google Cloud Storage.
"""

from pathlib import Path
from typing import List, Optional, Union

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from .config import get_config
from .logger import get_logger

logger = get_logger(__name__)


class GCSHelper:
    """Helper class for Google Cloud Storage operations."""

    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Initialize GCS helper.

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
                scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
            )
            self.client = storage.Client(project=self.project_id, credentials=credentials)
        else:
            # Use default credentials
            self.client = storage.Client(project=self.project_id)

        logger.info("gcs_client_initialized", project_id=self.project_id)

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.

        Args:
            bucket_name: Bucket name

        Returns:
            True if bucket exists, False otherwise
        """
        try:
            self.client.get_bucket(bucket_name)
            return True
        except NotFound:
            return False

    def create_bucket(
        self,
        bucket_name: str,
        location: str = "US",
        storage_class: str = "STANDARD",
        exists_ok: bool = True,
    ) -> storage.Bucket:
        """
        Create a GCS bucket.

        Args:
            bucket_name: Bucket name
            location: Bucket location
            storage_class: Storage class (STANDARD, NEARLINE, COLDLINE, ARCHIVE)
            exists_ok: If True, don't raise error if bucket exists

        Returns:
            Created or existing bucket
        """
        if exists_ok and self.bucket_exists(bucket_name):
            logger.info("bucket_already_exists", bucket_name=bucket_name)
            return self.client.get_bucket(bucket_name)

        try:
            bucket = self.client.bucket(bucket_name)
            bucket.storage_class = storage_class
            bucket = self.client.create_bucket(bucket, location=location)

            logger.info(
                "bucket_created",
                bucket_name=bucket_name,
                location=location,
                storage_class=storage_class,
            )
            return bucket
        except Exception as e:
            logger.error("bucket_creation_failed", bucket_name=bucket_name, error=str(e))
            raise

    def upload_file(
        self,
        local_path: Union[str, Path],
        bucket_name: str,
        blob_name: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> storage.Blob:
        """
        Upload a file to GCS.

        Args:
            local_path: Path to local file
            bucket_name: Target bucket name
            blob_name: Name for blob in GCS. If None, uses filename
            content_type: Content type. If None, auto-detected

        Returns:
            Uploaded blob
        """
        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f"File not found: {local_path}")

        if blob_name is None:
            blob_name = local_path.name

        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        logger.info(
            "uploading_file",
            local_path=str(local_path),
            bucket_name=bucket_name,
            blob_name=blob_name,
        )

        try:
            if content_type:
                blob.upload_from_filename(str(local_path), content_type=content_type)
            else:
                blob.upload_from_filename(str(local_path))

            logger.info(
                "file_uploaded",
                local_path=str(local_path),
                bucket_name=bucket_name,
                blob_name=blob_name,
                size_bytes=blob.size,
            )
            return blob
        except Exception as e:
            logger.error(
                "file_upload_failed",
                local_path=str(local_path),
                bucket_name=bucket_name,
                blob_name=blob_name,
                error=str(e),
            )
            raise

    def download_file(
        self,
        bucket_name: str,
        blob_name: str,
        local_path: Union[str, Path],
    ) -> Path:
        """
        Download a file from GCS.

        Args:
            bucket_name: Source bucket name
            blob_name: Blob name in GCS
            local_path: Target local path

        Returns:
            Path to downloaded file
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        logger.info(
            "downloading_file",
            bucket_name=bucket_name,
            blob_name=blob_name,
            local_path=str(local_path),
        )

        try:
            blob.download_to_filename(str(local_path))

            logger.info(
                "file_downloaded",
                bucket_name=bucket_name,
                blob_name=blob_name,
                local_path=str(local_path),
            )
            return local_path
        except Exception as e:
            logger.error(
                "file_download_failed",
                bucket_name=bucket_name,
                blob_name=blob_name,
                local_path=str(local_path),
                error=str(e),
            )
            raise

    def list_blobs(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> List[storage.Blob]:
        """
        List blobs in a bucket.

        Args:
            bucket_name: Bucket name
            prefix: Filter by prefix
            delimiter: Delimiter for hierarchical listing

        Returns:
            List of blobs
        """
        bucket = self.client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix, delimiter=delimiter))

        logger.info(
            "blobs_listed",
            bucket_name=bucket_name,
            prefix=prefix,
            count=len(blobs),
        )
        return blobs

    def blob_exists(self, bucket_name: str, blob_name: str) -> bool:
        """
        Check if a blob exists.

        Args:
            bucket_name: Bucket name
            blob_name: Blob name

        Returns:
            True if blob exists, False otherwise
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return bool(blob.exists())

    def delete_blob(self, bucket_name: str, blob_name: str, not_found_ok: bool = True) -> None:
        """
        Delete a blob.

        Args:
            bucket_name: Bucket name
            blob_name: Blob name
            not_found_ok: If True, don't raise error if blob doesn't exist
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        try:
            blob.delete()
            logger.info("blob_deleted", bucket_name=bucket_name, blob_name=blob_name)
        except NotFound:
            if not not_found_ok:
                raise
            logger.warning(
                "blob_not_found_for_deletion",
                bucket_name=bucket_name,
                blob_name=blob_name,
            )
        except Exception as e:
            logger.error(
                "blob_deletion_failed",
                bucket_name=bucket_name,
                blob_name=blob_name,
                error=str(e),
            )
            raise

    def get_blob_uri(self, bucket_name: str, blob_name: str) -> str:
        """
        Get GCS URI for a blob.

        Args:
            bucket_name: Bucket name
            blob_name: Blob name

        Returns:
            GCS URI (gs://bucket/blob)
        """
        return f"gs://{bucket_name}/{blob_name}"
