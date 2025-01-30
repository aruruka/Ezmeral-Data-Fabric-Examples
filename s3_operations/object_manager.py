"""AWS S3 Object Management Module.

This module provides functionality for managing objects in Amazon S3 buckets.
It includes support for upload, download, and listing operations with features like
multipart transfers and progress tracking.

The primary class ObjectManager offers a high-level interface for S3 operations
with built-in error handling and logging.

Dependencies:
    - boto3: AWS SDK for Python
    - botocore: Core functionality of boto3

Example:
    from object_manager import ObjectManager
    s3_client = boto3.client('s3')
    manager = ObjectManager(s3_client)
"""

import os
import logging
from typing import List, Iterator, Optional
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig


class ObjectManager:
    """Manages S3 object operations including upload, download, and listing.

    This class provides high-level methods for managing S3 objects with support for
    multipart transfers, progress tracking, and paginated listings.
    """

    def __init__(self, s3_client):
        """Initialize the object manager.

        Args:
            s3_client: A boto3 S3 client instance configured for the target endpoint.
        """
        self.client = s3_client
        self.logger = logging.getLogger(__name__)
        self.transfer_config = TransferConfig(
            multipart_threshold=1024 * 25, max_concurrency=10  # 25MB
        )

    def upload_file(
        self, bucket: str, file_path: str, object_name: Optional[str] = None
    ) -> bool:
        """Upload a file to an S3 bucket with progress tracking.

        Args:
            bucket (str): Name of the destination bucket.
            file_path (str): Local path to the file to upload.
            object_name (str, optional): Name to give the object in S3.
                If not provided, uses the basename of file_path.

        Returns:
            bool: True if upload was successful, False otherwise.
        """
        if not object_name:
            object_name = os.path.basename(file_path)

        try:
            with open(file_path, "rb") as file:
                self.client.upload_fileobj(
                    file,
                    bucket,
                    object_name,
                    Config=self.transfer_config,
                    Callback=self._transfer_progress("Upload"),
                )
            self.logger.info(
                "File %s uploaded to %s/%s", file_path, bucket, object_name
            )
            return True
        except (ClientError, IOError) as e:
            self.logger.error("Upload failed: %s", str(e))
            return False

    def download_file(self, bucket: str, object_name: str, download_path: str) -> bool:
        """Download an object from an S3 bucket with progress tracking.

        Args:
            bucket (str): Name of the source bucket.
            object_name (str): Name of the object to download.
            download_path (str): Local path where the file should be saved.

        Returns:
            bool: True if download was successful, False otherwise.
        """
        try:
            with open(download_path, "wb") as file:
                self.client.download_fileobj(
                    bucket,
                    object_name,
                    file,
                    Config=self.transfer_config,
                    Callback=self._transfer_progress("Download"),
                )
            self.logger.info("File %s downloaded to %s", object_name, download_path)
            return True
        except (ClientError, IOError) as e:
            self.logger.error("Download failed: %s", str(e))
            return False

    def list_objects_paginated(
        self, bucket: str, prefix: str = ""
    ) -> Iterator[List[dict]]:
        """List objects in an S3 bucket with pagination support.

        Args:
            bucket (str): Name of the bucket to list objects from.
            prefix (str, optional): Filter results to objects with this prefix.
                Defaults to empty string (list all objects).

        Returns:
            Iterator[List[dict]]: Iterator yielding lists of object keys per page.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 100}
        )

        for page in page_iterator:
            if "Contents" in page:
                yield [obj["Key"] for obj in page["Contents"]]

    def _transfer_progress(self, operation: str):
        """Create a callback function for tracking transfer progress.

        Args:
            operation (str): Name of the operation (Upload/Download) for progress message.

        Returns:
            callable: Callback function that prints progress information.
        """

        def callback(bytes_transferred):
            print(
                f"{operation} progress: {bytes_transferred} bytes transferred", end="\r"
            )

        return callback
