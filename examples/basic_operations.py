"""
Basic operations demonstration script for S3-compatible object storage.

This module demonstrates fundamental S3 operations using custom wrapper classes,
including bucket creation/deletion and object upload/download operations. It serves
as an example of how to interact with S3-compatible storage systems using the
provided S3 operation classes.

Operations demonstrated:
    - S3 client initialization
    - Bucket creation with unique timestamp-based naming
    - File upload to S3 bucket
    - Object listing with pagination support
    - File download from S3 bucket
    - Automatic bucket cleanup

Dependencies:
    - s3_operations.s3_client
    - s3_operations.bucket_manager
    - s3_operations.object_manager

Usage:
    Run this script directly to see a complete workflow of S3 operations:
    $ python basic_operations.py

Notes:
    - Creates a temporary test bucket with timestamp suffix
    - Automatically cleans up created resources after execution
    - Uses temporary files for demonstration purposes
"""


import logging
import tempfile
from datetime import datetime
from s3_operations.s3_client import S3Client
from s3_operations.bucket_manager import BucketManager
from s3_operations.object_manager import ObjectManager


def main():
    """
    Main function that performs basic S3 operations.

    - Sets up logging.
    - Initializes S3 client and managers.
    - Creates a test bucket with a unique name.
    - Uploads a temporary test file.
    - Lists objects in the bucket with pagination.
    - Downloads the uploaded file.
    - Cleans up by deleting the temporary bucket.
    """
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Initialize clients
    s3_client = S3Client().get_client()
    bucket_manager = BucketManager(s3_client)
    object_manager = ObjectManager(s3_client)

    # Create test bucket
    test_bucket = f"test-bucket-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    if not bucket_manager.create_bucket(test_bucket):
        return

    try:
        # File operations
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"Test content")
            tmp_path = tmp_file.name

        # Upload test file
        if object_manager.upload_file(test_bucket, tmp_path, "test-file.txt"):
            # List objects (with pagination example)
            print("\nBucket contents:")
            for page in object_manager.list_objects_paginated(test_bucket):
                for obj in page:
                    print(f" - {obj}")

            # Download test file
            download_path = "/tmp/downloaded_test_file.txt"
            if object_manager.download_file(
                test_bucket, "test-file.txt", download_path
            ):
                print(f"\nFile downloaded to: {download_path}")

    finally:
        # Cleanup
        if bucket_manager.delete_bucket(test_bucket):
            print(f"\nTemporary bucket {test_bucket} deleted")
        else:
            print(f"\nWarning: Failed to delete temporary bucket {test_bucket}")


if __name__ == "__main__":
    main()
