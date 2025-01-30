"""
HPE Ezmeral Data Fabric Object Store Bucket Management Module

This module provides a high-level interface for managing S3 bucket operations in HPE Ezmeral 
Data Fabric's Object Store. It works in conjunction with the S3Client module to provide 
bucket-level operations with integrated logging and error handling.

Features:
    - Create new S3 buckets
    - Delete existing buckets
    - List all available buckets
    - Integrated logging for operation tracking
    - Comprehensive error handling with boto3 exceptions

Usage:
    from s3_operations.s3_client import S3Client
    from s3_operations.bucket_manager import BucketManager

    # Initialize the client and bucket manager
    s3_client = S3Client()
    bucket_mgr = BucketManager(s3_client.get_client())

    # Create a new bucket
    bucket_mgr.create_bucket('my-bucket')

    # List all buckets
    buckets = bucket_mgr.list_buckets()

    # Delete a bucket
    bucket_mgr.delete_bucket('my-bucket')

Requirements:
    - boto3
    - botocore
    - Configured S3Client instance with valid credentials

Note:
    This module is part of the s3_operations package and is designed to work
    alongside ObjectManager for complete S3 functionality. While BucketManager
    handles bucket-level operations, use ObjectManager for object-level operations
    within buckets.
"""

import logging
from typing import List
from botocore.exceptions import ClientError


class BucketManager:
    """A class to manage S3 bucket operations.

    This class provides methods to create, delete and list S3 buckets using a boto3 S3 client.
    All operations are logged using the standard Python logging module.

    Attributes:
        client: A boto3 S3 client instance configured for the target endpoint
        logger: A logging instance for this class

    Methods:
        create_bucket(bucket_name): Creates a new S3 bucket
        delete_bucket(bucket_name): Deletes an existing S3 bucket
        list_buckets(): Lists all available S3 buckets

    Example:
        s3_client = boto3.client('s3')
        bucket_manager = BucketManager(s3_client)
        bucket_manager.create_bucket('my-bucket')
    """

    def __init__(self, s3_client):
        """Initialize the bucket manager.

        Args:
            s3_client: A boto3 S3 client instance configured for the target endpoint.
        """
        self.client = s3_client
        self.logger = logging.getLogger(__name__)

    def create_bucket(self, bucket_name: str) -> bool:
        """Create a new S3 bucket.

        Args:
            bucket_name (str): Name of the bucket to create.

        Returns:
            bool: True if bucket creation was successful, False otherwise.
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            self.logger.info("Bucket %s created successfully", bucket_name)
            return True
        except ClientError as e:
            self.logger.error(
                "Bucket creation failed: %s", e.response["Error"]["Message"]
            )
            return False

    def delete_bucket(self, bucket_name: str) -> bool:
        """Delete an existing S3 bucket.

        Args:
            bucket_name (str): Name of the bucket to delete.

        Returns:
            bool: True if bucket deletion was successful, False otherwise.
        """
        try:
            self.client.delete_bucket(Bucket=bucket_name)
            self.logger.info("Bucket %s deleted successfully", bucket_name)
            return True
        except ClientError as e:
            self.logger.error(
                "Bucket deletion failed: %s", e.response["Error"]["Message"]
            )
            return False

    def list_buckets(self) -> List[str]:
        """List all available S3 buckets.

        Returns:
            List[str]: A list of bucket names. Returns empty list if operation fails.
        """
        try:
            response = self.client.list_buckets()
            return [bucket["Name"] for bucket in response["Buckets"]]
        except ClientError as e:
            self.logger.error(
                "Bucket listing failed: %s", e.response["Error"]["Message"]
            )
            return []
