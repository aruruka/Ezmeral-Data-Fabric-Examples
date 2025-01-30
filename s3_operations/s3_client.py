"""
Core implementation for HPE Ezmeral Data Fabric Object Store operations.
"""

import os
import logging
from typing import Optional, Iterator, Dict, Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class ObjectStoreClient:
    """Main client for Object Store operations with SSL/TLS support."""
    
    def __init__(
        self,
        endpoint_url: str = "https://mip.storage.hpecorp.net:9000",
        ca_bundle: str = "/opt/mapr/conf/ca/chain-ca.pem",
        region_name: str = "us-east-1"  # Default region for S3 compatibility
    ):
        """
        Initialize the S3 client with proper configuration.
        
        Args:
            endpoint_url: Object Store endpoint URL
            ca_bundle: Path to CA certificate bundle for SSL verification
            region_name: S3 region name (default used for compatibility)
        """
        # Validate required environment variables
        self._validate_credentials()
        
        # Configure the S3 client
        config = Config(
            signature_version='s3v4',
            retries={'max_attempts': 3}
        )
        
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            verify=ca_bundle,
            region_name=region_name,
            config=config
        )
    
    @staticmethod
    def _validate_credentials():
        """Ensure required environment variables are set."""
        required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

class BucketManager:
    """Handles bucket-level operations."""
    
    def __init__(self, client: ObjectStoreClient):
        """
        Initialize with an ObjectStoreClient instance.
        
        Args:
            client: Configured ObjectStoreClient instance
        """
        self.client = client.client
    
    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a new bucket.
        
        Args:
            bucket_name: Name of the bucket to create
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            logger.info(f"Successfully created bucket: {bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to create bucket {bucket_name}: {str(e)}")
            return False
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket. If force=True, delete all objects first.
        
        Args:
            bucket_name: Name of the bucket to delete
            force: If True, delete all objects before deleting bucket
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if force:
                # Delete all objects first
                object_manager = ObjectManager(self.client)
                object_manager.delete_all_objects(bucket_name)
            
            self.client.delete_bucket(Bucket=bucket_name)
            logger.info(f"Successfully deleted bucket: {bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete bucket {bucket_name}: {str(e)}")
            return False
    
    def list_buckets(self) -> list:
        """
        List all buckets.
        
        Returns:
            list: List of bucket names
        """
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except ClientError as e:
            logger.error(f"Failed to list buckets: {str(e)}")
            return []

class ObjectManager:
    """Handles object-level operations."""
    
    def __init__(self, client: ObjectStoreClient):
        """
        Initialize with an ObjectStoreClient instance.
        
        Args:
            client: Configured ObjectStoreClient instance
        """
        self.client = client.client
    
    def upload_file(
        self,
        bucket_name: str,
        file_path: str,
        object_key: Optional[str] = None
    ) -> bool:
        """
        Upload a file to the specified bucket.
        
        Args:
            bucket_name: Target bucket name
            file_path: Path to the file to upload
            object_key: Key to use in S3 (defaults to file name)
            
        Returns:
            bool: True if successful, False otherwise
        """
        if object_key is None:
            object_key = os.path.basename(file_path)
        
        try:
            self.client.upload_file(file_path, bucket_name, object_key)
            logger.info(
                f"Successfully uploaded {file_path} to {bucket_name}/{object_key}"
            )
            return True
        except ClientError as e:
            logger.error(
                f"Failed to upload {file_path} to {bucket_name}/{object_key}: {str(e)}"
            )
            return False
    
    def download_file(
        self,
        bucket_name: str,
        object_key: str,
        file_path: str
    ) -> bool:
        """
        Download an object to a file.
        
        Args:
            bucket_name: Source bucket name
            object_key: Key of the object to download
            file_path: Path where the file should be saved
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.client.download_file(bucket_name, object_key, file_path)
            logger.info(
                f"Successfully downloaded {bucket_name}/{object_key} to {file_path}"
            )
            return True
        except ClientError as e:
            logger.error(
                f"Failed to download {bucket_name}/{object_key}: {str(e)}"
            )
            return False
    
    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        page_size: int = 1000
    ) -> Iterator[Dict[str, Any]]:
        """
        List objects in a bucket with pagination support.
        
        Args:
            bucket_name: Name of the bucket to list objects from
            prefix: Filter objects by prefix
            page_size: Number of objects per page
            
        Yields:
            Iterator[Dict]: Object information including Key, Size, LastModified, etc.
        """
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            PaginationConfig={'PageSize': page_size}
        )
        
        try:
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        yield obj
        except ClientError as e:
            logger.error(f"Failed to list objects in {bucket_name}: {str(e)}")
            return
    
    def delete_all_objects(self, bucket_name: str) -> bool:
        """
        Delete all objects in a bucket.
        
        Args:
            bucket_name: Name of the bucket to empty
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            for obj in self.list_objects(bucket_name):
                self.client.delete_object(
                    Bucket=bucket_name,
                    Key=obj['Key']
                )
            return True
        except ClientError as e:
            logger.error(
                f"Failed to delete objects in bucket {bucket_name}: {str(e)}"
            )
            return False
