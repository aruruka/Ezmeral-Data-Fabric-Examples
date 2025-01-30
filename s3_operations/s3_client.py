"""
HPE Ezmeral Data Fabric Object Store S3 Client Module

This module provides a wrapper for the boto3 S3 client specifically configured for
HPE Ezmeral Data Fabric's S3-compatible Object Store service. It handles secure
connections and provides a simplified interface for S3 operations.

Features:
    - Preconfigured endpoint for HPE Ezmeral Data Fabric Object Store
    - SSL/TLS support with custom CA certificate validation
    - Virtual-style addressing configuration
    - Automatic retry handling with configurable attempts
    - Environment variable-based authentication (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

Configuration:
    The client is configured with the following defaults:
    - Endpoint URL: https://mip.storage.hpecorp.net:9000
    - CA Bundle Path: /opt/mapr/conf/ca/chain-ca.pem
    - Virtual-style addressing
    - 3 retry attempts in standard mode

Usage:
    from s3_operations.s3_client import S3Client
    
    # Initialize with default settings
    s3_client = S3Client()
    
    # Or customize the CA certificate path
    s3_client = S3Client(ca_path="/custom/path/to/ca.pem")
    
    # Get the underlying boto3 client for operations
    boto3_client = s3_client.get_client()

Requirements:
    - boto3
    - botocore
    - Valid HPE Ezmeral Data Fabric Object Store credentials
    - Access to the HPE Ezmeral Data Fabric Object Store endpoint

Note:
    Ensure environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    are properly set before using this client.
"""

import boto3
from botocore.config import Config


class S3Client:
    """A wrapper class for boto3 S3 client with custom endpoint configuration.

    This class provides an interface to interact with S3-compatible storage services
    by configuring a boto3 client with custom endpoint, virtual-style addressing,
    and retry settings.
    """

    def __init__(self, ca_path: str = "/opt/mapr/conf/ca/chain-ca.pem"):
        """Initialize the S3 client with custom configuration.

        Args:
            ca_path (str, optional): Path to the CA certificate chain file.
                Defaults to "/opt/mapr/conf/ca/chain-ca.pem".
        """
        self.endpoint = "https://mip.storage.hpecorp.net:9000"
        self.ca_path = ca_path

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            config=Config(
                s3={"addressing_style": "virtual"},
                retries={"max_attempts": 3, "mode": "standard"},
            ),
            verify=self.ca_path,
        )

    def get_client(self):
        """Get the configured boto3 S3 client instance.

        Returns:
            boto3.client: Configured S3 client instance ready for use.
        """
        return self.client
