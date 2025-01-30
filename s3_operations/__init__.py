"""
HPE Ezmeral Data Fabric Object Store Operations Module

This module provides a simplified interface for interacting with HPE Ezmeral Data Fabric's
S3-compatible Object Store service using boto3.
"""

from .s3_client import ObjectStoreClient, BucketManager, ObjectManager

__all__ = ['ObjectStoreClient', 'BucketManager', 'ObjectManager']
