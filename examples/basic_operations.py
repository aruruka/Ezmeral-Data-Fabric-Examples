"""
Example script demonstrating basic operations with the HPE Ezmeral Data Fabric Object Store.
"""

import os
import logging
from s3_operations import ObjectStoreClient, BucketManager, ObjectManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Demonstrate basic Object Store operations."""
    
    # Initialize the client
    client = ObjectStoreClient(
        endpoint_url="https://mip.storage.hpecorp.net:9000",
        ca_bundle="/opt/mapr/conf/ca/chain-ca.pem"
    )
    
    # Initialize managers
    bucket_manager = BucketManager(client)
    object_manager = ObjectManager(client)
    
    # Test bucket operations
    test_bucket = "edf-test-bucket"
    
    # List existing buckets
    logger.info("Listing existing buckets:")
    buckets = bucket_manager.list_buckets()
    for bucket in buckets:
        logger.info(f"- {bucket}")
    
    # Create a test bucket
    logger.info(f"\nCreating bucket: {test_bucket}")
    if bucket_manager.create_bucket(test_bucket):
        # Upload a test file
        test_file = "test_upload.txt"
        with open(test_file, "w") as f:
            f.write("This is a test file for HPE EDF Object Store demo.")
        
        logger.info(f"\nUploading {test_file} to {test_bucket}")
        object_manager.upload_file(test_bucket, test_file)
        
        # List objects in the bucket
        logger.info(f"\nListing objects in {test_bucket}:")
        for obj in object_manager.list_objects(test_bucket):
            logger.info(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
        
        # Download the file with a different name
        download_path = "test_download.txt"
        logger.info(f"\nDownloading {test_file} to {download_path}")
        object_manager.download_file(test_bucket, test_file, download_path)
        
        # Clean up
        logger.info("\nCleaning up test files and bucket")
        os.remove(test_file)
        os.remove(download_path)
        bucket_manager.delete_bucket(test_bucket, force=True)
        
        logger.info("\nDemo completed successfully!")
    else:
        logger.error("Failed to create test bucket. Demo aborted.")

if __name__ == "__main__":
    main()
