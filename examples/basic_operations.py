import logging
import tempfile
from datetime import datetime
from s3_operations.s3_client import S3Client
from s3_operations.bucket_manager import BucketManager
from s3_operations.object_manager import ObjectManager

def main():
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
            if object_manager.download_file(test_bucket, "test-file.txt", download_path):
                print(f"\nFile downloaded to: {download_path}")
    
    finally:
        # Cleanup
        if bucket_manager.delete_bucket(test_bucket):
            print(f"\nTemporary bucket {test_bucket} deleted")
        else:
            print(f"\nWarning: Failed to delete temporary bucket {test_bucket}")

if __name__ == "__main__":
    main()
