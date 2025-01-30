import logging
from typing import List
from botocore.exceptions import ClientError

class BucketManager:
    def __init__(self, s3_client):
        self.client = s3_client
        self.logger = logging.getLogger(__name__)

    def create_bucket(self, bucket_name: str) -> bool:
        try:
            self.client.create_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket {bucket_name} created successfully")
            return True
        except ClientError as e:
            self.logger.error(f"Bucket creation failed: {e.response['Error']['Message']}")
            return False

    def delete_bucket(self, bucket_name: str) -> bool:
        try:
            self.client.delete_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket {bucket_name} deleted successfully")
            return True
        except ClientError as e:
            self.logger.error(f"Bucket deletion failed: {e.response['Error']['Message']}")
            return False

    def list_buckets(self) -> List[str]:
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except ClientError as e:
            self.logger.error(f"Bucket listing failed: {e.response['Error']['Message']}")
            return []
