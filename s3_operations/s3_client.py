import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

class S3Client:
    def __init__(self, ca_path: str = "/opt/mapr/conf/ca/chain-ca.pem"):
        self.endpoint = "https://mip.storage.hpecorp.net:9000"
        self.ca_path = ca_path
        
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            config=Config(
                s3={'addressing_style': 'virtual'},
                retries={'max_attempts': 3, 'mode': 'standard'}
            ),
            verify=self.ca_path
        )

    def get_client(self):
        return self.client
