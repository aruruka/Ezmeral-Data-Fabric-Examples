import os
import logging
from typing import List, Iterator
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

class ObjectManager:
    def __init__(self, s3_client):
        self.client = s3_client
        self.logger = logging.getLogger(__name__)
        self.transfer_config = TransferConfig(
            multipart_threshold=1024 * 25,  # 25MB
            max_concurrency=10
        )

    def upload_file(self, bucket: str, file_path: str, object_name: str = None) -> bool:
        if not object_name:
            object_name = os.path.basename(file_path)

        try:
            with open(file_path, 'rb') as file:
                self.client.upload_fileobj(
                    file,
                    bucket,
                    object_name,
                    Config=self.transfer_config,
                    Callback=self._transfer_progress('Upload')
                )
            self.logger.info(f"File {file_path} uploaded to {bucket}/{object_name}")
            return True
        except (ClientError, IOError) as e:
            self.logger.error(f"Upload failed: {str(e)}")
            return False

    def download_file(self, bucket: str, object_name: str, download_path: str) -> bool:
        try:
            with open(download_path, 'wb') as file:
                self.client.download_fileobj(
                    bucket,
                    object_name,
                    file,
                    Config=self.transfer_config,
                    Callback=self._transfer_progress('Download')
                )
            self.logger.info(f"File {object_name} downloaded to {download_path}")
            return True
        except (ClientError, IOError) as e:
            self.logger.error(f"Download failed: {str(e)}")
            return False

    def list_objects_paginated(self, bucket: str, prefix: str = '') -> Iterator[List[str]]:
        paginator = self.client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={'PageSize': 100}
        )

        for page in page_iterator:
            if 'Contents' in page:
                yield [obj['Key'] for obj in page['Contents']]

    def _transfer_progress(self, operation: str):
        def callback(bytes_transferred):
            print(f"{operation} progress: {bytes_transferred} bytes transferred", end='\r')
        return callback
