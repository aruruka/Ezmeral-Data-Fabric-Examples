# HPE Ezmeral Data Fabric Object Store Examples

This project demonstrates interaction with HPE Ezmeral Data Fabric's S3-compatible Object Store service using Python and the Boto3 SDK.

## Features

- SSL/TLS support with custom CA certificate
- Environment variable-based authentication
- Bucket operations (create, delete, list)
- Object operations (upload, download, list with pagination)
- Automatic cleanup and error handling

## Prerequisites

- Python 3.6+
- Access to an HPE Ezmeral Data Fabric Object Store instance
- Object Store access credentials (Access Key and Secret Key)
- Object Store CA certificate (for SSL/TLS verification)

## Installation

1. Clone this repository:

    ```bash
    git clone [repository-url]
    cd [repository-name]
    ```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up environment variables:

    ```bash
    export AWS_ACCESS_KEY_ID="your-access-key"
    export AWS_SECRET_ACCESS_KEY="your-secret-key"
    ```

## Configuration

The Object Store client is configured with the following defaults:

- Endpoint URL: <https://mip.storage.hpecorp.net:9000>
- CA Bundle Path: /opt/mapr/conf/ca/chain-ca.pem
- Region: us-east-1 (for S3 compatibility)

You can override these settings when initializing the client:

```python
from s3_operations import ObjectStoreClient

client = ObjectStoreClient(
    endpoint_url="your-endpoint-url",
    ca_bundle="/path/to/your/ca-bundle.pem",
    region_name="your-region"
)
```

## Usage Examples

The `examples/basic_operations.py` script demonstrates common operations:

Run the example script:

```bash
python examples/basic_operations.py
```

## Error Handling

All operations include error handling and logging. Enable debug logging for more detailed output:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

Please feel free to submit issues and pull requests for improvements.

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License](https://creativecommons.org/licenses/by-nc/4.0/).
