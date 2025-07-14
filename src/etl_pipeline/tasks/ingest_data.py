from src.config  import load_config, logger
from src.etl_pipeline.utils import get_s3_client, generate_s3_key, DataType
import os
import botocore.exceptions

class IngestData:
    """Class to handle ingestion of local files to MinIO bucket."""

    def __init__(self):
        config = load_config()
        self.bucket_name = config["MINIO_BUCKET"]
        self.local_file_path = config["RAW_LOCAL_FILE"]
        self.s3_client = get_s3_client()
        self.object_key = generate_s3_key(data_type=DataType.RAW)

    def ingest_raw_data(self):
        """Upload the local file to MinIO, creating bucket if missing."""
        try:
            logger.info(f"Checking for file at {self.local_file_path}")
            if not os.path.exists(self.local_file_path):
                raise FileNotFoundError(f"File does not exist: {self.local_file_path}")
            
            # Check and create bucket
            buckets = [b['Name'] for b in self.s3_client.list_buckets()['Buckets']]

            if self.bucket_name not in buckets:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
                        
            logger.info(f"Uploading to s3://{self.bucket_name}/{self.object_key}")
            self.s3_client.upload_file(self.local_file_path, self.bucket_name, self.object_key)
            logger.info("Upload successful")

        except botocore.exceptions.ClientError as e:
            logger.error(f"MinIO Client error: {e}")
            raise
        except Exception as e:
            logger.exception(f"Upload failed: {e}")

