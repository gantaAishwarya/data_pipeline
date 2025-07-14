import boto3
from src.config import load_config
from datetime import datetime, timezone
from enum import Enum

class DataType(Enum):
    """Enum to represent the data processing stage."""
    RAW = "raw"
    PROCESSED = "processed"

def get_s3_client() -> boto3.client:
    """Create and return a boto3 S3 client configured for MinIO."""
    config = load_config()
    return boto3.client(
        "s3",
        endpoint_url=config["MINIO_ENDPOINT"],
        aws_access_key_id=config["MINIO_ACCESS_KEY"],
        aws_secret_access_key=config["MINIO_SECRET_KEY"],
    )

def generate_s3_key(data_type: DataType) -> str:
    """Generate S3 key path based on the data type and current UTC date."""
    now = datetime.now(timezone.utc)
    prefix = data_type.value  # "raw" or "processed"
    filename = f"{prefix}_logs.json"
    return now.strftime(f"{prefix}/json/%Y/%m/%d/{filename}")