import pandas as pd
import io
from src.etl_pipeline.utils import get_s3_client, generate_s3_key, DataType
from src.config  import load_config, logger
from botocore.exceptions import ClientError

class TransformData:
    def __init__(self):
        config = load_config()
        self.bucket_name = config["MINIO_BUCKET"]
        self.raw_key = generate_s3_key(DataType.RAW)
        self.processed_key = generate_s3_key(DataType.PROCESSED)
        self.s3_client = get_s3_client()

    def _read_raw_json(self) -> pd.DataFrame:
        """
        Reads raw JSON data from an S3 (MinIO) bucket and loads it into a DataFrame.
        """
        try:
            logger.info(f"Reading raw data from s3://{self.bucket_name}/{self.raw_key}")
            
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.raw_key)
            body = response.get("Body")

            if body is None:
                logger.error("No content found in S3 object body.")
                return pd.DataFrame()

            raw_data = body.read()

            df = pd.read_json(io.BytesIO(raw_data), lines=False)

            if df.empty:
                logger.warning("Loaded DataFrame is empty.")
                return pd.DataFrame()

            logger.info(f"Successfully loaded {len(df)} records into DataFrame")
            return df

        except (ClientError, ValueError) as e:
            logger.error(f"Failed to read or parse JSON from S3: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error while reading from S3: {e}")
            raise
        

    def transform(self) -> pd.DataFrame:
        """
        Transform raw user action logs:
        - Flatten metadata
        - Filter invalid records
        - Normalize timestamp to ISO 8601 UTC (YYYY-MM-DDTHH:MM:SSZ)
        - Keep only required fields
        """
        try:
            logger.info("Starting transformation process")

            df = self._read_raw_json()
            logger.info(f"Raw records loaded: {len(df)}")

            if df.empty:
                logger.warning("DataFrame is empty. Skipping transformation.")
                return df

            # Step 1: Flatten metadata if present
            if 'metadata' in df.columns:
                logger.info("Flattening metadata column")
                metadata_df = pd.json_normalize(df['metadata'])
                df = df.drop(columns=['metadata']).join(metadata_df)

            # Step 2: Remove rows with missing critical fields
            required_fields = ["user_id", "action_type"]
            missing_before = df.shape[0]
            df = df.dropna(subset=required_fields)
            missing_after = df.shape[0]
            logger.info(f"Dropped {missing_before - missing_after} rows with missing user_id or action_type")

            # Step 3: Normalize timestamp to UTC ISO 8601 (no microseconds)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
                df = df.dropna(subset=["timestamp"])
                df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.info("Timestamp normalized to UTC ISO 8601 format")
            else:
                logger.warning("Timestamp column missing. Skipping timestamp normalization.")

            # Step 4: Extract only useful fields
            useful_fields = ["device", "location"]
            missing_cols = [col for col in useful_fields if col not in df.columns]
            for col in missing_cols:
                df[col] = None  # fill missing columns with None

            logger.info(f"Transformation complete. Final record count: {len(df)}")
            return df

        except Exception as e:
            logger.exception(f"Transformation failed: {e}")
            raise

    def save_to_json(self, df):
        """
        Serializes the given DataFrame to JSON and uploads it to MinIO at the processed S3 key.
        """
        try:
            if df.empty:
                logger.warning("Attempted to save empty DataFrame. Skipping upload.")
                return
            
            logger.info(f"Saving transformed data to s3://{self.bucket_name}/{self.processed_key}")
            
            json_bytes = df.to_json(orient='records', lines=False, indent=2).encode("utf-8")

            with io.BytesIO(json_bytes) as out_buffer:
                self.s3_client.upload_fileobj(out_buffer, self.bucket_name, self.processed_key)

            logger.info("Successfully saved transformed data as JSON")

        except Exception as e:
            logger.exception(f"Failed to save JSON to S3: {e}")
            raise
