import pandas as pd
import io
from src.config import load_config, logger
from src.database.models.base import SessionLocal
from src.database.models.dim_users import DimUser
from src.database.models.dim_actions import DimAction
from src.database.models.fact_user_actions import FactUserAction
from src.etl_pipeline.utils import get_s3_client, generate_s3_key, DataType
from src.etl_pipeline.tasks.quality_checks import run_data_quality_checks
from botocore.exceptions import ClientError

class LoadData:
    """Class to load processed JSON data from S3 into PostgreSQL database."""

    def __init__(self):
        config = load_config()
        self.bucket_name = config["MINIO_BUCKET"]
        self.processed_key = generate_s3_key(DataType.PROCESSED)
        self.s3_client = get_s3_client()

    def _read_processed_json(self) -> pd.DataFrame:
        """
        Reads processed JSON data from an S3 (MinIO) bucket and loads it into a DataFrame.
        """
        try:
            logger.info(f"Reading raw data from s3://{self.bucket_name}/{self.processed_key}")
            
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.processed_key)
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

    def load(self):
        """
        Loads processed user action data into PostgreSQL using SQLAlchemy ORM.
        """
        
        logger.info("Starting data load to PostgreSQL")

        df = self._read_processed_json()
        logger.info(f"Retrieved {len(df)} records from processed S3 file")

        if df.empty:
            logger.warning("No data to load. Aborting.")
            return

        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        except Exception as e:
            logger.error(f"Failed to parse timestamp column: {e}")
            raise

        # Run data quality checks
        data = run_data_quality_checks(df)
        if data.empty:
            logger.warning("Data after quality checks is empty. Skipping load.")
            return
        
        try:
            with SessionLocal() as session:
                for idx, row in data.iterrows():
                    logger.info(f"Processing row {idx}: user_id={row['user_id']}, action_type={row['action_type']}")
                    
                    # DIM USERS
                    user = session.query(DimUser).filter_by(user_id=row['user_id']).first()
                    if not user:
                        user = DimUser(
                            user_id=row['user_id'],
                            device=row['device'] if 'device' in row else None,
                            location=row['location'] if 'location' in row else None
                        )
                        session.add(user)
                        session.flush()

                    # DIM ACTIONS
                    action = session.query(DimAction).filter_by(action_type=row['action_type']).first()
                    if not action:
                        action = DimAction(action_type=row['action_type'])
                        session.add(action)
                        session.flush()
                    
                    # FACT TABLE
                    fact_exists = session.query(FactUserAction).filter_by(
                        user_id=row['user_id'],
                        action_id=action.action_id,
                        timestamp=row['timestamp']
                    ).first()

                    if not fact_exists:
                        fact = FactUserAction(
                            user_id=row['user_id'],
                            action_id=action.action_id,
                            timestamp=row['timestamp']
                        )
                        session.add(fact)

                session.commit()
                logger.info("Successfully loaded all data into PostgreSQL.")

        except Exception as e:
            logger.exception(f"Failed during DB load process: {e}")
            raise
