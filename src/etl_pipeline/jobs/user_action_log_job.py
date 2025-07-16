import logging
from src.etl_pipeline.tasks.ingest_data import IngestData
from src.etl_pipeline.tasks.transform_data import TransformData
from src.etl_pipeline.tasks.load_data import LoadData

logger = logging.getLogger(__name__)

def run_ingest():
    """Ingest raw data from source to staging."""
    logger.info("Starting the ingestion job...")
    ingest_data = IngestData()
    ingest_data.ingest_raw_data()
    logger.info("Ingestion job completed.")

def run_transform():
    """Transform raw data and save the processed result."""
    logger.info("Starting the transformation job...")
    transform_data = TransformData()
    transformed_df = transform_data.transform()
    transform_data.save_to_json(transformed_df)
    logger.info("Transformation job completed and saved to JSON.")

def run_load():
    """Load transformed data into PostgreSQL database."""
    logger.info("Starting the loading job to PostgreSQL...")
    loader = LoadData()
    loader.load()
    logger.info("Data load to PostgreSQL completed.")
