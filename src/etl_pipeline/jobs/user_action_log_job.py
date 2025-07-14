import logging
from src.etl_pipeline.tasks.ingest_data import IngestData
from src.etl_pipeline.tasks.transform_data import TransformData
from src.etl_pipeline.tasks.load_data import LoadData

logger = logging.getLogger(__name__)

def run_ingest():
    logger.info('Running Ingesting data Job.')
    ingest_Data = IngestData()
    ingest_Data.ingest_raw_data()

def run_transform():
    logger.info('Running Transforming data Job.')
    transform_data = TransformData()
    df_t = transform_data.transform()
    transform_data.save_to_json(df_t)

def load_to_postgres():
    logger.info('Running load to postgres Job.')
    loader = LoadData()
    loader.load()