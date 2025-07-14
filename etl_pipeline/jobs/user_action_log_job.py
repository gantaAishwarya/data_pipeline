import logging

logger = logging.getLogger(__name__)

def run_ingest():
    logger.info('Ingest Job')

def run_transform():
    logger.info('Transform Job')

def load_to_postgres():
    logger.info('Load to postgres Job')