import sys
import os 
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def load_env_var(name: str) -> str:
    val = os.getenv(name)
    if not val:
        logger.error(f"Missing required environment variable: {name}")
        sys.exit(1)
    return val

def load_config():
    keys = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET", "RAW_LOCAL_FILE"]
    config = {key: load_env_var(key) for key in keys}
    logger.info("Loaded config.")
    return config

def database_config():
    user = load_env_var("POSTGRES_USER")
    password = load_env_var("POSTGRES_PASSWORD")
    host = load_env_var("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = load_env_var("POSTGRES_DB")
    uri=f"postgresql://{user}:{password}@{host}:{port}/{db}"

    logger.info("Constructed database config successfully")
    return uri
