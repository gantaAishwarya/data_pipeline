from .models.base import Base, get_engine
from .models.dim_users import DimUser
from .models.dim_actions import DimAction
from .models.fact_user_actions import FactUserAction
from src.config import logger

def init_db():
    """
    Initializes the database by creating all defined tables if they do not exist.
    """
    logger.info("Initializing database...")

    try:
        engine = get_engine()
        logger.debug("Engine successfully retrieved.")

        logger.info("Creating tables if they do not exist...")
        Base.metadata.create_all(bind=engine)
        
        logger.info("Database initialized successfully. All tables are ready.")
    except Exception as e:
        logger.exception("Error during database initialization.")
        raise RuntimeError(f"Database initialization failed: {e}") from e
