from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from src.config import database_config

# Global declarative base for all models
Base = declarative_base()

# Create engine only once at module level
engine = create_engine(database_config(), echo=False)

# Thread-safe session factory
SessionLocal = scoped_session(sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
))

def get_engine():
    """
    Returns the SQLAlchemy engine instance.
    """
    return engine
