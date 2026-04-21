# src/antistrat/db/session.py
import logging
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from antistrat.utils.logging_config import configure_logging

from .base import Base

configure_logging()
logger = logging.getLogger(__name__)

# Ensure the data directory exists
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_DIR = PROJECT_ROOT / "data"
DB_DIR.mkdir(parents=True, exist_ok=True)

# SQLite database URL
DATABASE_URL = f"sqlite:///{(DB_DIR / 'antistrat.db').as_posix()}"
logger.info("Using database URL %s", DATABASE_URL)

# Create the SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # Needed for SQLite + Streamlit
)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Creates all tables in the database."""
    # Import models here so they are registered with Base before creation
    from . import models as _models

    _ = _models
    Base.metadata.create_all(bind=engine)
    logger.info("Database schema initialized")


def reset_db():
    """Drops and recreates all tables for clean test runs."""
    from . import models as _models

    _ = _models
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    logger.warning("Database schema reset")


def get_db():
    """Dependency to get a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
