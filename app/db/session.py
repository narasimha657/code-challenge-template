from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create database URL
SQLALCHEMY_DATABASE_URL = settings.get_database_uri

logger.info(f"SQLALCHEMY_DATABASE_URL: {SQLALCHEMY_DATABASE_URL}")
# Create engine with logging
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=False,  # Set to False in production
    pool_pre_ping=True,  # Helps with detecting disconnections
    pool_size=5,  # Number of connections to keep open
    max_overflow=10  # Max number of connections to open beyond pool_size
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Connection test function
def test_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Successfully connected to the database")
        return True
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return False