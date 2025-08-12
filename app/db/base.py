from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.sql import func

Base = declarative_base()

class TimestampMixin:
    """Timestamp mixin for SQLAlchemy models."""
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class BaseModel(Base, TimestampMixin):
    """Base model class that includes common fields."""
    __abstract__ = True
    
    id = Column(Integer, primary_key=True, index=True)