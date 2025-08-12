# app/models/weather.py
from sqlalchemy import Column, Integer, String, Date, Float, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from app.db.base import Base


class WeatherStation(Base):
    """
    Model to store weather station information
    """
    __tablename__ = "weather_stations"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, unique=True, index=True, nullable=False)  # e.g., "USC00110072"
    name = Column(String)  # Optional: station name if available
    state = Column(String)  # e.g., "Nebraska", "Iowa", etc.
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class WeatherRecord(Base):
    """
    Model to store daily weather data records
    """
    __tablename__ = "weather_records"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, nullable=False, index=True)  # Foreign key reference to station
    date = Column(Date, nullable=False, index=True)
    
    # Temperature in tenths of degrees Celsius (store as integer, convert when needed)
    max_temperature_tenths = Column(Integer)  # NULL if missing (-9999)
    min_temperature_tenths = Column(Integer)  # NULL if missing (-9999)
    
    # Precipitation in tenths of millimeters (store as integer, convert when needed)
    precipitation_tenths = Column(Integer)  # NULL if missing (-9999)
    
    # Computed columns for easier querying (in standard units)
    max_temperature_celsius = Column(Float)  # Computed: max_temperature_tenths / 10.0
    min_temperature_celsius = Column(Float)  # Computed: min_temperature_tenths / 10.0
    precipitation_mm = Column(Float)  # Computed: precipitation_tenths / 10.0
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Ensure no duplicate records for same station and date
    __table_args__ = (
        UniqueConstraint('station_id', 'date', name='uq_station_date'),
        Index('idx_station_date', 'station_id', 'date'),
        Index('idx_date_station', 'date', 'station_id'),
    )


class WeatherStatistics(Base):
    """
    Model to store calculated yearly statistics for each weather station
    """
    __tablename__ = "weather_statistics"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    
    # Calculated statistics
    avg_max_temperature_celsius = Column(Float)  # NULL if cannot be calculated
    avg_min_temperature_celsius = Column(Float)  # NULL if cannot be calculated
    total_precipitation_cm = Column(Float)  # NULL if cannot be calculated
    
    # Metadata about the calculations
    total_records = Column(Integer)  # Total records for this station/year
    valid_max_temp_records = Column(Integer)  # Records with valid max temperature
    valid_min_temp_records = Column(Integer)  # Records with valid min temperature
    valid_precipitation_records = Column(Integer)  # Records with valid precipitation
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Ensure no duplicate statistics for same station and year
    __table_args__ = (
        UniqueConstraint('station_id', 'year', name='uq_station_year'),
        Index('idx_station_year', 'station_id', 'year'),
        Index('idx_year_station', 'year', 'station_id'),
    )