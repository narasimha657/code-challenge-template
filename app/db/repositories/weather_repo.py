# app/db/repositories/weather_repo.py
from typing import List, Optional, Dict, Any
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, extract, text

from app.models.weather import WeatherStation, WeatherRecord, WeatherStatistics
from app.schemas.weather import (
    WeatherStationCreate, WeatherStationUpdate,
    WeatherRecordCreate, WeatherRecordUpdate,
    WeatherStatisticsCreate, WeatherStatisticsUpdate
)


class WeatherStationRepository:
    """Repository for weather station operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create(self, obj_in: WeatherStationCreate) -> WeatherStation:
        """Create a new weather station"""
        db_obj = WeatherStation(
            station_id=obj_in.station_id,
            name=obj_in.name,
            state=obj_in.state
        )
        self.db.add(db_obj)
        self.db.flush()
        return db_obj
    
    def get_by_station_id(self, station_id: str) -> Optional[WeatherStation]:
        """Get weather station by station ID"""
        return self.db.query(WeatherStation).filter(
            WeatherStation.station_id == station_id
        ).first()
    
    def get_or_create_station(self, station_id: str, state: Optional[str] = None) -> WeatherStation:
        """Get existing station or create new one"""
        station = self.get_by_station_id(station_id)
        if station:
            return station
        
        # Create new station
        station_data = WeatherStationCreate(
            station_id=station_id,
            state=state
        )
        return self.create(station_data)
    
    def list_all(self) -> List[WeatherStation]:
        """Get all weather stations"""
        return self.db.query(WeatherStation).all()


class WeatherRecordRepository:
    """Repository for weather record operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_or_update_record(self, record_data: WeatherRecordCreate) -> WeatherRecord:
        """Create new record or update existing one"""
        # Check if record already exists
        existing_record = self.get_by_station_and_date(
            record_data.station_id, 
            record_data.date
        )
        
        if existing_record:
            # Update existing record
            return self.update_record(existing_record, record_data)
        else:
            # Create new record
            return self.create_record(record_data)
    
    def create_record(self, record_data: WeatherRecordCreate) -> WeatherRecord:
        """Create a new weather record"""
        # Convert tenths to standard units for computed columns
        max_temp_celsius = (
            record_data.max_temperature_tenths / 10.0 
            if record_data.max_temperature_tenths is not None 
            else None
        )
        min_temp_celsius = (
            record_data.min_temperature_tenths / 10.0 
            if record_data.min_temperature_tenths is not None 
            else None
        )
        precipitation_mm = (
            record_data.precipitation_tenths / 10.0 
            if record_data.precipitation_tenths is not None 
            else None
        )
        
        db_obj = WeatherRecord(
            station_id=record_data.station_id,
            date=record_data.date,
            max_temperature_tenths=record_data.max_temperature_tenths,
            min_temperature_tenths=record_data.min_temperature_tenths,
            precipitation_tenths=record_data.precipitation_tenths,
            max_temperature_celsius=max_temp_celsius,
            min_temperature_celsius=min_temp_celsius,
            precipitation_mm=precipitation_mm
        )
        self.db.add(db_obj)
        self.db.flush()
        return db_obj
    
    def update_record(self, db_obj: WeatherRecord, record_data: WeatherRecordCreate) -> WeatherRecord:
        """Update an existing weather record"""
        # Update tenths values
        db_obj.max_temperature_tenths = record_data.max_temperature_tenths
        db_obj.min_temperature_tenths = record_data.min_temperature_tenths
        db_obj.precipitation_tenths = record_data.precipitation_tenths
        
        # Update computed columns
        db_obj.max_temperature_celsius = (
            record_data.max_temperature_tenths / 10.0 
            if record_data.max_temperature_tenths is not None 
            else None
        )
        db_obj.min_temperature_celsius = (
            record_data.min_temperature_tenths / 10.0 
            if record_data.min_temperature_tenths is not None 
            else None
        )
        db_obj.precipitation_mm = (
            record_data.precipitation_tenths / 10.0 
            if record_data.precipitation_tenths is not None 
            else None
        )
        
        self.db.flush()
        return db_obj
    
    def get_by_station_and_date(self, station_id: str, record_date: date) -> Optional[WeatherRecord]:
        """Get weather record by station and date"""
        return self.db.query(WeatherRecord).filter(
            and_(
                WeatherRecord.station_id == station_id,
                WeatherRecord.date == record_date
            )
        ).first()
    
    def get_records_by_filters(
        self,
        station_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[WeatherRecord]:
        """Get weather records with optional filtering and pagination"""
        query = self.db.query(WeatherRecord)
        
        # Apply filters
        if station_id:
            query = query.filter(WeatherRecord.station_id == station_id)
        if start_date:
            query = query.filter(WeatherRecord.date >= start_date)
        if end_date:
            query = query.filter(WeatherRecord.date <= end_date)
        
        # Apply ordering and pagination
        query = query.order_by(WeatherRecord.station_id, WeatherRecord.date)
        query = query.offset(skip).limit(limit)
        
        return query.all()
    
    def count_records_by_filters(
        self,
        station_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """Count weather records matching filters"""
        query = self.db.query(WeatherRecord)
        
        # Apply filters
        if station_id:
            query = query.filter(WeatherRecord.station_id == station_id)
        if start_date:
            query = query.filter(WeatherRecord.date >= start_date)
        if end_date:
            query = query.filter(WeatherRecord.date <= end_date)
        
        return query.count()
    
    def get_yearly_statistics_data(self, station_id: str, year: int) -> WeatherStatisticsCreate:
        """Calculate yearly statistics for a station"""
        # Get all records for the station and year
        records = self.db.query(WeatherRecord).filter(
            and_(
                WeatherRecord.station_id == station_id,
                extract('year', WeatherRecord.date) == year
            )
        ).all()
        
        total_records = len(records)
        
        # Calculate statistics, ignoring missing values (None)
        valid_max_temps = [r.max_temperature_celsius for r in records if r.max_temperature_celsius is not None]
        valid_min_temps = [r.min_temperature_celsius for r in records if r.min_temperature_celsius is not None]
        valid_precips = [r.precipitation_mm for r in records if r.precipitation_mm is not None]
        
        # Calculate averages and totals
        avg_max_temp = sum(valid_max_temps) / len(valid_max_temps) if valid_max_temps else None
        avg_min_temp = sum(valid_min_temps) / len(valid_min_temps) if valid_min_temps else None
        
        # Convert total precipitation from mm to cm
        total_precip_cm = sum(valid_precips) / 10.0 if valid_precips else None
        
        return WeatherStatisticsCreate(
            station_id=station_id,
            year=year,
            avg_max_temperature_celsius=avg_max_temp,
            avg_min_temperature_celsius=avg_min_temp,
            total_precipitation_cm=total_precip_cm,
            total_records=total_records,
            valid_max_temp_records=len(valid_max_temps),
            valid_min_temp_records=len(valid_min_temps),
            valid_precipitation_records=len(valid_precips)
        )


class WeatherStatisticsRepository:
    """Repository for weather statistics operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_or_update_statistics(
        self, 
        station_id: str, 
        year: int, 
        stats_data: WeatherStatisticsCreate
    ) -> WeatherStatistics:
        """Create new statistics record or update existing one"""
        existing_stats = self.get_by_station_and_year(station_id, year)
        
        if existing_stats:
            return self.update(existing_stats, stats_data)
        else:
            return self.create(stats_data)
    
    def create(self, stats_data: WeatherStatisticsCreate) -> WeatherStatistics:
        """Create new weather statistics record"""
        db_obj = WeatherStatistics(**stats_data.dict())
        self.db.add(db_obj)
        self.db.flush()
        return db_obj
    
    def update(self, db_obj: WeatherStatistics, stats_data: WeatherStatisticsCreate) -> WeatherStatistics:
        """Update existing weather statistics record"""
        for field, value in stats_data.dict().items():
            setattr(db_obj, field, value)
        self.db.flush()
        return db_obj
    
    def get_by_station_and_year(self, station_id: str, year: int) -> Optional[WeatherStatistics]:
        """Get statistics by station and year"""
        return self.db.query(WeatherStatistics).filter(
            and_(
                WeatherStatistics.station_id == station_id,
                WeatherStatistics.year == year
            )
        ).first()
    
    def get_statistics_by_filters(
        self,
        station_id: Optional[str] = None,
        year: Optional[int] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[WeatherStatistics]:
        """Get weather statistics with optional filtering and pagination"""
        query = self.db.query(WeatherStatistics)
        
        # Apply filters
        if station_id:
            query = query.filter(WeatherStatistics.station_id == station_id)
        if year:
            query = query.filter(WeatherStatistics.year == year)
        if start_year:
            query = query.filter(WeatherStatistics.year >= start_year)
        if end_year:
            query = query.filter(WeatherStatistics.year <= end_year)
        
        # Apply ordering and pagination
        query = query.order_by(WeatherStatistics.station_id, WeatherStatistics.year)
        query = query.offset(skip).limit(limit)
        
        return query.all()
    
    def count_statistics_by_filters(
        self,
        station_id: Optional[str] = None,
        year: Optional[int] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> int:
        """Count weather statistics matching filters"""
        query = self.db.query(WeatherStatistics)
        
        # Apply filters
        if station_id:
            query = query.filter(WeatherStatistics.station_id == station_id)
        if year:
            query = query.filter(WeatherStatistics.year == year)
        if start_year:
            query = query.filter(WeatherStatistics.year >= start_year)
        if end_year:
            query = query.filter(WeatherStatistics.year <= end_year)
        
        return query.count()