# app/schemas/weather.py
from typing import Optional
from pydantic import BaseModel, validator
from datetime import date


# Weather Station Schemas
class WeatherStationBase(BaseModel):
    station_id: str
    name: Optional[str] = None
    state: Optional[str] = None


class WeatherStationCreate(WeatherStationBase):
    pass


class WeatherStationUpdate(BaseModel):
    name: Optional[str] = None
    state: Optional[str] = None


class WeatherStation(WeatherStationBase):
    id: int
    
    class Config:
        from_attributes = True


# Weather Record Schemas
class WeatherRecordBase(BaseModel):
    station_id: str
    date: date
    max_temperature_celsius: Optional[float] = None
    min_temperature_celsius: Optional[float] = None
    precipitation_mm: Optional[float] = None


class WeatherRecordCreate(BaseModel):
    station_id: str
    date: date
    max_temperature_tenths: Optional[int] = None
    min_temperature_tenths: Optional[int] = None
    precipitation_tenths: Optional[int] = None
    
    @validator('max_temperature_tenths', 'min_temperature_tenths', 'precipitation_tenths')
    def validate_missing_values(cls, v):
        """Convert -9999 (missing values) to None"""
        return None if v == -9999 else v


class WeatherRecordUpdate(BaseModel):
    max_temperature_tenths: Optional[int] = None
    min_temperature_tenths: Optional[int] = None
    precipitation_tenths: Optional[int] = None
    
    @validator('max_temperature_tenths', 'min_temperature_tenths', 'precipitation_tenths')
    def validate_missing_values(cls, v):
        """Convert -9999 (missing values) to None"""
        return None if v == -9999 else v


class WeatherRecord(WeatherRecordBase):
    id: int
    max_temperature_tenths: Optional[int] = None
    min_temperature_tenths: Optional[int] = None
    precipitation_tenths: Optional[int] = None
    
    class Config:
        from_attributes = True


# Weather Statistics Schemas
class WeatherStatisticsBase(BaseModel):
    station_id: str
    year: int
    avg_max_temperature_celsius: Optional[float] = None
    avg_min_temperature_celsius: Optional[float] = None
    total_precipitation_cm: Optional[float] = None


class WeatherStatisticsCreate(WeatherStatisticsBase):
    total_records: int = 0
    valid_max_temp_records: int = 0
    valid_min_temp_records: int = 0
    valid_precipitation_records: int = 0


class WeatherStatisticsUpdate(BaseModel):
    avg_max_temperature_celsius: Optional[float] = None
    avg_min_temperature_celsius: Optional[float] = None
    total_precipitation_cm: Optional[float] = None
    total_records: Optional[int] = None
    valid_max_temp_records: Optional[int] = None
    valid_min_temp_records: Optional[int] = None
    valid_precipitation_records: Optional[int] = None


class WeatherStatistics(WeatherStatisticsBase):
    id: int
    total_records: int
    valid_max_temp_records: int
    valid_min_temp_records: int
    valid_precipitation_records: int
    
    class Config:
        from_attributes = True


# API Response Schemas
class WeatherDataResponse(BaseModel):
    """Response schema for weather data API"""
    total: int
    page: int
    size: int
    data: list[WeatherRecord]


class WeatherStatsResponse(BaseModel):
    """Response schema for weather statistics API"""
    total: int
    page: int
    size: int
    data: list[WeatherStatistics]