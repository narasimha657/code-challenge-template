# app/api/routes/weather.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import date
import logging

from app.db.session import get_db
from app.db.repositories.weather_repo import WeatherRecordRepository, WeatherStatisticsRepository
from app.schemas.weather import WeatherDataResponse, WeatherStatsResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=WeatherDataResponse)
async def get_weather_data(
    station_id: Optional[str] = Query(None, description="Filter by weather station ID"),
    start_date: Optional[date] = Query(None, description="Filter by start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Filter by end date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    size: int = Query(50, ge=1, le=1000, description="Number of records per page"),
    db: Session = Depends(get_db)
):
    """
    Get weather data records with optional filtering and pagination.
    
    Returns ingested weather data including:
    - Date
    - Maximum temperature (degrees Celsius)
    - Minimum temperature (degrees Celsius)
    - Precipitation (millimeters)
    
    Query parameters:
    - station_id: Filter by specific weather station ID
    - start_date: Filter by start date (inclusive, format: YYYY-MM-DD)
    - end_date: Filter by end date (inclusive, format: YYYY-MM-DD)
    - page: Page number for pagination (starts from 1)
    - size: Number of records per page (max 1000)
    """
    try:
        logger.info(f"Weather data API called with filters: station_id={station_id}, start_date={start_date}, end_date={end_date}")
        
        # Calculate skip for pagination
        skip = (page - 1) * size
        
        # Create repository
        record_repo = WeatherRecordRepository(db)
        
        # Get weather records with filters
        records = record_repo.get_records_by_filters(
            station_id=station_id,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=size
        )
        
        # Get total count for pagination
        total = record_repo.count_records_by_filters(
            station_id=station_id,
            start_date=start_date,
            end_date=end_date
        )
        
        return WeatherDataResponse(
            total=total,
            page=page,
            size=size,
            data=records
        )
        
    except Exception as e:
        logger.error(f"Error getting weather data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get weather data: {str(e)}"
        )


@router.get("/stats", response_model=WeatherStatsResponse)
async def get_weather_stats(
    station_id: Optional[str] = Query(None, description="Filter by weather station ID"),
    year: Optional[int] = Query(None, description="Filter by specific year"),
    start_year: Optional[int] = Query(None, description="Filter by start year (inclusive)"),
    end_year: Optional[int] = Query(None, description="Filter by end year (inclusive)"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    size: int = Query(50, ge=1, le=1000, description="Number of records per page"),
    db: Session = Depends(get_db)
):
    """
    Get weather statistics with optional filtering and pagination.
    
    Returns calculated yearly statistics for weather stations including:
    - Average maximum temperature (degrees Celsius)
    - Average minimum temperature (degrees Celsius)
    - Total accumulated precipitation (centimeters)
    
    Note: These statistics must be calculated first using the /api/weather/statistics/calculate endpoint.
    
    Query parameters:
    - station_id: Filter by specific weather station
    - year: Filter by specific year
    - start_year: Filter by start year (inclusive)
    - end_year: Filter by end year (inclusive)
    - page: Page number for pagination (starts from 1)
    - size: Number of records per page (max 1000)
    """
    try:
        logger.info(f"Weather stats API called with filters: station_id={station_id}, year={year}, start_year={start_year}, end_year={end_year}")
        
        # Calculate skip for pagination
        skip = (page - 1) * size
        
        # Create repository
        stats_repo = WeatherStatisticsRepository(db)
        
        # Get statistics with filters
        statistics = stats_repo.get_statistics_by_filters(
            station_id=station_id,
            year=year,
            start_year=start_year,
            end_year=end_year,
            skip=skip,
            limit=size
        )
        
        # Get total count for pagination
        total = stats_repo.count_statistics_by_filters(
            station_id=station_id,
            year=year,
            start_year=start_year,
            end_year=end_year
        )
        
        return WeatherStatsResponse(
            total=total,
            page=page,
            size=size,
            data=statistics
        )
        
    except Exception as e:
        logger.error(f"Error getting weather statistics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get weather statistics: {str(e)}"
        )